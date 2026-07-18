package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	relayTestTenant    = admission.TenantID(71)
	relayTestToken     = "relay-test-tenant-token"
	relayTestRequestID = "1234567890abcdef1234567890abcdef"
	relayTestRequest   = `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8,"stream":true}`

	relayTestChunkOne = "data: {\"object\":\"chat.completion.chunk\"}\n\n"
	relayTestChunkTwo = "data: {\"id\":\"chunk-2\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\r\n\r\n"
	relayTestDone     = "data: [DONE]\n\n"
)

var relayTestBodyClosedError = errors.New("relay test body closed")

type relayTestPermit struct {
	ctx context.Context

	mu       sync.Mutex
	outcomes []admission.ServingOutcome
}

func (p *relayTestPermit) Context() context.Context { return p.ctx }

func (p *relayTestPermit) Finish(outcome admission.ServingOutcome) admission.TerminalResult {
	p.mu.Lock()
	p.outcomes = append(p.outcomes, outcome)
	p.mu.Unlock()
	return admission.TerminalResult{AccountingReleased: true}
}

func (p *relayTestPermit) recordedOutcomes() []admission.ServingOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]admission.ServingOutcome(nil), p.outcomes...)
}

type relayTestGate struct {
	permit *relayTestPermit
	calls  atomic.Int32
}

func (g *relayTestGate) Acquire(context.Context, admission.Admission) (workPermit, admission.Decision) {
	g.calls.Add(1)
	return g.permit, admission.Decision{Kind: admission.DecisionDispatched}
}

type relayTestUpstream struct {
	body io.ReadCloser

	calls atomic.Int32
	mu    sync.Mutex
	mode  contract.RequestMode
}

func (u *relayTestUpstream) Complete(_ context.Context, request contract.Request) (UpstreamResponse, error) {
	u.calls.Add(1)
	u.mu.Lock()
	u.mode = request.Mode()
	u.mu.Unlock()
	return UpstreamResponse{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":      []string{"text/event-stream; charset=utf-8"},
			"X-Upstream-Secret": []string{"must-not-be-relayed"},
		},
		Body: u.body,
	}, nil
}

func (u *relayTestUpstream) requestMode() contract.RequestMode {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.mode
}

type relayTestBody struct {
	mu        sync.Mutex
	immediate []byte
	offset    int
	slow      []byte
	slowIndex int
	slowDelay time.Duration
	block     bool
	closeErr  error

	closed      chan struct{}
	releaseEOF  chan struct{}
	blocked     chan struct{}
	closeOnce   sync.Once
	releaseOnce sync.Once
	blockedOnce sync.Once
	closeCalls  atomic.Int32
}

func newRelayTestBody(immediate string) *relayTestBody {
	return &relayTestBody{
		immediate:  []byte(immediate),
		closed:     make(chan struct{}),
		releaseEOF: make(chan struct{}),
		blocked:    make(chan struct{}),
	}
}

func (b *relayTestBody) Read(destination []byte) (int, error) {
	select {
	case <-b.closed:
		return 0, relayTestBodyClosedError
	default:
	}

	b.mu.Lock()
	if b.offset < len(b.immediate) {
		n := copy(destination, b.immediate[b.offset:])
		b.offset += n
		b.mu.Unlock()
		return n, nil
	}
	if b.slowIndex < len(b.slow) {
		value := b.slow[b.slowIndex]
		b.slowIndex++
		delay := b.slowDelay
		b.mu.Unlock()
		b.markBlocked()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			if len(destination) == 0 {
				return 0, nil
			}
			destination[0] = value
			return 1, nil
		case <-b.closed:
			return 0, relayTestBodyClosedError
		}
	}
	block := b.block
	b.mu.Unlock()
	if !block {
		return 0, io.EOF
	}
	b.markBlocked()
	select {
	case <-b.closed:
		return 0, relayTestBodyClosedError
	case <-b.releaseEOF:
		return 0, io.EOF
	}
}

func (b *relayTestBody) Close() error {
	b.closeCalls.Add(1)
	b.closeOnce.Do(func() { close(b.closed) })
	return b.closeErr
}

func (b *relayTestBody) markBlocked() {
	b.blockedOnce.Do(func() { close(b.blocked) })
}

func (b *relayTestBody) waitBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-b.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream body did not reach its blocking read")
	}
}

func (b *relayTestBody) allowEOF() {
	b.releaseOnce.Do(func() { close(b.releaseEOF) })
}

type relayTestWriter struct {
	header http.Header

	mu          sync.Mutex
	status      int
	body        bytes.Buffer
	writes      int
	flushes     int
	failWriteAt int
	failFlushAt int
	writeErr    error
	flushErr    error
	flushed     chan struct{}
}

func newRelayTestWriter() *relayTestWriter {
	return &relayTestWriter{
		header:  make(http.Header),
		flushed: make(chan struct{}, 32),
	}
}

func (w *relayTestWriter) Header() http.Header { return w.header }

func (w *relayTestWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *relayTestWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.writes++
	if w.failWriteAt != 0 && w.writes == w.failWriteAt {
		return 0, w.writeErr
	}
	return w.body.Write(body)
}

func (w *relayTestWriter) FlushError() error {
	w.mu.Lock()
	w.flushes++
	flushes := w.flushes
	failAt := w.failFlushAt
	err := w.flushErr
	w.mu.Unlock()
	select {
	case w.flushed <- struct{}{}:
	default:
	}
	if failAt != 0 && flushes == failAt {
		return err
	}
	return nil
}

func (w *relayTestWriter) waitFlush(t *testing.T) {
	t.Helper()
	select {
	case <-w.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("downstream event was not flushed")
	}
}

func (w *relayTestWriter) snapshot() (int, string, int, http.Header) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.body.String(), w.flushes, w.header.Clone()
}

type relayTestNoFlushWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newRelayTestNoFlushWriter() *relayTestNoFlushWriter {
	return &relayTestNoFlushWriter{header: make(http.Header)}
}

func (w *relayTestNoFlushWriter) Header() http.Header { return w.header }

func (w *relayTestNoFlushWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *relayTestNoFlushWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(body)
}

type relayTestFixture struct {
	handler  *Handler
	gate     *relayTestGate
	permit   *relayTestPermit
	upstream *relayTestUpstream
	body     *relayTestBody
}

func newRelayTestFixture(t *testing.T, body *relayTestBody, permitContext context.Context) *relayTestFixture {
	t.Helper()
	parser, err := contract.NewParser("local-model", contract.Limits{
		MaxBodyBytes:        2048,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 512,
		MaxCompletionTokens: 512,
		CompletionWeight:    2,
		MaxRequestUnits:     4096,
	})
	if err != nil {
		t.Fatalf("contract.NewParser() error = %v", err)
	}
	validator, err := contract.NewResponseValidator(contract.ResponseLimits{MaxBodyBytes: 2048})
	if err != nil {
		t.Fatalf("contract.NewResponseValidator() error = %v", err)
	}
	if permitContext == nil {
		permitContext = context.Background()
	}
	permit := &relayTestPermit{ctx: permitContext}
	gate := &relayTestGate{permit: permit}
	upstream := &relayTestUpstream{body: body}
	digest := sha256.Sum256([]byte(relayTestToken))
	handler := &Handler{
		parser:              parser,
		gate:                gate,
		upstream:            upstream,
		responseValidator:   validator,
		sseLimits:           contract.SSELimits{MaxTotalBytes: 2048, MaxEventBytes: 1024, MaxEvents: 16},
		defaultQueueTimeout: 100 * time.Millisecond,
		bodyReadTimeout:     time.Second,
		upstreamTimeout:     3 * time.Second,
		streamReadTimeout:   500 * time.Millisecond,
		streamEventTimeout:  time.Second,
		globalSlots:         make(chan struct{}, 1),
		tenantSlots: map[admission.TenantID]chan struct{}{
			relayTestTenant: make(chan struct{}, 1),
		},
		credentials: []storedCredential{{tenant: relayTestTenant, digest: digest}},
		requestIDs:  func() (string, error) { return relayTestRequestID, nil },
	}
	return &relayTestFixture{
		handler:  handler,
		gate:     gate,
		permit:   permit,
		upstream: upstream,
		body:     body,
	}
}

func relayTestRequestFor(ctx context.Context) *http.Request {
	request := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(relayTestRequest))
	if ctx != nil {
		request = request.WithContext(ctx)
	}
	request.Header.Set("Authorization", "Bearer "+relayTestToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(requestIDHeader, "client-controlled-request-id")
	return request
}

func relayTestRunAsync(handler *Handler, writer http.ResponseWriter, request *http.Request) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, request)
		close(done)
	}()
	return done
}

func relayTestWaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streaming handler did not return")
	}
}

func relayTestRequireOutcome(t *testing.T, permit *relayTestPermit, want admission.ServingOutcome) {
	t.Helper()
	if got := permit.recordedOutcomes(); !reflect.DeepEqual(got, []admission.ServingOutcome{want}) {
		t.Fatalf("Finish() outcomes = %v, want exactly [%v]", got, want)
	}
}

func relayTestRequireDispatchedOnce(t *testing.T, fixture *relayTestFixture) {
	t.Helper()
	if got := fixture.gate.calls.Load(); got != 1 {
		t.Fatalf("admission Acquire calls = %d, want 1", got)
	}
	if got := fixture.upstream.calls.Load(); got != 1 {
		t.Fatalf("upstream Complete calls = %d, want exactly 1", got)
	}
	if got := fixture.upstream.requestMode(); got != contract.RequestModeStreaming {
		t.Fatalf("upstream request mode = %v, want streaming", got)
	}
	if got := fixture.body.closeCalls.Load(); got != 1 {
		t.Fatalf("upstream body Close calls = %d, want exactly 1", got)
	}
}

func relayTestErrorBody(failure publicError) string {
	return `{"error":{"code":"` + failure.code + `","message":"` + failure.message + `"}}` + "\n"
}

func TestHandlerRelaysExactSSEAndFinishesPermitOnce(t *testing.T) {
	body := newRelayTestBody(relayTestChunkOne + relayTestChunkTwo + relayTestDone)
	fixture := newRelayTestFixture(t, body, context.Background())
	writer := newRelayTestWriter()
	writer.header.Set("Content-Length", "999")
	writer.header.Set("Content-Encoding", "gzip")
	writer.header.Set("X-Preexisting-Secret", "must-be-cleared")

	fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

	status, responseBody, flushes, header := writer.snapshot()
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", status, responseBody)
	}
	if want := relayTestChunkOne + relayTestChunkTwo + relayTestDone; responseBody != want {
		t.Fatalf("body = %q, want exact %q", responseBody, want)
	}
	if flushes != 3 {
		t.Fatalf("flush count = %d, want one per event (3)", flushes)
	}
	wantHeader := http.Header{
		"Cache-Control":          []string{"no-store"},
		"Content-Type":           []string{"text/event-stream"},
		"X-Content-Type-Options": []string{"nosniff"},
		requestIDHeader:          []string{relayTestRequestID},
	}
	if !reflect.DeepEqual(header, wantHeader) {
		t.Fatalf("headers = %#v, want exact safe headers %#v", header, wantHeader)
	}
	relayTestRequireDispatchedOnce(t, fixture)
	relayTestRequireOutcome(t, fixture.permit, admission.ServingCompleted)
}

func TestHandlerRejectsMalformedFirstSSEEventBeforeStreamCommit(t *testing.T) {
	body := newRelayTestBody("data: {\"object\":\"not-a-chat-chunk\"}\n\n")
	fixture := newRelayTestFixture(t, body, context.Background())
	writer := newRelayTestWriter()

	fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

	status, responseBody, flushes, header := writer.snapshot()
	if status != http.StatusBadGateway || responseBody != relayTestErrorBody(errBadUpstream) {
		t.Fatalf("response = (%d, %q), want static 502 %q", status, responseBody, relayTestErrorBody(errBadUpstream))
	}
	if flushes != 0 || header.Get("Content-Type") != "application/json" {
		t.Fatalf("first invalid event committed SSE: flushes=%d headers=%#v", flushes, header)
	}
	if strings.Contains(responseBody, "not-a-chat-chunk") {
		t.Fatal("static error leaked malformed upstream data")
	}
	relayTestRequireDispatchedOnce(t, fixture)
	relayTestRequireOutcome(t, fixture.permit, admission.ServingUpstreamFailed)
}

func TestHandlerTruncatesCommittedSSEOnUpstreamFailure(t *testing.T) {
	largeEvent := "data: {\"object\":\"chat.completion.chunk\",\"padding\":\"" + strings.Repeat("x", 128) + "\"}\n\n"
	tests := []struct {
		name      string
		stream    string
		configure func(*Handler)
	}{
		{name: "malformed event", stream: relayTestChunkOne + "data: not-json\n\n"},
		{
			name:   "oversized event",
			stream: relayTestChunkOne + largeEvent,
			configure: func(handler *Handler) {
				handler.sseLimits = contract.SSELimits{MaxTotalBytes: 1024, MaxEventBytes: 64, MaxEvents: 16}
			},
		},
		{name: "truncated event", stream: relayTestChunkOne + "data: {\"object\":\"chat.completion.chunk\"}"},
		{name: "trailing data after done", stream: relayTestChunkOne + relayTestDone + "x"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := newRelayTestBody(test.stream)
			fixture := newRelayTestFixture(t, body, context.Background())
			if test.configure != nil {
				test.configure(fixture.handler)
			}
			writer := newRelayTestWriter()

			fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

			status, responseBody, flushes, header := writer.snapshot()
			if status != http.StatusOK || responseBody != relayTestChunkOne {
				t.Fatalf("committed response = (%d, %q), want only first chunk", status, responseBody)
			}
			if flushes != 1 || header.Get("Content-Type") != "text/event-stream" {
				t.Fatalf("committed stream state = flushes:%d headers:%#v", flushes, header)
			}
			if strings.Contains(responseBody, `"error"`) || strings.Contains(responseBody, "[DONE]") {
				t.Fatal("committed failure appended JSON or a synthetic terminal marker")
			}
			relayTestRequireDispatchedOnce(t, fixture)
			relayTestRequireOutcome(t, fixture.permit, admission.ServingUpstreamFailed)
		})
	}
}

func TestHandlerHoldsDoneUntilEOFAndSuccessfulClose(t *testing.T) {
	t.Run("clean EOF and close release terminal marker", func(t *testing.T) {
		body := newRelayTestBody(relayTestChunkOne + relayTestDone)
		body.block = true
		fixture := newRelayTestFixture(t, body, context.Background())
		writer := newRelayTestWriter()
		done := relayTestRunAsync(fixture.handler, writer, relayTestRequestFor(context.Background()))

		writer.waitFlush(t)
		body.waitBlocked(t)
		status, responseBody, flushes, _ := writer.snapshot()
		if status != http.StatusOK || responseBody != relayTestChunkOne || flushes != 1 {
			t.Fatalf("before EOF response = (%d, %q, flushes=%d), want only first chunk", status, responseBody, flushes)
		}
		if strings.Contains(responseBody, "[DONE]") {
			t.Fatal("terminal marker was flushed before EOF")
		}

		body.allowEOF()
		relayTestWaitDone(t, done)
		status, responseBody, flushes, _ = writer.snapshot()
		if want := relayTestChunkOne + relayTestDone; status != http.StatusOK || responseBody != want || flushes != 2 {
			t.Fatalf("completed response = (%d, %q, flushes=%d), want %q and 2 flushes", status, responseBody, flushes, want)
		}
		relayTestRequireDispatchedOnce(t, fixture)
		relayTestRequireOutcome(t, fixture.permit, admission.ServingCompleted)
	})

	t.Run("close failure withholds terminal marker", func(t *testing.T) {
		body := newRelayTestBody(relayTestChunkOne + relayTestDone)
		body.closeErr = errors.New("close failed")
		fixture := newRelayTestFixture(t, body, context.Background())
		writer := newRelayTestWriter()

		fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

		status, responseBody, flushes, _ := writer.snapshot()
		if status != http.StatusOK || responseBody != relayTestChunkOne || flushes != 1 {
			t.Fatalf("close-failed response = (%d, %q, flushes=%d), want terminal marker withheld", status, responseBody, flushes)
		}
		if strings.Contains(responseBody, "[DONE]") || strings.Contains(responseBody, `"error"`) {
			t.Fatal("close failure appended a terminal marker or JSON error")
		}
		relayTestRequireDispatchedOnce(t, fixture)
		relayTestRequireOutcome(t, fixture.permit, admission.ServingUpstreamFailed)
	})
}

func TestHandlerRejectsStreamingWithoutFlusherBeforeAdmission(t *testing.T) {
	body := newRelayTestBody(relayTestChunkOne + relayTestDone)
	fixture := newRelayTestFixture(t, body, context.Background())
	writer := newRelayTestNoFlushWriter()

	fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

	if writer.status != http.StatusInternalServerError || writer.body.String() != relayTestErrorBody(errInternal) {
		t.Fatalf("response = (%d, %q), want static 500", writer.status, writer.body.String())
	}
	if fixture.gate.calls.Load() != 0 || fixture.upstream.calls.Load() != 0 {
		t.Fatalf("unsupported flusher reached admission/upstream: %d/%d", fixture.gate.calls.Load(), fixture.upstream.calls.Load())
	}
	if got := fixture.permit.recordedOutcomes(); len(got) != 0 {
		t.Fatalf("unsupported flusher finished an unowned permit: %v", got)
	}
	if got := body.closeCalls.Load(); got != 0 {
		t.Fatalf("unattempted upstream body Close calls = %d, want 0", got)
	}
}

func TestHandlerStreamingTimeoutsBeforeAndAfterCommit(t *testing.T) {
	type timeoutKind uint8
	const (
		readTimeout timeoutKind = iota + 1
		eventTimeout
		totalTimeout
	)
	tests := []struct {
		name      string
		kind      timeoutKind
		committed bool
	}{
		{name: "read before commit", kind: readTimeout},
		{name: "event before commit", kind: eventTimeout},
		{name: "total before commit", kind: totalTimeout},
		{name: "read after commit", kind: readTimeout, committed: true},
		{name: "event after commit", kind: eventTimeout, committed: true},
		{name: "total after commit", kind: totalTimeout, committed: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prefix := ""
			if test.committed {
				prefix = relayTestChunkOne
			}
			body := newRelayTestBody(prefix)
			body.block = true
			if test.kind == eventTimeout {
				body.slow = []byte("data: {\"object\":\"chat.completion.chunk\"}\n\n")
				body.slowDelay = 15 * time.Millisecond
			}
			fixture := newRelayTestFixture(t, body, context.Background())
			switch test.kind {
			case readTimeout:
				fixture.handler.streamReadTimeout = 50 * time.Millisecond
				fixture.handler.streamEventTimeout = 500 * time.Millisecond
				fixture.handler.upstreamTimeout = 2 * time.Second
			case eventTimeout:
				fixture.handler.streamReadTimeout = 500 * time.Millisecond
				fixture.handler.streamEventTimeout = 100 * time.Millisecond
				fixture.handler.upstreamTimeout = 2 * time.Second
			case totalTimeout:
				fixture.handler.streamReadTimeout = 500 * time.Millisecond
				fixture.handler.streamEventTimeout = time.Second
				fixture.handler.upstreamTimeout = 100 * time.Millisecond
			}
			writer := newRelayTestWriter()

			fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

			status, responseBody, flushes, header := writer.snapshot()
			if test.committed {
				if status != http.StatusOK || responseBody != relayTestChunkOne || flushes != 1 {
					t.Fatalf("post-commit timeout response = (%d, %q, flushes=%d), want first chunk only", status, responseBody, flushes)
				}
				if header.Get("Content-Type") != "text/event-stream" ||
					strings.Contains(responseBody, `"error"`) || strings.Contains(responseBody, "[DONE]") {
					t.Fatal("post-commit timeout appended JSON or a synthetic terminal marker")
				}
			} else {
				if status != http.StatusGatewayTimeout || responseBody != relayTestErrorBody(errUpstreamTimeout) {
					t.Fatalf("pre-commit timeout response = (%d, %q), want static 504", status, responseBody)
				}
				if flushes != 0 || header.Get("Content-Type") != "application/json" {
					t.Fatalf("pre-commit timeout committed SSE: flushes=%d headers=%#v", flushes, header)
				}
			}
			relayTestRequireDispatchedOnce(t, fixture)
			relayTestRequireOutcome(t, fixture.permit, admission.ServingUpstreamFailed)
		})
	}
}

func TestHandlerStreamingCancellationPriority(t *testing.T) {
	tests := []struct {
		name       string
		client     bool
		committed  bool
		wantStatus int
		wantBody   string
	}{
		{name: "client before commit", client: true},
		{name: "permit before commit", wantStatus: http.StatusServiceUnavailable, wantBody: relayTestErrorBody(errDraining)},
		{name: "client after commit", client: true, committed: true, wantStatus: http.StatusOK, wantBody: relayTestChunkOne},
		{name: "permit after commit", committed: true, wantStatus: http.StatusOK, wantBody: relayTestChunkOne},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestContext, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			permitContext := context.Background()
			cancelPermit := func() {}
			if test.client {
				permitContext = requestContext
			} else {
				var cancel context.CancelFunc
				permitContext, cancel = context.WithCancel(context.Background())
				cancelPermit = cancel
				defer cancel()
			}
			prefix := ""
			if test.committed {
				prefix = relayTestChunkOne
			}
			body := newRelayTestBody(prefix)
			body.block = true
			fixture := newRelayTestFixture(t, body, permitContext)
			fixture.handler.streamReadTimeout = 2 * time.Second
			fixture.handler.streamEventTimeout = 2 * time.Second
			fixture.handler.upstreamTimeout = 3 * time.Second
			writer := newRelayTestWriter()
			done := relayTestRunAsync(fixture.handler, writer, relayTestRequestFor(requestContext))

			if test.committed {
				writer.waitFlush(t)
			}
			body.waitBlocked(t)
			if test.client {
				cancelRequest()
			} else {
				cancelPermit()
			}
			relayTestWaitDone(t, done)

			status, responseBody, flushes, _ := writer.snapshot()
			if status != test.wantStatus || responseBody != test.wantBody {
				t.Fatalf("canceled response = (%d, %q), want (%d, %q)", status, responseBody, test.wantStatus, test.wantBody)
			}
			wantFlushes := 0
			if test.committed {
				wantFlushes = 1
			}
			if flushes != wantFlushes {
				t.Fatalf("flush count = %d, want %d", flushes, wantFlushes)
			}
			if strings.Contains(responseBody, "[DONE]") || test.committed && strings.Contains(responseBody, `"error"`) {
				t.Fatal("cancellation appended a terminal marker or post-commit JSON")
			}
			relayTestRequireDispatchedOnce(t, fixture)
			relayTestRequireOutcome(t, fixture.permit, admission.ServingCanceled)
		})
	}
}

func TestHandlerStreamingDownstreamFailuresFinishOnce(t *testing.T) {
	writeFailure := errors.New("downstream write failed")
	flushFailure := errors.New("downstream flush failed")
	tests := []struct {
		name      string
		configure func(*relayTestWriter)
		wantBody  string
		flushes   int
	}{
		{
			name: "write failure",
			configure: func(writer *relayTestWriter) {
				writer.failWriteAt = 1
				writer.writeErr = writeFailure
			},
		},
		{
			name: "flush failure",
			configure: func(writer *relayTestWriter) {
				writer.failFlushAt = 1
				writer.flushErr = flushFailure
			},
			wantBody: relayTestChunkOne,
			flushes:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := newRelayTestBody(relayTestChunkOne + relayTestDone)
			fixture := newRelayTestFixture(t, body, context.Background())
			writer := newRelayTestWriter()
			test.configure(writer)

			fixture.handler.ServeHTTP(writer, relayTestRequestFor(context.Background()))

			status, responseBody, flushes, header := writer.snapshot()
			if status != http.StatusOK || responseBody != test.wantBody || flushes != test.flushes {
				t.Fatalf("downstream-failed response = (%d, %q, flushes=%d), want (200, %q, %d)", status, responseBody, flushes, test.wantBody, test.flushes)
			}
			if header.Get("Content-Type") != "text/event-stream" ||
				strings.Contains(responseBody, `"error"`) || strings.Contains(responseBody, "[DONE]") {
				t.Fatal("downstream failure appended JSON or a terminal marker")
			}
			relayTestRequireDispatchedOnce(t, fixture)
			relayTestRequireOutcome(t, fixture.permit, admission.ServingDownstreamFailed)
		})
	}
}
