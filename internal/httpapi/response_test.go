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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	responseTestTenant    = admission.TenantID(7)
	responseTestToken     = "response-test-token"
	responseTestRequestID = "0123456789abcdef0123456789abcdef"
	responseTestRequest   = `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8}`
)

type responseTestContextKey struct{}

type responseTestPermit struct {
	ctx context.Context

	mu       sync.Mutex
	outcomes []admission.ServingOutcome
}

func (p *responseTestPermit) Context() context.Context {
	return p.ctx
}

func (p *responseTestPermit) Finish(outcome admission.ServingOutcome) admission.TerminalResult {
	p.mu.Lock()
	p.outcomes = append(p.outcomes, outcome)
	p.mu.Unlock()
	return admission.TerminalResult{AccountingReleased: true}
}

func (p *responseTestPermit) recordedOutcomes() []admission.ServingOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]admission.ServingOutcome(nil), p.outcomes...)
}

type responseTestGate struct {
	permit *responseTestPermit

	mu         sync.Mutex
	calls      int
	admissions []admission.Admission
}

func (g *responseTestGate) Acquire(_ context.Context, request admission.Admission) (workPermit, admission.Decision) {
	g.mu.Lock()
	g.calls++
	g.admissions = append(g.admissions, request)
	g.mu.Unlock()
	return g.permit, admission.Decision{Kind: admission.DecisionDispatched}
}

type responseTestUpstreamFunc func(context.Context, contract.Request) (UpstreamResponse, error)

func (f responseTestUpstreamFunc) Complete(ctx context.Context, request contract.Request) (UpstreamResponse, error) {
	return f(ctx, request)
}

type responseTestBody struct {
	reader   io.Reader
	readErr  error
	closeErr error
	closes   atomic.Int32
}

func (b *responseTestBody) Read(destination []byte) (int, error) {
	n, err := b.reader.Read(destination)
	if errors.Is(err, io.EOF) && b.readErr != nil {
		return n, b.readErr
	}
	return n, err
}

func (b *responseTestBody) Close() error {
	b.closes.Add(1)
	return b.closeErr
}

func (b *responseTestBody) closeCount() int32 {
	return b.closes.Load()
}

type responseTestFailWriter struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	short    bool
	writeErr error
}

func newResponseTestFailWriter() *responseTestFailWriter {
	return &responseTestFailWriter{header: make(http.Header)}
}

func (w *responseTestFailWriter) Header() http.Header {
	return w.header
}

func (w *responseTestFailWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *responseTestFailWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short && len(body) != 0 {
		written := len(body) - 1
		_, _ = w.body.Write(body[:written])
		return written, nil
	}
	return w.body.Write(body)
}

func responseTestNewHandler(
	t *testing.T,
	upstream NonStreamingUpstream,
	permitContext context.Context,
	maxResponseBodyBytes uint64,
	upstreamTimeout time.Duration,
) (*Handler, *responseTestPermit, *responseTestGate) {
	t.Helper()

	parser, err := contract.NewParser("local-model", contract.Limits{
		MaxBodyBytes:        2048,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 512,
		MaxCompletionTokens: 512,
		CompletionWeight:    4,
		MaxRequestUnits:     4096,
	})
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	validator, err := contract.NewResponseValidator(contract.ResponseLimits{
		MaxBodyBytes: maxResponseBodyBytes,
	})
	if err != nil {
		t.Fatalf("NewResponseValidator() error = %v", err)
	}
	if permitContext == nil {
		permitContext = context.Background()
	}
	if upstreamTimeout <= 0 {
		upstreamTimeout = time.Second
	}

	permit := &responseTestPermit{ctx: permitContext}
	gate := &responseTestGate{permit: permit}
	digest := sha256.Sum256([]byte(responseTestToken))
	return &Handler{
		parser:              parser,
		gate:                gate,
		upstream:            upstream,
		responseValidator:   validator,
		defaultQueueTimeout: 100 * time.Millisecond,
		bodyReadTimeout:     time.Second,
		upstreamTimeout:     upstreamTimeout,
		globalSlots:         make(chan struct{}, 1),
		tenantSlots: map[admission.TenantID]chan struct{}{
			responseTestTenant: make(chan struct{}, 1),
		},
		credentials: []storedCredential{{tenant: responseTestTenant, digest: digest}},
		requestIDs: func() (string, error) {
			return responseTestRequestID, nil
		},
	}, permit, gate
}

func responseTestNewRequest() *http.Request {
	request := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(responseTestRequest))
	request.Header.Set("Authorization", "Bearer "+responseTestToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(requestIDHeader, "client-controlled-request-id")
	return request
}

func responseTestJSONHeader() http.Header {
	return http.Header{
		"Content-Type":      []string{"application/json"},
		"X-Upstream-Secret": []string{"UPSTREAM_HEADER_SECRET"},
	}
}

func responseTestRequireOutcome(t *testing.T, permit *responseTestPermit, want admission.ServingOutcome) {
	t.Helper()
	if got := permit.recordedOutcomes(); !reflect.DeepEqual(got, []admission.ServingOutcome{want}) {
		t.Fatalf("Finish() outcomes = %v, want [%v]", got, want)
	}
}

func TestHandlerRelaysValidatedResponseExactly(t *testing.T) {
	const responseBody = " \n{\"id\":\"chatcmpl-1\",\"object\":\"chat.completion\",\"choices\":[]}\n"

	upstreamBody := &responseTestBody{reader: strings.NewReader(responseBody)}
	permitContext := context.WithValue(context.Background(), responseTestContextKey{}, "permit-owned")
	var sawPermitContext bool
	var sawRequest contract.Request
	upstream := responseTestUpstreamFunc(func(ctx context.Context, request contract.Request) (UpstreamResponse, error) {
		sawPermitContext = ctx.Value(responseTestContextKey{}) == "permit-owned"
		sawRequest = request
		header := responseTestJSONHeader()
		header.Set("Content-Length", "999999")
		return UpstreamResponse{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       upstreamBody,
		}, nil
	})
	handler, permit, gate := responseTestNewHandler(t, upstream, permitContext, 512, time.Second)

	requestContext := context.WithValue(context.Background(), responseTestContextKey{}, "inbound")
	request := responseTestNewRequest().WithContext(requestContext)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != responseBody {
		t.Fatalf("body = %q, want exact %q", got, responseBody)
	}
	wantHeader := http.Header{
		"Cache-Control":          []string{"no-store"},
		"Content-Length":         []string{strconv.Itoa(len(responseBody))},
		"Content-Type":           []string{"application/json"},
		"X-Content-Type-Options": []string{"nosniff"},
		requestIDHeader:          []string{responseTestRequestID},
	}
	if got := recorder.Header(); !reflect.DeepEqual(got, wantHeader) {
		t.Fatalf("headers = %#v, want %#v", got, wantHeader)
	}
	if !sawPermitContext {
		t.Fatal("upstream context does not descend from permit.Context()")
	}
	if sawRequest.Model() != "local-model" || !bytes.Equal(sawRequest.BodyCopy(), []byte(responseTestRequest)) {
		t.Fatal("upstream did not receive the exact validated request")
	}
	if upstreamBody.closeCount() != 1 {
		t.Fatalf("upstream body close count = %d, want 1", upstreamBody.closeCount())
	}
	gate.mu.Lock()
	gateCalls := gate.calls
	gate.mu.Unlock()
	if gateCalls != 1 {
		t.Fatalf("admission gate calls = %d, want 1", gateCalls)
	}
	responseTestRequireOutcome(t, permit, admission.ServingCompleted)
}

func TestHandlerRejectsUnsafeUpstreamResponsesWithoutLeakage(t *testing.T) {
	const (
		bodyCanary  = "UPSTREAM_BODY_SECRET"
		errorCanary = "UPSTREAM_ERROR_SECRET"
	)

	tests := []struct {
		name            string
		status          int
		header          http.Header
		body            string
		nilBody         bool
		readErr         error
		closeErr        error
		upstreamErr     error
		maxResponseBody uint64
		wantCloseCount  int32
	}{
		{
			name:            "non-200 status",
			status:          http.StatusTooManyRequests,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "missing content type",
			status:          http.StatusOK,
			header:          http.Header{"X-Upstream-Secret": []string{"UPSTREAM_HEADER_SECRET"}},
			body:            `{"object":"chat.completion"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "invalid content type",
			status:          http.StatusOK,
			header:          http.Header{"Content-Type": []string{"text/plain"}},
			body:            `{"object":"chat.completion"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:   "content encoding",
			status: http.StatusOK,
			header: http.Header{
				"Content-Type":     []string{"application/json"},
				"Content-Encoding": []string{"gzip"},
			},
			body:            `{"object":"chat.completion"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "nil body",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			nilBody:         true,
			maxResponseBody: 256,
			wantCloseCount:  0,
		},
		{
			name:            "malformed JSON",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "trailing JSON",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion"}{"leak":"` + bodyCanary + `"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "wrong object",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion.chunk","leak":"` + bodyCanary + `"}`,
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "oversize body",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            strings.Repeat(bodyCanary, 20),
			maxResponseBody: 64,
			wantCloseCount:  1,
		},
		{
			name:            "body read error",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion"}`,
			readErr:         errors.New(errorCanary),
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "body close error",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion"}`,
			closeErr:        errors.New(errorCanary),
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
		{
			name:            "upstream error with body",
			status:          http.StatusOK,
			header:          responseTestJSONHeader(),
			body:            `{"object":"chat.completion","leak":"` + bodyCanary + `"}`,
			upstreamErr:     errors.New(errorCanary),
			maxResponseBody: 256,
			wantCloseCount:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var body *responseTestBody
			var responseBody io.ReadCloser
			if !test.nilBody {
				body = &responseTestBody{
					reader:   strings.NewReader(test.body),
					readErr:  test.readErr,
					closeErr: test.closeErr,
				}
				responseBody = body
			}
			upstream := responseTestUpstreamFunc(func(context.Context, contract.Request) (UpstreamResponse, error) {
				return UpstreamResponse{
					StatusCode: test.status,
					Header:     test.header.Clone(),
					Body:       responseBody,
				}, test.upstreamErr
			})
			handler, permit, _ := responseTestNewHandler(
				t,
				upstream,
				context.Background(),
				test.maxResponseBody,
				time.Second,
			)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, responseTestNewRequest())

			if recorder.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusBadGateway, recorder.Body.String())
			}
			const wantBody = `{"error":{"code":"invalid_upstream_response","message":"The upstream response could not be safely relayed."}}` + "\n"
			if got := recorder.Body.String(); got != wantBody {
				t.Fatalf("body = %q, want exact %q", got, wantBody)
			}
			if strings.Contains(recorder.Body.String(), bodyCanary) || strings.Contains(recorder.Body.String(), errorCanary) {
				t.Fatal("public response leaks upstream body or error data")
			}
			if got := recorder.Header().Get("X-Upstream-Secret"); got != "" {
				t.Fatalf("public headers leak upstream header %q", got)
			}
			if got := recorder.Header().Get("Content-Encoding"); got != "" {
				t.Fatalf("public response relays upstream content encoding %q", got)
			}
			if got := recorder.Header().Get(requestIDHeader); got != responseTestRequestID {
				t.Fatalf("request ID = %q, want %q", got, responseTestRequestID)
			}
			if body != nil && body.closeCount() != test.wantCloseCount {
				t.Fatalf("upstream body close count = %d, want %d", body.closeCount(), test.wantCloseCount)
			}
			responseTestRequireOutcome(t, permit, admission.ServingUpstreamFailed)
		})
	}
}

func TestHandlerMapsUpstreamDeadlineToStaticTimeout(t *testing.T) {
	var deadlineSet atomic.Bool
	upstream := responseTestUpstreamFunc(func(ctx context.Context, _ contract.Request) (UpstreamResponse, error) {
		_, hasDeadline := ctx.Deadline()
		deadlineSet.Store(hasDeadline)
		select {
		case <-ctx.Done():
			return UpstreamResponse{}, errors.New("UPSTREAM_TIMEOUT_SECRET")
		case <-time.After(time.Second):
			return UpstreamResponse{}, errors.New("upstream context was never canceled")
		}
	})
	handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 256, 20*time.Millisecond)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, responseTestNewRequest())

	if !deadlineSet.Load() {
		t.Fatal("upstream context has no policy deadline")
	}
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusGatewayTimeout, recorder.Body.String())
	}
	const wantBody = `{"error":{"code":"upstream_timeout","message":"The upstream did not complete before its deadline."}}` + "\n"
	if got := recorder.Body.String(); got != wantBody {
		t.Fatalf("body = %q, want exact %q", got, wantBody)
	}
	if strings.Contains(recorder.Body.String(), "UPSTREAM_TIMEOUT_SECRET") {
		t.Fatal("public timeout response leaks upstream error data")
	}
	responseTestRequireOutcome(t, permit, admission.ServingUpstreamFailed)
}

func TestHandlerMarksCommittedWriteFailuresAsDownstreamFailures(t *testing.T) {
	const responseBody = `{"object":"chat.completion","choices":[]}`

	tests := []struct {
		name      string
		configure func(*responseTestFailWriter)
	}{
		{
			name: "short write",
			configure: func(writer *responseTestFailWriter) {
				writer.short = true
			},
		},
		{
			name: "write error",
			configure: func(writer *responseTestFailWriter) {
				writer.writeErr = errors.New("DOWNSTREAM_WRITE_SECRET")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstreamBody := &responseTestBody{reader: strings.NewReader(responseBody)}
			upstream := responseTestUpstreamFunc(func(context.Context, contract.Request) (UpstreamResponse, error) {
				return UpstreamResponse{
					StatusCode: http.StatusOK,
					Header:     responseTestJSONHeader(),
					Body:       upstreamBody,
				}, nil
			})
			handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 256, time.Second)
			writer := newResponseTestFailWriter()
			test.configure(writer)
			handler.ServeHTTP(writer, responseTestNewRequest())

			if writer.status != http.StatusOK {
				t.Fatalf("status = %d, want %d", writer.status, http.StatusOK)
			}
			if got := writer.header.Get("Content-Length"); got != strconv.Itoa(len(responseBody)) {
				t.Fatalf("Content-Length = %q, want %d", got, len(responseBody))
			}
			if strings.Contains(writer.body.String(), `"error"`) {
				t.Fatal("handler appended an error envelope after response commit")
			}
			if upstreamBody.closeCount() != 1 {
				t.Fatalf("upstream body close count = %d, want 1", upstreamBody.closeCount())
			}
			responseTestRequireOutcome(t, permit, admission.ServingDownstreamFailed)
		})
	}
}
