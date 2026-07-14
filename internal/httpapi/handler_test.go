package httpapi

import (
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
	coreTestTenantOne = admission.TenantID(41)
	coreTestTenantTwo = admission.TenantID(42)
	coreTestTokenOne  = "core-tenant-one-token"
	coreTestTokenTwo  = "core-tenant-two-token=="
	coreTestRequestID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	coreTestRequest   = `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8}`
)

type coreTestPermit struct {
	ctx context.Context

	mu       sync.Mutex
	outcomes []admission.ServingOutcome
}

func (p *coreTestPermit) Context() context.Context { return p.ctx }

func (p *coreTestPermit) Finish(outcome admission.ServingOutcome) admission.TerminalResult {
	p.mu.Lock()
	p.outcomes = append(p.outcomes, outcome)
	p.mu.Unlock()
	return admission.TerminalResult{AccountingReleased: true}
}

func (p *coreTestPermit) recordedOutcomes() []admission.ServingOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]admission.ServingOutcome(nil), p.outcomes...)
}

type coreTestGate struct {
	permit   workPermit
	decision admission.Decision
	acquire  func(context.Context, admission.Admission) (workPermit, admission.Decision)

	mu         sync.Mutex
	calls      int
	admissions []admission.Admission
}

func (g *coreTestGate) Acquire(ctx context.Context, request admission.Admission) (workPermit, admission.Decision) {
	g.mu.Lock()
	g.calls++
	g.admissions = append(g.admissions, request)
	g.mu.Unlock()
	if g.acquire != nil {
		return g.acquire(ctx, request)
	}
	return g.permit, g.decision
}

func (g *coreTestGate) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

func (g *coreTestGate) recordedAdmissions() []admission.Admission {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]admission.Admission(nil), g.admissions...)
}

type coreTestUpstream struct {
	complete func(context.Context, contract.Request) (UpstreamResponse, error)
	calls    atomic.Int32
}

func (u *coreTestUpstream) Complete(ctx context.Context, request contract.Request) (UpstreamResponse, error) {
	u.calls.Add(1)
	if u.complete != nil {
		return u.complete(ctx, request)
	}
	return UpstreamResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"object":"chat.completion","choices":[]}`)),
	}, nil
}

type coreTestUnreadBody struct {
	reads  atomic.Int32
	closes atomic.Int32
}

func (b *coreTestUnreadBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, errors.New("early rejection unexpectedly read the request body")
}

func (b *coreTestUnreadBody) Close() error {
	b.closes.Add(1)
	return nil
}

type coreTestBlockingBody struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	closes    atomic.Int32
}

func newCoreTestBlockingBody() *coreTestBlockingBody {
	return &coreTestBlockingBody{started: make(chan struct{}), closed: make(chan struct{})}
}

func (b *coreTestBlockingBody) Read([]byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.closed
	return 0, errors.New("body closed")
}

func (b *coreTestBlockingBody) Close() error {
	b.closes.Add(1)
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

type coreTestPanicCloseBody struct {
	io.Reader
}

func (coreTestPanicCloseBody) Close() error { panic("close panic canary") }

func coreTestNewHandler(t *testing.T) (*Handler, *coreTestGate, *coreTestUpstream, *coreTestPermit) {
	t.Helper()
	parser, err := contract.NewParser("local-model", contract.Limits{
		MaxBodyBytes:        1024,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 256,
		MaxCompletionTokens: 128,
		CompletionWeight:    2,
		MaxRequestUnits:     2048,
	})
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	validator, err := contract.NewResponseValidator(contract.ResponseLimits{MaxBodyBytes: 1024})
	if err != nil {
		t.Fatalf("NewResponseValidator() error = %v", err)
	}
	permit := &coreTestPermit{ctx: context.Background()}
	gate := &coreTestGate{
		permit:   permit,
		decision: admission.Decision{Kind: admission.DecisionDispatched},
	}
	upstream := &coreTestUpstream{}
	oneDigest := sha256.Sum256([]byte(coreTestTokenOne))
	twoDigest := sha256.Sum256([]byte(coreTestTokenTwo))
	handler := &Handler{
		parser:              parser,
		gate:                gate,
		upstream:            upstream,
		responseValidator:   validator,
		defaultQueueTimeout: 250 * time.Millisecond,
		bodyReadTimeout:     time.Second,
		upstreamTimeout:     time.Second,
		globalSlots:         make(chan struct{}, 1),
		tenantSlots: map[admission.TenantID]chan struct{}{
			coreTestTenantOne: make(chan struct{}, 1),
			coreTestTenantTwo: make(chan struct{}, 1),
		},
		credentials: []storedCredential{
			{tenant: coreTestTenantOne, digest: oneDigest},
			{tenant: coreTestTenantTwo, digest: twoDigest},
		},
		requestIDs: func() (string, error) { return coreTestRequestID, nil },
	}
	return handler, gate, upstream, permit
}

func coreTestNewRequest(body io.ReadCloser) *http.Request {
	request := httptest.NewRequest(http.MethodPost, chatCompletionsPath, nil)
	if body == nil {
		body = io.NopCloser(strings.NewReader(coreTestRequest))
	}
	request.Body = body
	request.ContentLength = int64(len(coreTestRequest))
	request.Header.Set("Authorization", "Bearer "+coreTestTokenOne)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(requestIDHeader, "client-request-id-must-be-replaced")
	return request
}

func coreTestErrorBody(failure publicError) string {
	return `{"error":{"code":"` + failure.code + `","message":"` + failure.message + `"}}` + "\n"
}

func coreTestRequireStaticError(t *testing.T, recorder *httptest.ResponseRecorder, failure publicError) {
	t.Helper()
	result := recorder.Result()
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("ReadAll(response) error = %v", err)
	}
	if result.StatusCode != failure.status || string(body) != coreTestErrorBody(failure) {
		t.Fatalf("response = (%d, %q), want (%d, %q)", result.StatusCode, body, failure.status, coreTestErrorBody(failure))
	}
	if got := result.Header.Get(requestIDHeader); got != coreTestRequestID {
		t.Fatalf("%s = %q, want %q", requestIDHeader, got, coreTestRequestID)
	}
	if got := result.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := result.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := result.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := result.Header.Get("Retry-After"); got != "" {
		t.Fatalf("Retry-After = %q, want absent", got)
	}
}

func TestHandlerRejectsInStaticPrecedenceWithoutReadingBody(t *testing.T) {
	tests := []struct {
		name       string
		failure    publicError
		mutate     func(*Handler, *http.Request)
		wantHeader [2]string
	}{
		{
			name:    "path before every other field",
			failure: errUnsupportedPath,
			mutate: func(_ *Handler, request *http.Request) {
				request.URL.Path = chatCompletionsPath + "/"
				request.Method = http.MethodGet
				request.Header.Del("Authorization")
			},
		},
		{
			name:    "query alias is not the exact path",
			failure: errUnsupportedPath,
			mutate: func(_ *Handler, request *http.Request) {
				request.URL.RawQuery = "debug=true"
			},
		},
		{
			name:    "method before authentication",
			failure: errUnsupportedMethod,
			mutate: func(_ *Handler, request *http.Request) {
				request.Method = http.MethodGet
				request.Header.Del("Authorization")
			},
			wantHeader: [2]string{"Allow", http.MethodPost},
		},
		{
			name:    "authentication before media type",
			failure: errInvalidCredential,
			mutate: func(_ *Handler, request *http.Request) {
				request.Header.Del("Authorization")
				request.Header.Set("Content-Type", "text/plain")
			},
			wantHeader: [2]string{"WWW-Authenticate", "Bearer"},
		},
		{
			name:    "media type before queue header",
			failure: errUnsupportedMedia,
			mutate: func(_ *Handler, request *http.Request) {
				request.Header.Set("Content-Type", "text/plain")
				request.Header.Set(queueTimeoutHeader, "0")
			},
		},
		{
			name:    "queue header before body",
			failure: errInvalidRequest,
			mutate: func(_ *Handler, request *http.Request) {
				request.Header.Set(queueTimeoutHeader, "+1")
			},
		},
		{
			name:    "declared body size before slots",
			failure: errRequestTooLarge,
			mutate: func(handler *Handler, request *http.Request) {
				request.ContentLength = int64(handler.parser.MaxBodyBytes() + 1)
			},
		},
		{
			name:    "tenant slot before body",
			failure: errTenantCapacity,
			mutate: func(handler *Handler, _ *http.Request) {
				handler.tenantSlots[coreTestTenantOne] <- struct{}{}
			},
		},
		{
			name:    "global slot releases tenant slot",
			failure: errOverloaded,
			mutate: func(handler *Handler, _ *http.Request) {
				handler.globalSlots <- struct{}{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, gate, upstream, _ := coreTestNewHandler(t)
			body := &coreTestUnreadBody{}
			request := coreTestNewRequest(body)
			test.mutate(handler, request)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			coreTestRequireStaticError(t, recorder, test.failure)
			if test.wantHeader[0] != "" && recorder.Header().Get(test.wantHeader[0]) != test.wantHeader[1] {
				t.Fatalf("%s = %q, want %q", test.wantHeader[0], recorder.Header().Get(test.wantHeader[0]), test.wantHeader[1])
			}
			if got := body.reads.Load(); got != 0 {
				t.Fatalf("body reads = %d, want 0", got)
			}
			if got := body.closes.Load(); got != 1 {
				t.Fatalf("body closes = %d, want 1", got)
			}
			if gate.callCount() != 0 || upstream.calls.Load() != 0 {
				t.Fatalf("gate/upstream calls = %d/%d, want 0/0", gate.callCount(), upstream.calls.Load())
			}
			if test.name == "global slot releases tenant slot" && len(handler.tenantSlots[coreTestTenantOne]) != 0 {
				t.Fatal("global saturation leaked the tenant pre-dispatch slot")
			}
		})
	}
}

func TestHandlerRejectsAmbiguousAuthenticationAndMediaHeaders(t *testing.T) {
	tests := []struct {
		name    string
		failure publicError
		mutate  func(*http.Request)
	}{
		{name: "duplicate authorization", failure: errInvalidCredential, mutate: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+coreTestTokenOne) }},
		{name: "comma authorization", failure: errInvalidCredential, mutate: func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+coreTestTokenOne+",Bearer "+coreTestTokenTwo)
		}},
		{name: "tab after bearer", failure: errInvalidCredential, mutate: func(r *http.Request) { r.Header.Set("Authorization", "Bearer\t"+coreTestTokenOne) }},
		{name: "duplicate content type", failure: errUnsupportedMedia, mutate: func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }},
		{name: "unsupported charset", failure: errUnsupportedMedia, mutate: func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=iso-8859-1") }},
		{name: "content encoding", failure: errUnsupportedMedia, mutate: func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, gate, upstream, _ := coreTestNewHandler(t)
			body := &coreTestUnreadBody{}
			request := coreTestNewRequest(body)
			test.mutate(request)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			coreTestRequireStaticError(t, recorder, test.failure)
			if body.reads.Load() != 0 || gate.callCount() != 0 || upstream.calls.Load() != 0 {
				t.Fatal("ambiguous headers reached body parsing, admission, or upstream")
			}
		})
	}
}

func TestHandlerMapsAdmissionDecisionsWithoutCallingUpstream(t *testing.T) {
	tests := []struct {
		name        string
		decision    admission.Decision
		failure     *publicError
		withPermit  bool
		wantOutcome []admission.ServingOutcome
	}{
		{name: "tenant capacity", decision: admission.Decision{Kind: admission.DecisionTenantRejected}, failure: &errTenantCapacity},
		{name: "global capacity", decision: admission.Decision{Kind: admission.DecisionGlobalRejected}, failure: &errOverloaded},
		{name: "queue expiry", decision: admission.Decision{Kind: admission.DecisionQueueExpired}, failure: &errQueueDeadline},
		{name: "queued shutdown", decision: admission.Decision{Kind: admission.DecisionShutdown}, failure: &errDraining},
		{name: "already draining", decision: admission.Decision{Kind: admission.DecisionDraining}, failure: &errDraining},
		{name: "invalid decision", decision: admission.Decision{Kind: admission.DecisionInvalid}, failure: &errInternal},
		{name: "dispatched without permit", decision: admission.Decision{Kind: admission.DecisionDispatched}, failure: &errInternal},
		{name: "queued cancellation", decision: admission.Decision{Kind: admission.DecisionCanceledQueued}},
		{name: "before-start cancellation", decision: admission.Decision{Kind: admission.DecisionCanceledBeforeStart}},
		{
			name:        "unexpected permit is released",
			decision:    admission.Decision{Kind: admission.DecisionInvalid},
			failure:     &errInternal,
			withPermit:  true,
			wantOutcome: []admission.ServingOutcome{admission.ServingInternalFailure},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, gate, upstream, permit := coreTestNewHandler(t)
			gate.decision = test.decision
			gate.permit = nil
			if test.withPermit {
				gate.permit = permit
			}
			request := coreTestNewRequest(nil)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if test.failure != nil {
				coreTestRequireStaticError(t, recorder, *test.failure)
			} else if recorder.Body.Len() != 0 {
				t.Fatalf("canceled admission wrote %q, want no body", recorder.Body.String())
			}
			if upstream.calls.Load() != 0 {
				t.Fatalf("upstream calls = %d, want 0", upstream.calls.Load())
			}
			if got := permit.recordedOutcomes(); !reflect.DeepEqual(got, test.wantOutcome) {
				t.Fatalf("Finish outcomes = %v, want %v", got, test.wantOutcome)
			}
			if len(handler.globalSlots) != 0 || len(handler.tenantSlots[coreTestTenantOne]) != 0 {
				t.Fatal("admission decision leaked pre-dispatch capacity")
			}
		})
	}
}

func TestHandlerUsesDefaultOrStrictlyBoundedQueueTimeout(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		want       time.Duration
		wantReject bool
	}{
		{name: "default", want: 250 * time.Millisecond},
		{name: "client shorter", header: "17", want: 17 * time.Millisecond},
		{name: "equal default", header: "250", want: 250 * time.Millisecond},
		{name: "zero", header: "0", wantReject: true},
		{name: "negative", header: "-1", wantReject: true},
		{name: "fraction", header: "1.5", wantReject: true},
		{name: "over default", header: "251", wantReject: true},
		{name: "overflow length", header: "99999999999", wantReject: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, gate, _, _ := coreTestNewHandler(t)
			request := coreTestNewRequest(nil)
			if test.header != "" {
				request.Header.Set(queueTimeoutHeader, test.header)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			if test.wantReject {
				coreTestRequireStaticError(t, recorder, errInvalidRequest)
				if gate.callCount() != 0 {
					t.Fatalf("gate calls = %d, want 0", gate.callCount())
				}
				return
			}
			admissions := gate.recordedAdmissions()
			if len(admissions) != 1 || admissions[0].QueueTimeout != test.want {
				t.Fatalf("admissions = %+v, want queue timeout %s", admissions, test.want)
			}
		})
	}
}

func TestHandlerCancellationClosesBlockedBodyAndReleasesSlots(t *testing.T) {
	handler, gate, upstream, _ := coreTestNewHandler(t)
	body := newCoreTestBlockingBody()
	request := coreTestNewRequest(body)
	requestContext, cancel := context.WithCancel(request.Context())
	request = request.WithContext(requestContext)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-body.started:
	case <-time.After(2 * time.Second):
		t.Fatal("request body read did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after cancellation")
	}

	if recorder.Body.Len() != 0 {
		t.Fatalf("canceled request wrote %q, want no body", recorder.Body.String())
	}
	if body.closes.Load() != 1 {
		t.Fatalf("body closes = %d, want 1", body.closes.Load())
	}
	if gate.callCount() != 0 || upstream.calls.Load() != 0 {
		t.Fatal("canceled body read reached admission or upstream")
	}
	if len(handler.globalSlots) != 0 || len(handler.tenantSlots[coreTestTenantOne]) != 0 {
		t.Fatal("canceled body read leaked pre-dispatch capacity")
	}
}

func TestHandlerQueuedCancellationWritesNothingAndReleasesSlots(t *testing.T) {
	handler, gate, upstream, _ := coreTestNewHandler(t)
	started := make(chan struct{})
	gate.acquire = func(ctx context.Context, _ admission.Admission) (workPermit, admission.Decision) {
		close(started)
		<-ctx.Done()
		return nil, admission.Decision{Kind: admission.DecisionCanceledQueued}
	}
	request := coreTestNewRequest(nil)
	requestContext, cancel := context.WithCancel(request.Context())
	request = request.WithContext(requestContext)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after queued cancellation")
	}

	if recorder.Body.Len() != 0 || upstream.calls.Load() != 0 {
		t.Fatal("queued cancellation wrote a response or reached upstream")
	}
	if len(handler.globalSlots) != 0 || len(handler.tenantSlots[coreTestTenantOne]) != 0 {
		t.Fatal("queued cancellation leaked pre-dispatch capacity")
	}
}

func TestHandlerInflightClientCancellationWritesNothing(t *testing.T) {
	handler, _, upstream, permit := coreTestNewHandler(t)
	started := make(chan struct{})
	upstream.complete = func(ctx context.Context, _ contract.Request) (UpstreamResponse, error) {
		close(started)
		<-ctx.Done()
		return UpstreamResponse{}, ctx.Err()
	}
	request := coreTestNewRequest(nil)
	requestContext, cancel := context.WithCancel(request.Context())
	request = request.WithContext(requestContext)
	permit.ctx = request.Context()
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after in-flight cancellation")
	}

	if recorder.Body.Len() != 0 {
		t.Fatalf("client cancellation wrote %q, want no body", recorder.Body.String())
	}
	if got := permit.recordedOutcomes(); !reflect.DeepEqual(got, []admission.ServingOutcome{admission.ServingCanceled}) {
		t.Fatalf("Finish outcomes = %v, want canceled", got)
	}
}

func TestHandlerRecoversPanicsAndFinishesPermit(t *testing.T) {
	t.Run("upstream panic", func(t *testing.T) {
		handler, _, upstream, permit := coreTestNewHandler(t)
		upstream.complete = func(context.Context, contract.Request) (UpstreamResponse, error) {
			panic("upstream panic canary")
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, coreTestNewRequest(nil))

		coreTestRequireStaticError(t, recorder, errInternal)
		if got := permit.recordedOutcomes(); !reflect.DeepEqual(got, []admission.ServingOutcome{admission.ServingInternalFailure}) {
			t.Fatalf("Finish outcomes = %v, want internal failure", got)
		}
	})

	t.Run("final request body close panic", func(t *testing.T) {
		handler, _, _, _ := coreTestNewHandler(t)
		request := coreTestNewRequest(coreTestPanicCloseBody{Reader: strings.NewReader(coreTestRequest)})
		request.URL.Path = "/unsupported"
		recorder := httptest.NewRecorder()

		handler.ServeHTTP(recorder, request)

		coreTestRequireStaticError(t, recorder, errUnsupportedPath)
	})
}
