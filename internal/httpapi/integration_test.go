package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const integrationTestRequest = `{"model":"portfolio-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8}`

type integrationTestUpstream struct {
	calls atomic.Int32
}

func (u *integrationTestUpstream) Complete(context.Context, contract.Request) (UpstreamResponse, error) {
	u.calls.Add(1)
	return UpstreamResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"object":"chat.completion","choices":[]}`)),
	}, nil
}

func integrationTestRequestFor(ctx context.Context) *http.Request {
	request := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(integrationTestRequest))
	request = request.WithContext(ctx)
	request.Header.Set("Authorization", "Bearer tenant-one-primary")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestNewHandlerCompletesThroughRealScheduler(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	upstream := &integrationTestUpstream{}
	handler, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, integrationTestRequestFor(context.Background()))

	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"object":"chat.completion","choices":[]}` {
		t.Fatalf("response = (%d, %q), want exact 200 completion", recorder.Code, recorder.Body.String())
	}
	if !validRequestID(recorder.Header().Get(requestIDHeader)) {
		t.Fatalf("request ID = %q, want server-generated 128-bit hex", recorder.Header().Get(requestIDHeader))
	}
	if upstream.calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.calls.Load())
	}
	snapshot, err := scheduler.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.Global != (admission.Counters{}) {
		t.Fatalf("terminal global counters = %+v, want zero", snapshot.Global)
	}
}

func TestNewHandlerCancelsRealQueuedRequestBeforeUpstream(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	upstream := &integrationTestUpstream{}
	handler, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	blockingPermit, decision := scheduler.Acquire(context.Background(), admission.Admission{
		Tenant:       configTestTenantOne,
		BodyBytes:    1,
		WorkUnits:    configTestMaxRequestUnits,
		QueueTimeout: time.Second,
	})
	if blockingPermit == nil || decision.Kind != admission.DecisionDispatched {
		t.Fatalf("blocking Acquire() = (%v, %+v), want dispatched", blockingPermit, decision)
	}
	defer blockingPermit.Finish(admission.ServingCompleted)

	requestContext, cancelRequest := context.WithCancel(context.Background())
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, integrationTestRequestFor(requestContext))
		close(done)
	}()

	waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global.QueuedCount == 1 && snapshot.Global.InflightCount == 1
	})
	cancelRequest()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after real queued cancellation")
	}

	if recorder.Body.Len() != 0 {
		t.Fatalf("queued cancellation wrote %q, want no response body", recorder.Body.String())
	}
	if upstream.calls.Load() != 0 {
		t.Fatalf("upstream calls = %d, want 0", upstream.calls.Load())
	}
	snapshot := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global.QueuedCount == 0 && snapshot.Global.InflightCount == 1
	})
	if snapshot.Global.QueuedBytes != 0 || snapshot.Global.QueuedWork != 0 {
		t.Fatalf("queued accounting after cancellation = %+v, want zero queued resources", snapshot.Global)
	}
	if len(handler.globalSlots) != 0 || len(handler.tenantSlots[configTestTenantOne]) != 0 {
		t.Fatal("real queued cancellation leaked pre-dispatch slots")
	}
}

func waitForIntegrationSnapshot(
	t *testing.T,
	scheduler *admission.Scheduler,
	predicate func(admission.Snapshot) bool,
) admission.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := scheduler.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot() error = %v", err)
		}
		if predicate(snapshot) {
			return snapshot
		}
		runtime.Gosched()
	}
	t.Fatal("scheduler snapshot did not reach the expected state")
	return admission.Snapshot{}
}
