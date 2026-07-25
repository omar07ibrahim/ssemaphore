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

type integrationAcquireResult struct {
	permit   *admission.Permit
	decision admission.Decision
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

func TestNewHandlerMapsRealQueueExpiryWithoutTypedNilPermit(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	upstream := &integrationTestUpstream{}
	config := configTestBaseHandlerConfig()
	config.DefaultQueueTimeout = 250 * time.Millisecond
	handler, err := NewHandler(config, parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	blockingPermit := integrationAcquireBlockingPermit(t, scheduler, configTestTenantOne)
	defer blockingPermit.Finish(admission.ServingCompleted)

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, integrationTestRequestFor(context.Background()))
		close(done)
	}()

	waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global.QueuedCount == 1 && snapshot.Global.InflightCount == 1
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP did not return after real queue expiry")
	}

	assertIntegrationPublicError(t, recorder, errQueueDeadline)
	if upstream.calls.Load() != 0 {
		t.Fatalf("upstream calls = %d, want 0", upstream.calls.Load())
	}
	snapshot := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global.QueuedCount == 0 && snapshot.Global.InflightCount == 1
	})
	if snapshot.Global.QueuedBytes != 0 || snapshot.Global.QueuedWork != 0 {
		t.Fatalf("queued accounting after expiry = %+v, want zero queued resources", snapshot.Global)
	}
	if len(handler.globalSlots) != 0 || len(handler.tenantSlots[configTestTenantOne]) != 0 {
		t.Fatal("real queue expiry leaked pre-dispatch slots")
	}
}

func TestNewHandlerMapsRealCapacityRejectionsWithoutTypedNilPermit(t *testing.T) {
	tests := []struct {
		name             string
		mutateScheduler  func(*admission.Config)
		queuedTenants    []admission.TenantID
		wantTenantQueued uint64
		want             publicError
	}{
		{
			name:             "tenant queue",
			queuedTenants:    []admission.TenantID{configTestTenantOne, configTestTenantOne},
			wantTenantQueued: 2,
			want:             errTenantCapacity,
		},
		{
			name: "global queue",
			mutateScheduler: func(config *admission.Config) {
				config.GlobalQueue.Count = 3
			},
			queuedTenants: []admission.TenantID{
				configTestTenantOne,
				configTestTenantTwo,
				configTestTenantTwo,
			},
			wantTenantQueued: 1,
			want:             errOverloaded,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
			scheduler := configTestNewScheduler(t, test.mutateScheduler)
			upstream := &integrationTestUpstream{}
			handler, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, upstream)
			if err != nil {
				t.Fatalf("NewHandler() error = %v", err)
			}

			firstBlocker := integrationAcquireBlockingPermit(t, scheduler, configTestTenantOne)
			defer firstBlocker.Finish(admission.ServingCompleted)
			secondBlocker := integrationAcquireBlockingPermit(t, scheduler, configTestTenantTwo)
			defer secondBlocker.Finish(admission.ServingCompleted)

			cancels := make([]context.CancelFunc, 0, len(test.queuedTenants))
			results := make([]<-chan integrationAcquireResult, 0, len(test.queuedTenants))
			for _, tenant := range test.queuedTenants {
				cancel, result := integrationStartQueuedAcquire(scheduler, tenant)
				cancels = append(cancels, cancel)
				results = append(results, result)
			}
			defer func() {
				for _, cancel := range cancels {
					cancel()
				}
			}()
			waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
				if snapshot.Global.QueuedCount != uint64(len(test.queuedTenants)) ||
					snapshot.Global.InflightCount != 2 {
					return false
				}
				return integrationTenantQueuedCount(snapshot, configTestTenantOne) == test.wantTenantQueued
			})

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, integrationTestRequestFor(context.Background()))

			assertIntegrationPublicError(t, recorder, test.want)
			if upstream.calls.Load() != 0 {
				t.Fatalf("upstream calls = %d, want 0", upstream.calls.Load())
			}
			if len(handler.globalSlots) != 0 || len(handler.tenantSlots[configTestTenantOne]) != 0 {
				t.Fatal("real capacity rejection leaked pre-dispatch slots")
			}
			snapshot, err := scheduler.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() after capacity rejection error = %v", err)
			}
			queued := uint64(len(test.queuedTenants))
			wantGlobal := admission.Counters{
				QueuedCount:   queued,
				QueuedBytes:   queued,
				QueuedWork:    queued,
				InflightCount: 2,
				InflightWork:  2 * configTestMaxRequestUnits,
			}
			if snapshot.Global != wantGlobal ||
				integrationTenantQueuedCount(snapshot, configTestTenantOne) != test.wantTenantQueued {
				t.Fatalf(
					"accounting after capacity rejection = %+v, want global %+v and tenant queued %d",
					snapshot,
					wantGlobal,
					test.wantTenantQueued,
				)
			}

			integrationCancelQueuedAcquires(t, cancels, results)
			waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
				return snapshot.Global.QueuedCount == 0 && snapshot.Global.InflightCount == 2
			})
		})
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

	blockingPermit := integrationAcquireBlockingPermit(t, scheduler, configTestTenantOne)
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

func integrationAcquireBlockingPermit(
	t *testing.T,
	scheduler *admission.Scheduler,
	tenant admission.TenantID,
) *admission.Permit {
	t.Helper()
	permit, decision := scheduler.Acquire(context.Background(), admission.Admission{
		Tenant:       tenant,
		BodyBytes:    1,
		WorkUnits:    configTestMaxRequestUnits,
		QueueTimeout: time.Second,
	})
	if permit == nil || decision.Kind != admission.DecisionDispatched {
		t.Fatalf("blocking Acquire() = (%v, %+v), want dispatched", permit, decision)
	}
	return permit
}

func integrationStartQueuedAcquire(
	scheduler *admission.Scheduler,
	tenant admission.TenantID,
) (context.CancelFunc, <-chan integrationAcquireResult) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan integrationAcquireResult, 1)
	go func() {
		permit, decision := scheduler.Acquire(ctx, admission.Admission{
			Tenant:       tenant,
			BodyBytes:    1,
			WorkUnits:    1,
			QueueTimeout: 5 * time.Second,
		})
		result <- integrationAcquireResult{permit: permit, decision: decision}
	}()
	return cancel, result
}

func integrationCancelQueuedAcquires(
	t *testing.T,
	cancels []context.CancelFunc,
	results []<-chan integrationAcquireResult,
) {
	t.Helper()
	for _, cancel := range cancels {
		cancel()
	}
	for _, result := range results {
		select {
		case acquired := <-result:
			if acquired.permit != nil {
				acquired.permit.Finish(admission.ServingCanceled)
				t.Fatalf("queued Acquire() unexpectedly dispatched with decision %+v", acquired.decision)
			}
			if acquired.decision.Kind != admission.DecisionCanceledQueued {
				t.Fatalf("queued Acquire() decision = %+v, want canceled queued", acquired.decision)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queued Acquire() did not return after cancellation")
		}
	}
}

func integrationTenantQueuedCount(snapshot admission.Snapshot, tenant admission.TenantID) uint64 {
	for _, tenantSnapshot := range snapshot.Tenants {
		if tenantSnapshot.ID == tenant {
			return tenantSnapshot.Counters.QueuedCount
		}
	}
	return 0
}

func assertIntegrationPublicError(t *testing.T, recorder *httptest.ResponseRecorder, want publicError) {
	t.Helper()
	wantBody := `{"error":{"code":"` + want.code + `","message":"` + want.message + `"}}` + "\n"
	if recorder.Code != want.status || recorder.Body.String() != wantBody {
		t.Fatalf(
			"response = (%d, %q), want exact (%d, %q)",
			recorder.Code,
			recorder.Body.String(),
			want.status,
			wantBody,
		)
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
