package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

func TestForcedShutdownPreservesSchedulerTerminalAttribution(t *testing.T) {
	scheduler := newDrainIntegrationScheduler(t)
	permit, decision := scheduler.Acquire(context.Background(), admission.Admission{
		Tenant:       1,
		BodyBytes:    1,
		WorkUnits:    1,
		QueueTimeout: time.Second,
	})
	if permit == nil || decision.Kind != admission.DecisionDispatched {
		t.Fatalf("Acquire() = (%v, %+v), want dispatched permit", permit, decision)
	}

	started := make(chan struct{})
	terminal := make(chan admission.TerminalResult, 1)
	inner := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-permit.Context().Done()
		terminal <- permit.Finish(admission.ServingCanceled)
	})
	observedScheduler := &drainIntegrationSchedulerObserver{Scheduler: scheduler}
	underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
	validated, err := validateConfig(Config{
		HeaderReadTimeout:       time.Second,
		ResponseWriteTimeout:    time.Second,
		IdleTimeout:             time.Second,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}, validDrainIntegrationPolicy())
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	idle := &drainIntegrationIdleCloser{}
	server := newServer(validated, underlying, inner, observedScheduler, idle)
	forcedHTTP := &drainIntegrationHTTP{permitContext: permit.Context()}
	server.http = forcedHTTP

	handlerReturned := make(chan struct{})
	go func() {
		server.handlers.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		)
		close(handlerReturned)
	}()
	receive(t, started, "active permit handler")

	result, err := server.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !result.Forced {
		t.Fatal("Shutdown() Forced = false, want true")
	}
	if result.Drain.InFlightActiveAtStart != 1 {
		t.Fatalf("drain in-flight count = %d, want 1", result.Drain.InFlightActiveAtStart)
	}
	if result.Force.NewlyShutdownSignaled != 1 {
		t.Fatalf("force-signaled count = %d, want 1", result.Force.NewlyShutdownSignaled)
	}
	if got := receive(t, terminal, "permit terminal result"); got.Outcome != admission.TerminalShutdown || !got.AccountingReleased {
		t.Fatalf("permit terminal result = %+v, want released shutdown", got)
	}
	receive(t, handlerReturned, "tracked handler return")
	if !forcedHTTP.closeObservedShutdown.Load() {
		t.Fatal("HTTP Close ran before scheduler shutdown cancellation was observable")
	}
	if observedScheduler.snapshotErr != nil {
		t.Fatalf("Snapshot() before scheduler Close error = %v", observedScheduler.snapshotErr)
	}
	if observedScheduler.beforeClose.Global != (admission.Counters{}) {
		t.Fatalf("scheduler counters before Close = %+v, want zero", observedScheduler.beforeClose.Global)
	}
	if observedScheduler.beforeClose.Accepting {
		t.Fatal("scheduler was still accepting before Close")
	}
	if idle.calls.Load() < 1 {
		t.Fatal("upstream idle connections were not closed")
	}
}

type drainIntegrationHTTP struct {
	permitContext         context.Context
	closeObservedShutdown atomic.Bool
}

func (*drainIntegrationHTTP) Serve(net.Listener) error {
	return errors.New("drain integration Serve must not be called")
}

func (*drainIntegrationHTTP) Shutdown(ctx context.Context) error {
	return errors.New("graceful HTTP shutdown failed")
}

func (h *drainIntegrationHTTP) Close() error {
	select {
	case <-h.permitContext.Done():
		h.closeObservedShutdown.Store(true)
	default:
	}
	return nil
}

type drainIntegrationSchedulerObserver struct {
	*admission.Scheduler
	beforeClose admission.Snapshot
	snapshotErr error
}

func (s *drainIntegrationSchedulerObserver) Close(ctx context.Context) error {
	s.beforeClose, s.snapshotErr = s.Scheduler.Snapshot(context.Background())
	return s.Scheduler.Close(ctx)
}

type drainIntegrationIdleCloser struct {
	calls atomic.Int64
}

func (c *drainIntegrationIdleCloser) CloseIdleConnections() {
	c.calls.Add(1)
}

func newDrainIntegrationScheduler(t *testing.T) *admission.Scheduler {
	t.Helper()
	scheduler, err := admission.New(admission.Config{
		MaxBodyBytes:    128,
		MaxRequestUnits: 256,
		BaseQuantum:     256,
		DeficitCap:      512,
		GlobalQueue:     admission.QueueLimits{Count: 2, Bytes: 256, Work: 512},
		GlobalInflight:  admission.InflightLimits{Count: 1, Work: 256},
		Tenants: []admission.TenantConfig{{
			ID:       1,
			Weight:   1,
			Queue:    admission.QueueLimits{Count: 2, Bytes: 256, Work: 512},
			Inflight: admission.InflightLimits{Count: 1, Work: 256},
		}},
	})
	if err != nil {
		t.Fatalf("admission.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Close(ctx); err != nil {
			t.Errorf("Scheduler.Close() cleanup error = %v", err)
		}
	})
	return scheduler
}

func validDrainIntegrationPolicy() httpapi.TimeoutPolicy {
	return httpapi.TimeoutPolicy{
		DefaultQueueTimeout: time.Second,
		BodyReadTimeout:     time.Second,
		UpstreamTimeout:     time.Second,
	}
}
