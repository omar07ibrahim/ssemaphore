package httpapi

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

func TestHandlerLifecyclePolicyAndSchedulerIdentity(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	ownedScheduler := configTestNewScheduler(t, nil)
	otherScheduler := configTestNewScheduler(t, nil)
	upstream := &lifecycleTestIdlePanicUpstream{}
	config := configTestBaseHandlerConfig()
	handler, err := NewHandler(config, parser, ownedScheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	if !handler.UsesScheduler(ownedScheduler) {
		t.Fatal("UsesScheduler(owned) = false, want true")
	}
	if handler.UsesScheduler(otherScheduler) || handler.UsesScheduler(nil) {
		t.Fatal("UsesScheduler accepted a scheduler the handler does not own")
	}
	if got := handler.TimeoutPolicy(); got != (TimeoutPolicy{
		DefaultQueueTimeout: config.DefaultQueueTimeout,
		BodyReadTimeout:     config.BodyReadTimeout,
		UpstreamTimeout:     config.UpstreamTimeout,
	}) {
		t.Fatalf("TimeoutPolicy() = %+v, want validated handler policy", got)
	}

	// A faulty optional resource hook must not panic out of the terminal server
	// cleanup goroutine. The call counter proves the hook was still attempted.
	handler.CloseIdleConnections()
	if got := upstream.closeCalls.Load(); got != 1 {
		t.Fatalf("CloseIdleConnections calls = %d, want 1", got)
	}
}

type lifecycleTestIdlePanicUpstream struct {
	closeCalls atomic.Int64
}

func (*lifecycleTestIdlePanicUpstream) Complete(context.Context, contract.Request) (UpstreamResponse, error) {
	panic("lifecycle test upstream must not complete")
}

func (u *lifecycleTestIdlePanicUpstream) CloseIdleConnections() {
	u.closeCalls.Add(1)
	panic("idle close panic canary")
}
