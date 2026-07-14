package app

import (
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestSubscribeSignalsRegistersTerminationSignalsAndStopsOnce(t *testing.T) {
	var notifyCalls int
	var notified chan<- os.Signal
	var registered []os.Signal
	var stopCalls atomic.Int32
	var stopped chan<- os.Signal

	events, stop, err := subscribeSignals(signalAPI{
		notify: func(destination chan<- os.Signal, signals ...os.Signal) {
			notifyCalls++
			notified = destination
			registered = append([]os.Signal(nil), signals...)
		},
		stop: func(destination chan<- os.Signal) {
			stopCalls.Add(1)
			stopped = destination
		},
	})
	if err != nil {
		t.Fatalf("subscribeSignals() error = %v, want nil", err)
	}
	if events == nil {
		t.Fatal("subscribeSignals() events = nil")
	}
	if stop == nil {
		t.Fatal("subscribeSignals() stop = nil")
	}
	if notifyCalls != 1 {
		t.Fatalf("notify calls = %d, want 1", notifyCalls)
	}
	if notified == nil {
		t.Fatal("notify destination = nil")
	}
	if capacity := cap(notified); capacity != 1 {
		t.Fatalf("notify destination capacity = %d, want 1", capacity)
	}
	if reflect.ValueOf(events).Pointer() != reflect.ValueOf(notified).Pointer() {
		t.Fatal("returned events channel differs from notify destination")
	}
	if len(registered) != 2 {
		t.Fatalf("registered signals = %d, want 2", len(registered))
	}
	if registered[0] != os.Interrupt || registered[1] != syscall.SIGTERM {
		t.Fatalf("registered signals = %v, want [SIGINT SIGTERM]", registered)
	}

	const callers = 64
	start := make(chan struct{})
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			stop()
		}()
	}
	close(start)
	callersDone.Wait()
	stop()
	stop()

	if calls := stopCalls.Load(); calls != 1 {
		t.Fatalf("underlying stop calls = %d, want 1", calls)
	}
	if stopped == nil {
		t.Fatal("underlying stop destination = nil")
	}
	if reflect.ValueOf(stopped).Pointer() != reflect.ValueOf(events).Pointer() {
		t.Fatal("underlying stop channel differs from returned events channel")
	}
}

func TestSubscribeSignalsRejectsIncompleteAPIWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name      string
		notifyNil bool
		stopNil   bool
	}{
		{name: "nil notify", notifyNil: true},
		{name: "nil stop", stopNil: true},
		{name: "nil notify and stop", notifyNil: true, stopNil: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var notifyCalls atomic.Int32
			var stopCalls atomic.Int32
			api := signalAPI{
				notify: func(chan<- os.Signal, ...os.Signal) { notifyCalls.Add(1) },
				stop:   func(chan<- os.Signal) { stopCalls.Add(1) },
			}
			if test.notifyNil {
				api.notify = nil
			}
			if test.stopNil {
				api.stop = nil
			}

			events, stop, err := subscribeSignals(api)

			if err != errGatewayStartFailed {
				t.Fatalf("subscribeSignals() error = %v, want exact static start failure", err)
			}
			if events != nil {
				t.Fatal("subscribeSignals() events != nil")
			}
			if stop != nil {
				t.Fatal("subscribeSignals() stop != nil")
			}
			if calls := notifyCalls.Load(); calls != 0 {
				t.Fatalf("notify calls = %d, want 0", calls)
			}
			if calls := stopCalls.Load(); calls != 0 {
				t.Fatalf("stop calls = %d, want 0", calls)
			}
		})
	}
}
