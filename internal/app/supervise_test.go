package app

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

func TestSuperviseAcceptsOnlyTerminationSignalsAfterCompleteShutdown(t *testing.T) {
	for _, event := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(event.String(), func(t *testing.T) {
			serveStarted := make(chan struct{})
			releaseServe := make(chan struct{})
			stopCalled := make(chan struct{})
			var releaseOnce sync.Once
			var stopCalls atomic.Int32
			var shutdownSawStop atomic.Bool
			var shutdownSawDeadline atomic.Bool

			runtime := &runtimeServerStub{
				serve: func() error {
					close(serveStarted)
					<-releaseServe
					return nil
				},
				shutdown: func(ctx context.Context) (server.ShutdownResult, error) {
					select {
					case <-stopCalled:
						shutdownSawStop.Store(true)
					default:
					}
					if _, present := ctx.Deadline(); present && ctx.Err() == nil {
						shutdownSawDeadline.Store(true)
					}
					releaseOnce.Do(func() { close(releaseServe) })
					return server.ShutdownResult{Forced: true}, nil
				},
			}
			events := make(chan os.Signal, 1)
			events <- event

			got := supervise(runtime, events, func() {
				stopCalls.Add(1)
				close(stopCalled)
			}, time.Second)
			<-serveStarted

			if got != outcomeCleanShutdown {
				t.Fatalf("supervise() = %v, want clean shutdown", got)
			}
			if calls := runtime.serveCalls.Load(); calls != 1 {
				t.Fatalf("Serve calls = %d, want 1", calls)
			}
			if calls := runtime.shutdownCalls.Load(); calls != 1 {
				t.Fatalf("Shutdown calls = %d, want 1", calls)
			}
			if calls := stopCalls.Load(); calls != 1 {
				t.Fatalf("stop calls = %d, want 1", calls)
			}
			if !shutdownSawStop.Load() {
				t.Fatal("Shutdown started before stop")
			}
			if !shutdownSawDeadline.Load() {
				t.Fatal("Shutdown context was not independently bounded")
			}
		})
	}
}

func TestSuperviseTreatsUnexpectedServeReturnAsFailureAndStillShutsDown(t *testing.T) {
	for _, serveErr := range []error{nil, errors.New("serve canary secret")} {
		name := "nil"
		if serveErr != nil {
			name = "error"
		}
		t.Run(name, func(t *testing.T) {
			var stopCalls atomic.Int32
			runtime := &runtimeServerStub{
				serve: func() error { return serveErr },
				shutdown: func(context.Context) (server.ShutdownResult, error) {
					return server.ShutdownResult{}, nil
				},
			}

			got := supervise(runtime, make(chan os.Signal), func() { stopCalls.Add(1) }, time.Second)

			if got != outcomeFailure {
				t.Fatalf("supervise() = %v, want static failure", got)
			}
			if calls := runtime.shutdownCalls.Load(); calls != 1 {
				t.Fatalf("Shutdown calls = %d, want 1", calls)
			}
			if calls := stopCalls.Load(); calls != 1 {
				t.Fatalf("stop calls = %d, want 1", calls)
			}
		})
	}
}

func TestSuperviseRejectsOtherAndClosedSignalChannelsAfterShutdown(t *testing.T) {
	tests := []struct {
		name   string
		events func() <-chan os.Signal
	}{
		{
			name: "other signal",
			events: func() <-chan os.Signal {
				result := make(chan os.Signal, 1)
				result <- testSignal("signal canary secret")
				return result
			},
		},
		{
			name: "uncomparable signal",
			events: func() <-chan os.Signal {
				result := make(chan os.Signal, 1)
				result <- uncomparableTestSignal{1}
				return result
			},
		},
		{
			name: "closed channel",
			events: func() <-chan os.Signal {
				result := make(chan os.Signal)
				close(result)
				return result
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			releaseServe := make(chan struct{})
			runtime := &runtimeServerStub{
				serve: func() error {
					<-releaseServe
					return nil
				},
				shutdown: func(context.Context) (server.ShutdownResult, error) {
					close(releaseServe)
					return server.ShutdownResult{}, nil
				},
			}
			var stopCalls atomic.Int32

			got := supervise(runtime, test.events(), func() { stopCalls.Add(1) }, time.Second)

			if got != outcomeFailure {
				t.Fatalf("supervise() = %v, want static failure", got)
			}
			if calls := runtime.shutdownCalls.Load(); calls != 1 {
				t.Fatalf("Shutdown calls = %d, want 1", calls)
			}
			if calls := stopCalls.Load(); calls != 1 {
				t.Fatalf("stop calls = %d, want 1", calls)
			}
		})
	}
}

func TestSuperviseShutdownFailureIsStatic(t *testing.T) {
	releaseServe := make(chan struct{})
	runtime := &runtimeServerStub{
		serve: func() error {
			<-releaseServe
			return nil
		},
		shutdown: func(context.Context) (server.ShutdownResult, error) {
			close(releaseServe)
			return server.ShutdownResult{}, errors.New("shutdown canary secret")
		},
	}
	events := make(chan os.Signal, 1)
	events <- os.Interrupt

	if got := supervise(runtime, events, func() {}, time.Second); got != outcomeFailure {
		t.Fatalf("supervise() = %v, want static failure", got)
	}
}

func TestSuperviseBoundsServeJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serveStarted := make(chan struct{})
		releaseServe := make(chan struct{})
		runtime := &runtimeServerStub{
			serve: func() error {
				close(serveStarted)
				<-releaseServe
				return nil
			},
			shutdown: func(context.Context) (server.ShutdownResult, error) {
				return server.ShutdownResult{}, nil
			},
		}
		events := make(chan os.Signal, 1)
		events <- os.Interrupt
		result := make(chan outcome, 1)

		go func() {
			result <- supervise(runtime, events, func() {}, time.Second)
		}()
		<-serveStarted
		if got := <-result; got != outcomeFailure {
			t.Fatalf("supervise() = %v, want bounded join failure", got)
		}
		close(releaseServe)
		synctest.Wait()
	})
}

func TestSuperviseValidatesDependenciesAndWaitBound(t *testing.T) {
	validRuntime := &runtimeServerStub{
		serve: func() error { panic("Serve called after failed validation") },
		shutdown: func(context.Context) (server.ShutdownResult, error) {
			panic("Shutdown called after failed validation")
		},
	}
	validEvents := make(chan os.Signal)
	validStop := func() { panic("stop called after failed validation") }
	var typedNil *runtimeServerStub
	tests := []struct {
		name    string
		runtime runtimeServer
		events  <-chan os.Signal
		stop    func()
		wait    time.Duration
	}{
		{name: "nil server", events: validEvents, stop: validStop, wait: time.Second},
		{name: "typed nil server", runtime: typedNil, events: validEvents, stop: validStop, wait: time.Second},
		{name: "nil event channel", runtime: validRuntime, stop: validStop, wait: time.Second},
		{name: "nil stop", runtime: validRuntime, events: validEvents, wait: time.Second},
		{name: "zero wait", runtime: validRuntime, events: validEvents, stop: validStop},
		{name: "negative wait", runtime: validRuntime, events: validEvents, stop: validStop, wait: -time.Second},
		{name: "wait above bound", runtime: validRuntime, events: validEvents, stop: validStop, wait: maximumShutdownWait + time.Nanosecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := supervise(test.runtime, test.events, test.stop, test.wait); got != outcomeFailure {
				t.Fatalf("supervise() = %v, want validation failure", got)
			}
		})
	}
}

type runtimeServerStub struct {
	serve    func() error
	shutdown func(context.Context) (server.ShutdownResult, error)

	serveCalls    atomic.Int32
	shutdownCalls atomic.Int32
}

func (s *runtimeServerStub) Serve() error {
	s.serveCalls.Add(1)
	return s.serve()
}

func (s *runtimeServerStub) Shutdown(ctx context.Context) (server.ShutdownResult, error) {
	s.shutdownCalls.Add(1)
	return s.shutdown(ctx)
}

type testSignal string

func (s testSignal) String() string { return string(s) }
func (testSignal) Signal()          {}

type uncomparableTestSignal []byte

func (uncomparableTestSignal) String() string { return "uncomparable signal" }
func (uncomparableTestSignal) Signal()        {}
