package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestTrackedHandlerAccountsForActiveHandlerUntilReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		returned := make(chan struct{})
		handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(started)
			<-release
		}))

		go func() {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			close(returned)
		}()
		<-started
		handler.seal()

		waited := make(chan error, 1)
		go func() {
			waited <- handler.wait(context.Background())
		}()
		synctest.Wait()
		select {
		case err := <-waited:
			t.Fatalf("wait() returned while inner handler was active: %v", err)
		default:
		}

		close(release)
		<-returned
		if err := <-waited; err != nil {
			t.Fatalf("wait() error after handler return = %v", err)
		}
	})
}

func TestTrackedHandlerSealRejectsNewWork(t *testing.T) {
	var calls atomic.Int64
	handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	handler.seal()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))

	if got := recorder.Code; got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Connection"); got != "close" {
		t.Fatalf("Connection header = %q, want %q", got, "close")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("inner handler calls = %d, want 0", got)
	}
	if err := handler.wait(context.Background()); err != nil {
		t.Fatalf("wait() after seal error = %v", err)
	}
}

func TestTrackedHandlerWaitRejectsNilContext(t *testing.T) {
	handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	if err := handler.wait(nil); !errors.Is(err, errHandlerWaitContextNil) {
		t.Fatalf("wait(nil) error = %v, want %v", err, errHandlerWaitContextNil)
	}
}

func TestTrackedHandlerWaitHonorsContextTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		if err := handler.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait() error = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func TestTrackedHandlerCompletedDrainWinsCanceledContextRace(t *testing.T) {
	handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	handler.seal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := handler.wait(ctx); err != nil {
		t.Fatalf("wait() error with completed drain and canceled context = %v, want nil", err)
	}
}

func TestTrackedHandlerConcurrentFinishAndSeal(t *testing.T) {
	const handlerCount = 64
	const sealerCount = 64

	started := make(chan struct{}, handlerCount)
	release := make(chan struct{})
	returned := make(chan struct{}, handlerCount)
	handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		started <- struct{}{}
		<-release
	}))

	for range handlerCount {
		go func() {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			returned <- struct{}{}
		}()
	}
	for range handlerCount {
		receiveTrackedHandlerTest(t, started, "inner handler start")
	}

	startRace := make(chan struct{})
	var sealers sync.WaitGroup
	sealers.Add(sealerCount)
	for range sealerCount {
		go func() {
			defer sealers.Done()
			<-startRace
			handler.seal()
		}()
	}
	close(startRace)
	close(release)
	sealers.Wait()
	for range handlerCount {
		receiveTrackedHandlerTest(t, returned, "inner handler return")
	}

	if err := handler.wait(context.Background()); err != nil {
		t.Fatalf("wait() after concurrent finish and seal error = %v", err)
	}
	if got := handler.active; got != 0 {
		t.Fatalf("active handlers = %d, want 0", got)
	}
}

func TestTrackedHandlerPanicStillFinishesAccounting(t *testing.T) {
	wantPanic := "inner handler panic"
	handler := newTrackedHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(wantPanic)
	}))

	func() {
		defer func() {
			if got := recover(); got != wantPanic {
				t.Fatalf("recovered panic = %v, want %q", got, wantPanic)
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()

	handler.seal()
	if err := handler.wait(context.Background()); err != nil {
		t.Fatalf("wait() after inner panic error = %v", err)
	}
	if got := handler.active; got != 0 {
		t.Fatalf("active handlers after inner panic = %d, want 0", got)
	}
}

func receiveTrackedHandlerTest[T any](t *testing.T, channel <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		var zero T
		return zero
	}
}
