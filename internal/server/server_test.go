package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

func TestNewRejectsDependenciesAndUnsafeListenerWithoutTakingOwnership(t *testing.T) {
	tests := []struct {
		name      string
		address   net.Addr
		handler   *httpapi.Handler
		scheduler *admission.Scheduler
	}{
		{
			name:      "nil handler",
			address:   serverLifecycleLoopbackAddress(),
			scheduler: &admission.Scheduler{},
		},
		{
			name:    "nil scheduler",
			address: serverLifecycleLoopbackAddress(),
			handler: &httpapi.Handler{},
		},
		{
			name:      "unsafe listener",
			address:   &net.TCPAddr{IP: net.IPv4zero, Port: 8080},
			handler:   &httpapi.Handler{},
			scheduler: &admission.Scheduler{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener := newServerLifecycleListener(test.address)
			server, err := New(Config{}, listener, test.handler, test.scheduler)
			if err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
			if server != nil {
				t.Fatalf("New() server = %v, want nil", server)
			}
			if got := listener.closeCalls.Load(); got != 0 {
				t.Fatalf("listener Close calls after rejected New = %d, want 0", got)
			}
		})
	}
}

func TestNewServerConfiguresExactHTTPBoundaries(t *testing.T) {
	config := serverLifecycleConfig()
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	scheduler := &serverLifecycleScheduler{}
	upstream := &serverLifecycleIdleCloser{}
	server := newServer(
		config,
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)

	httpServer, ok := server.http.(*http.Server)
	if !ok {
		t.Fatalf("HTTP lifecycle type = %T, want *http.Server", server.http)
	}
	if httpServer.Handler != server.handlers {
		t.Fatal("HTTP Handler does not use the tracked handler")
	}
	if !httpServer.DisableGeneralOptionsHandler {
		t.Fatal("DisableGeneralOptionsHandler = false, want true")
	}
	if httpServer.ReadHeaderTimeout != config.headerReadTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", httpServer.ReadHeaderTimeout, config.headerReadTimeout)
	}
	if httpServer.ReadTimeout != config.readTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", httpServer.ReadTimeout, config.readTimeout)
	}
	if httpServer.WriteTimeout != config.writeTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", httpServer.WriteTimeout, config.writeTimeout)
	}
	if httpServer.IdleTimeout != config.idleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", httpServer.IdleTimeout, config.idleTimeout)
	}
	if httpServer.MaxHeaderBytes != config.netHTTPMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", httpServer.MaxHeaderBytes, config.netHTTPMaxHeaderBytes)
	}
	if httpServer.Protocols == nil || !httpServer.Protocols.HTTP1() || httpServer.Protocols.HTTP2() || httpServer.Protocols.UnencryptedHTTP2() {
		t.Fatalf("Protocols = %v, want HTTP/1 only", httpServer.Protocols)
	}
	if httpServer.ErrorLog == nil {
		t.Fatal("ErrorLog = nil, want discard logger")
	}
	if httpServer.ErrorLog.Writer() != io.Discard {
		t.Fatalf("ErrorLog writer = %T, want io.Discard", httpServer.ErrorLog.Writer())
	}
	if httpServer.BaseContext == nil {
		t.Fatal("BaseContext = nil, want server-owned context")
	}
	baseContext := httpServer.BaseContext(server.listener)
	if baseContext == nil || baseContext.Err() != nil {
		t.Fatalf("initial base context error = %v, want nil", baseContext.Err())
	}
	if got := cap(server.listener.slots); got != config.maxConnections {
		t.Fatalf("connection slot capacity = %d, want %d", got, config.maxConnections)
	}

	server.baseCancel()
	if !errors.Is(baseContext.Err(), context.Canceled) {
		t.Fatalf("base context error after cancellation = %v, want context canceled", baseContext.Err())
	}
	if err := server.listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
}

func TestServeUnexpectedFailureIsStaticAndStartsCleanup(t *testing.T) {
	injectedFailure := errors.New("injected serve failure")
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	scheduler := &serverLifecycleScheduler{}
	upstream := &serverLifecycleIdleCloser{}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)
	httpLifecycle := &serverLifecycleHTTP{
		serve: func(net.Listener) error { return injectedFailure },
	}
	server.http = httpLifecycle

	if err := server.Serve(); err != ErrServeFailed {
		t.Fatalf("Serve() error = %v, want static ErrServeFailed", err)
	}
	if _, err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() after Serve failure error = %v", err)
	}
	if got := httpLifecycle.serveCalls.Load(); got != 1 {
		t.Fatalf("HTTP Serve calls = %d, want 1", got)
	}
	if got := scheduler.beginCalls.Load(); got != 1 {
		t.Fatalf("BeginDrain calls = %d, want 1", got)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls = %d, want 1", got)
	}
	if err := server.Serve(); err != ErrServeAlreadyStarted {
		t.Fatalf("second Serve() error = %v, want ErrServeAlreadyStarted", err)
	}
	if got := httpLifecycle.serveCalls.Load(); got != 1 {
		t.Fatalf("HTTP Serve calls after second Serve = %d, want 1", got)
	}
}

func TestShutdownPreCanceledContextDoesNotStartCleanup(t *testing.T) {
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	scheduler := &serverLifecycleScheduler{}
	upstream := &serverLifecycleIdleCloser{}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)
	httpLifecycle := &serverLifecycleHTTP{}
	server.http = httpLifecycle

	if _, err := server.Shutdown(nil); err == nil {
		t.Fatal("Shutdown(nil) error = nil, want validation error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := server.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown(pre-canceled) error = %v, want context canceled", err)
	}
	if result != (ShutdownResult{}) {
		t.Fatalf("Shutdown(pre-canceled) result = %+v, want zero result", result)
	}
	if server.shutdownStarted.Load() {
		t.Fatal("pre-canceled Shutdown started cleanup")
	}
	if got := scheduler.totalCalls(); got != 0 {
		t.Fatalf("scheduler lifecycle calls = %d, want 0", got)
	}
	if got := httpLifecycle.totalCalls(); got != 0 {
		t.Fatalf("HTTP lifecycle calls = %d, want 0", got)
	}
	if got := listener.closeCalls.Load(); got != 0 {
		t.Fatalf("listener Close calls = %d, want 0", got)
	}

	server.baseCancel()
	if err := server.listener.Close(); err != nil {
		t.Fatalf("test cleanup listener Close() error = %v", err)
	}
}

func TestShutdownCallerTimeoutDoesNotCancelOwnedCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		beginEntered := make(chan struct{})
		releaseBegin := make(chan struct{})
		var beginOnce sync.Once
		wantDrain := admission.DrainResult{QueuedShutdownCanceled: 7, InFlightActiveAtStart: 3}
		scheduler := &serverLifecycleScheduler{
			begin: func(ctx context.Context) (admission.DrainResult, error) {
				beginOnce.Do(func() { close(beginEntered) })
				select {
				case <-releaseBegin:
					return wantDrain, nil
				case <-ctx.Done():
					return admission.DrainResult{}, ctx.Err()
				}
			},
		}
		listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
		server := newServer(
			serverLifecycleConfig(),
			listener,
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
			scheduler,
			&serverLifecycleIdleCloser{},
		)
		server.http = &serverLifecycleHTTP{}

		callerContext, cancelCaller := context.WithTimeout(context.Background(), time.Second)
		defer cancelCaller()
		firstResult := make(chan serverLifecycleShutdownCall, 1)
		go func() {
			result, err := server.Shutdown(callerContext)
			firstResult <- serverLifecycleShutdownCall{result: result, err: err}
		}()

		serverLifecycleReceive(t, beginEntered, "BeginDrain entry")
		first := serverLifecycleReceive(t, firstResult, "caller deadline")
		if !errors.Is(first.err, context.DeadlineExceeded) {
			t.Fatalf("first Shutdown() error = %v, want caller deadline", first.err)
		}
		if first.result != (ShutdownResult{}) {
			t.Fatalf("first Shutdown() result = %+v, want zero result", first.result)
		}
		select {
		case <-server.shutdownDone:
			t.Fatal("owned cleanup ended with the caller deadline")
		default:
		}

		close(releaseBegin)
		secondResult := make(chan serverLifecycleShutdownCall, 1)
		go func() {
			result, err := server.Shutdown(context.Background())
			secondResult <- serverLifecycleShutdownCall{result: result, err: err}
		}()
		second := serverLifecycleReceive(t, secondResult, "owned cleanup completion")
		if second.err != nil {
			t.Fatalf("second Shutdown() error = %v", second.err)
		}
		if second.result.Drain != wantDrain || second.result.Forced {
			t.Fatalf("second Shutdown() result = %+v, want graceful %+v", second.result, wantDrain)
		}
		if got := scheduler.beginCalls.Load(); got != 1 {
			t.Fatalf("BeginDrain calls = %d, want 1", got)
		}
	})
}

func TestConcurrentShutdownReturnsOneIdenticalResult(t *testing.T) {
	const callers = 32
	wantDrain := admission.DrainResult{QueuedShutdownCanceled: 11, InFlightActiveAtStart: 5}
	beginEntered := make(chan struct{})
	releaseBegin := make(chan struct{})
	var beginOnce sync.Once
	scheduler := &serverLifecycleScheduler{
		begin: func(context.Context) (admission.DrainResult, error) {
			beginOnce.Do(func() { close(beginEntered) })
			<-releaseBegin
			return wantDrain, nil
		},
	}
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	upstream := &serverLifecycleIdleCloser{}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)
	httpLifecycle := &serverLifecycleHTTP{}
	server.http = httpLifecycle

	start := make(chan struct{})
	results := make(chan serverLifecycleShutdownCall, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			result, err := server.Shutdown(context.Background())
			results <- serverLifecycleShutdownCall{result: result, err: err}
		}()
	}
	ready.Wait()
	close(start)
	serverLifecycleReceive(t, beginEntered, "concurrent BeginDrain")
	close(releaseBegin)

	wantResult := ShutdownResult{Drain: wantDrain}
	for range callers {
		got := serverLifecycleReceive(t, results, "concurrent Shutdown result")
		if got.err != nil {
			t.Fatalf("concurrent Shutdown() error = %v", got.err)
		}
		if got.result != wantResult {
			t.Fatalf("concurrent Shutdown() result = %+v, want %+v", got.result, wantResult)
		}
	}
	if got := scheduler.beginCalls.Load(); got != 1 {
		t.Fatalf("BeginDrain calls = %d, want 1", got)
	}
	if got := scheduler.waitCalls.Load(); got != 1 {
		t.Fatalf("WaitDrained calls = %d, want 1", got)
	}
	if got := scheduler.closeCalls.Load(); got != 1 {
		t.Fatalf("scheduler Close calls = %d, want 1", got)
	}
	if got := scheduler.forceCalls.Load(); got != 0 {
		t.Fatalf("ForceCancelInflight calls = %d, want 0", got)
	}
	if got := httpLifecycle.shutdownCalls.Load(); got != 1 {
		t.Fatalf("HTTP Shutdown calls = %d, want 1", got)
	}
	if got := httpLifecycle.closeCalls.Load(); got != 0 {
		t.Fatalf("HTTP Close calls = %d, want 0", got)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls = %d, want 1", got)
	}
	if got := upstream.calls.Load(); got != 2 {
		t.Fatalf("upstream idle-close calls = %d, want 2", got)
	}
}

func TestGracefulShutdownOrderAndCounts(t *testing.T) {
	events := &serverLifecycleEvents{}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	listener.onClose = func() { events.add("listener-close") }
	wantDrain := admission.DrainResult{QueuedShutdownCanceled: 13, InFlightActiveAtStart: 2}
	scheduler := &serverLifecycleScheduler{
		begin: func(context.Context) (admission.DrainResult, error) {
			events.add("begin-drain")
			return wantDrain, nil
		},
		wait: func(context.Context) error {
			events.add("wait-drained")
			return nil
		},
		close: func(context.Context) error {
			events.add("scheduler-close")
			return nil
		},
	}
	upstream := &serverLifecycleIdleCloser{close: func() { events.add("upstream-idle-close") }}
	innerHandler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		events.add("handler-start")
		close(handlerStarted)
		<-releaseHandler
		events.add("handler-finish")
	})
	server := newServer(serverLifecycleConfig(), listener, innerHandler, scheduler, upstream)
	httpLifecycle := &serverLifecycleHTTP{
		shutdown: func(context.Context) error {
			events.add("http-shutdown")
			return nil
		},
	}
	server.http = httpLifecycle
	server.baseCancel = func() { events.add("base-cancel") }

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		server.handlers.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodPost, "http://example.test/request", nil),
		)
	}()
	serverLifecycleReceive(t, handlerStarted, "active handler")

	shutdownCall := make(chan serverLifecycleShutdownCall, 1)
	go func() {
		result, err := server.Shutdown(context.Background())
		shutdownCall <- serverLifecycleShutdownCall{result: result, err: err}
	}()
	serverLifecycleReceive(t, listener.closed, "graceful listener close")
	if got := events.snapshot(); !reflect.DeepEqual(got, []string{
		"handler-start",
		"begin-drain",
		"upstream-idle-close",
		"http-shutdown",
		"listener-close",
	}) {
		t.Fatalf("events before handler release = %v, want graceful prefix", got)
	}
	if got := scheduler.waitCalls.Load(); got != 0 {
		t.Fatalf("WaitDrained calls before active handler finished = %d, want 0", got)
	}
	if got := scheduler.closeCalls.Load(); got != 0 {
		t.Fatalf("scheduler Close calls before active handler finished = %d, want 0", got)
	}

	close(releaseHandler)
	serverLifecycleReceive(t, handlerDone, "active handler completion")
	shutdown := serverLifecycleReceive(t, shutdownCall, "graceful Shutdown")
	if shutdown.err != nil {
		t.Fatalf("Shutdown() error = %v", shutdown.err)
	}
	if shutdown.result != (ShutdownResult{Drain: wantDrain}) {
		t.Fatalf("Shutdown() result = %+v, want graceful drain result", shutdown.result)
	}
	wantEvents := []string{
		"handler-start",
		"begin-drain",
		"upstream-idle-close",
		"http-shutdown",
		"listener-close",
		"handler-finish",
		"wait-drained",
		"base-cancel",
		"scheduler-close",
		"upstream-idle-close",
	}
	if got := events.snapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("graceful shutdown events = %v, want %v", got, wantEvents)
	}
	if got := scheduler.beginCalls.Load(); got != 1 {
		t.Fatalf("BeginDrain calls = %d, want 1", got)
	}
	if got := scheduler.forceCalls.Load(); got != 0 {
		t.Fatalf("ForceCancelInflight calls = %d, want 0", got)
	}
	if got := scheduler.waitCalls.Load(); got != 1 {
		t.Fatalf("WaitDrained calls = %d, want 1", got)
	}
	if got := scheduler.closeCalls.Load(); got != 1 {
		t.Fatalf("scheduler Close calls = %d, want 1", got)
	}
	if got := httpLifecycle.shutdownCalls.Load(); got != 1 {
		t.Fatalf("HTTP Shutdown calls = %d, want 1", got)
	}
	if got := httpLifecycle.closeCalls.Load(); got != 0 {
		t.Fatalf("HTTP Close calls = %d, want 0", got)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls = %d, want 1", got)
	}
	if got := upstream.calls.Load(); got != 2 {
		t.Fatalf("upstream idle-close calls = %d, want 2", got)
	}
}

func TestGraceFailureForcesCancellationBeforeDownstreamClose(t *testing.T) {
	events := &serverLifecycleEvents{}
	injectedGraceFailure := errors.New("injected graceful failure")
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	listener.onClose = func() { events.add("listener-close") }
	wantDrain := admission.DrainResult{QueuedShutdownCanceled: 17, InFlightActiveAtStart: 4}
	wantForce := admission.ForceCancelResult{NewlyShutdownSignaled: 3}
	var baseCanceled atomic.Bool
	var orderingViolation atomic.Bool
	httpLifecycle := &serverLifecycleHTTP{
		shutdown: func(context.Context) error {
			events.add("http-shutdown")
			return injectedGraceFailure
		},
		close: func() error {
			events.add("http-close")
			return nil
		},
	}
	scheduler := &serverLifecycleScheduler{
		begin: func(context.Context) (admission.DrainResult, error) {
			events.add("begin-drain")
			return wantDrain, nil
		},
		force: func(context.Context) (admission.ForceCancelResult, error) {
			events.add("force-cancel")
			if baseCanceled.Load() || httpLifecycle.closeCalls.Load() != 0 || listener.closeCalls.Load() != 0 {
				orderingViolation.Store(true)
			}
			return wantForce, nil
		},
		wait: func(context.Context) error {
			events.add("wait-drained")
			return nil
		},
		close: func(context.Context) error {
			events.add("scheduler-close")
			return nil
		},
	}
	upstream := &serverLifecycleIdleCloser{close: func() { events.add("upstream-idle-close") }}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)
	server.http = httpLifecycle
	server.baseCancel = func() {
		events.add("base-cancel")
		baseCanceled.Store(true)
	}

	result, err := server.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	wantResult := ShutdownResult{Drain: wantDrain, Forced: true, Force: wantForce}
	if result != wantResult {
		t.Fatalf("Shutdown() result = %+v, want %+v", result, wantResult)
	}
	if orderingViolation.Load() {
		t.Fatal("downstream cancellation occurred before ForceCancelInflight completed")
	}
	wantEvents := []string{
		"begin-drain",
		"upstream-idle-close",
		"http-shutdown",
		"force-cancel",
		"base-cancel",
		"http-close",
		"listener-close",
		"upstream-idle-close",
		"wait-drained",
		"scheduler-close",
		"upstream-idle-close",
	}
	if got := events.snapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("forced shutdown events = %v, want %v", got, wantEvents)
	}
	if got := scheduler.beginCalls.Load(); got != 1 {
		t.Fatalf("BeginDrain calls = %d, want 1", got)
	}
	if got := scheduler.forceCalls.Load(); got != 1 {
		t.Fatalf("ForceCancelInflight calls = %d, want 1", got)
	}
	if got := scheduler.waitCalls.Load(); got != 1 {
		t.Fatalf("WaitDrained calls = %d, want 1", got)
	}
	if got := scheduler.closeCalls.Load(); got != 1 {
		t.Fatalf("scheduler Close calls = %d, want 1", got)
	}
	if got := httpLifecycle.shutdownCalls.Load(); got != 1 {
		t.Fatalf("HTTP Shutdown calls = %d, want 1", got)
	}
	if got := httpLifecycle.closeCalls.Load(); got != 1 {
		t.Fatalf("HTTP Close calls = %d, want 1", got)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls = %d, want 1", got)
	}
	if got := upstream.calls.Load(); got != 3 {
		t.Fatalf("upstream idle-close calls = %d, want 3", got)
	}
}

func TestForcedShutdownRetriesOnlyUncommittedDrainAndPreservesResult(t *testing.T) {
	wantDrain := admission.DrainResult{QueuedShutdownCanceled: 23, InFlightActiveAtStart: 7}
	wantForce := admission.ForceCancelResult{NewlyShutdownSignaled: 5}
	injectedFirstFailure := errors.New("first drain was not committed")
	var beginSequence atomic.Int64
	scheduler := &serverLifecycleScheduler{
		begin: func(context.Context) (admission.DrainResult, error) {
			if schedulerCall := beginSequence.Add(1); schedulerCall == 1 {
				return admission.DrainResult{}, injectedFirstFailure
			}
			return wantDrain, nil
		},
		force: func(context.Context) (admission.ForceCancelResult, error) {
			return wantForce, nil
		},
	}
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		&serverLifecycleIdleCloser{},
	)
	httpLifecycle := &serverLifecycleHTTP{}
	server.http = httpLifecycle

	result, err := server.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if result != (ShutdownResult{Drain: wantDrain, Forced: true, Force: wantForce}) {
		t.Fatalf("Shutdown() result = %+v, want recovered drain and force results", result)
	}
	if got := scheduler.beginCalls.Load(); got != 2 {
		t.Fatalf("BeginDrain calls = %d, want initial failure plus one force retry", got)
	}
	if got := scheduler.forceCalls.Load(); got != 1 {
		t.Fatalf("ForceCancelInflight calls = %d, want 1", got)
	}
	if got := httpLifecycle.shutdownCalls.Load(); got != 0 {
		t.Fatalf("HTTP Shutdown calls = %d, want 0 before a committed drain", got)
	}
	if got := httpLifecycle.closeCalls.Load(); got != 1 {
		t.Fatalf("HTTP Close calls = %d, want 1", got)
	}
}

func TestForcedShutdownIncompleteReturnsOnlyStaticError(t *testing.T) {
	injectedFailure := errors.New("injected drain wait failure")
	wantForce := admission.ForceCancelResult{NewlyShutdownSignaled: 19}
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	scheduler := &serverLifecycleScheduler{
		begin: func(context.Context) (admission.DrainResult, error) {
			return admission.DrainResult{}, injectedFailure
		},
		force: func(context.Context) (admission.ForceCancelResult, error) {
			return wantForce, nil
		},
		wait: func(context.Context) error { return injectedFailure },
	}
	upstream := &serverLifecycleIdleCloser{}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		upstream,
	)
	httpLifecycle := &serverLifecycleHTTP{}
	server.http = httpLifecycle

	result, err := server.Shutdown(context.Background())
	if err != ErrShutdownIncomplete {
		t.Fatalf("Shutdown() error = %v, want static ErrShutdownIncomplete", err)
	}
	if result != (ShutdownResult{Forced: true, Force: wantForce}) {
		t.Fatalf("Shutdown() result = %+v, want forced result", result)
	}
	if got := scheduler.beginCalls.Load(); got != 2 {
		t.Fatalf("BeginDrain calls = %d, want 2", got)
	}
	if got := scheduler.forceCalls.Load(); got != 1 {
		t.Fatalf("ForceCancelInflight calls = %d, want 1", got)
	}
	if got := scheduler.waitCalls.Load(); got != 1 {
		t.Fatalf("WaitDrained calls = %d, want 1", got)
	}
	if got := scheduler.closeCalls.Load(); got != 1 {
		t.Fatalf("scheduler Close calls after incomplete wait = %d, want 1", got)
	}
	if got := httpLifecycle.closeCalls.Load(); got != 1 {
		t.Fatalf("HTTP Close calls = %d, want 1", got)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls = %d, want 1", got)
	}
	if got := upstream.calls.Load(); got != 2 {
		t.Fatalf("upstream idle-close calls = %d, want 2", got)
	}
}

func TestShutdownBeforeServeClosesListenerAndOwnsErrServerClosed(t *testing.T) {
	listener := newServerLifecycleListener(serverLifecycleLoopbackAddress())
	scheduler := &serverLifecycleScheduler{}
	server := newServer(
		serverLifecycleConfig(),
		listener,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		scheduler,
		&serverLifecycleIdleCloser{},
	)
	var serveBeforeClose atomic.Bool
	httpLifecycle := &serverLifecycleHTTP{
		serve: func(net.Listener) error {
			select {
			case <-listener.closed:
			default:
				serveBeforeClose.Store(true)
			}
			return http.ErrServerClosed
		},
	}
	server.http = httpLifecycle

	if _, err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() before Serve error = %v", err)
	}
	if got := listener.closeCalls.Load(); got != 1 {
		t.Fatalf("listener Close calls before Serve = %d, want 1", got)
	}
	if err := server.Serve(); err != nil {
		t.Fatalf("Serve() after owned shutdown error = %v, want nil", err)
	}
	if serveBeforeClose.Load() {
		t.Fatal("HTTP Serve ran before the owned listener was closed")
	}
	if err := server.Serve(); err != ErrServeAlreadyStarted {
		t.Fatalf("second Serve() error = %v, want ErrServeAlreadyStarted", err)
	}
	if got := httpLifecycle.serveCalls.Load(); got != 1 {
		t.Fatalf("HTTP Serve calls = %d, want 1", got)
	}
}

func serverLifecycleConfig() validatedConfig {
	return validatedConfig{
		headerReadTimeout:     2 * time.Second,
		readTimeout:           5 * time.Second,
		writeTimeout:          13 * time.Second,
		idleTimeout:           17 * time.Second,
		graceTimeout:          10 * time.Second,
		forceTimeout:          7 * time.Second,
		headerReadEnvelope:    16 << 10,
		netHTTPMaxHeaderBytes: 12 << 10,
		maxConnections:        7,
	}
}

func serverLifecycleLoopbackAddress() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

type serverLifecycleShutdownCall struct {
	result ShutdownResult
	err    error
}

type serverLifecycleScheduler struct {
	begin func(context.Context) (admission.DrainResult, error)
	force func(context.Context) (admission.ForceCancelResult, error)
	wait  func(context.Context) error
	close func(context.Context) error

	beginCalls atomic.Int64
	forceCalls atomic.Int64
	waitCalls  atomic.Int64
	closeCalls atomic.Int64
}

func (s *serverLifecycleScheduler) BeginDrain(ctx context.Context) (admission.DrainResult, error) {
	s.beginCalls.Add(1)
	if s.begin != nil {
		return s.begin(ctx)
	}
	return admission.DrainResult{}, nil
}

func (s *serverLifecycleScheduler) ForceCancelInflight(ctx context.Context) (admission.ForceCancelResult, error) {
	s.forceCalls.Add(1)
	if s.force != nil {
		return s.force(ctx)
	}
	return admission.ForceCancelResult{}, nil
}

func (s *serverLifecycleScheduler) WaitDrained(ctx context.Context) error {
	s.waitCalls.Add(1)
	if s.wait != nil {
		return s.wait(ctx)
	}
	return nil
}

func (s *serverLifecycleScheduler) Close(ctx context.Context) error {
	s.closeCalls.Add(1)
	if s.close != nil {
		return s.close(ctx)
	}
	return nil
}

func (s *serverLifecycleScheduler) totalCalls() int64 {
	return s.beginCalls.Load() + s.forceCalls.Load() + s.waitCalls.Load() + s.closeCalls.Load()
}

type serverLifecycleHTTP struct {
	serve    func(net.Listener) error
	shutdown func(context.Context) error
	close    func() error

	serveCalls    atomic.Int64
	shutdownCalls atomic.Int64
	closeCalls    atomic.Int64
}

func (s *serverLifecycleHTTP) Serve(listener net.Listener) error {
	s.serveCalls.Add(1)
	if s.serve != nil {
		return s.serve(listener)
	}
	return http.ErrServerClosed
}

func (s *serverLifecycleHTTP) Shutdown(ctx context.Context) error {
	s.shutdownCalls.Add(1)
	if s.shutdown != nil {
		return s.shutdown(ctx)
	}
	return nil
}

func (s *serverLifecycleHTTP) Close() error {
	s.closeCalls.Add(1)
	if s.close != nil {
		return s.close()
	}
	return nil
}

func (s *serverLifecycleHTTP) totalCalls() int64 {
	return s.serveCalls.Load() + s.shutdownCalls.Load() + s.closeCalls.Load()
}

type serverLifecycleIdleCloser struct {
	close func()
	calls atomic.Int64
}

func (c *serverLifecycleIdleCloser) CloseIdleConnections() {
	c.calls.Add(1)
	if c.close != nil {
		c.close()
	}
}

type serverLifecycleListener struct {
	address net.Addr
	closed  chan struct{}

	closeOnce  sync.Once
	closeCalls atomic.Int64
	closeErr   error
	onClose    func()
}

func newServerLifecycleListener(address net.Addr) *serverLifecycleListener {
	return &serverLifecycleListener{
		address: address,
		closed:  make(chan struct{}),
	}
}

func (l *serverLifecycleListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *serverLifecycleListener) Close() error {
	l.closeCalls.Add(1)
	l.closeOnce.Do(func() {
		if l.onClose != nil {
			l.onClose()
		}
		close(l.closed)
	})
	return l.closeErr
}

func (l *serverLifecycleListener) Addr() net.Addr { return l.address }

type serverLifecycleEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *serverLifecycleEvents) add(event string) {
	e.mu.Lock()
	e.events = append(e.events, event)
	e.mu.Unlock()
}

func (e *serverLifecycleEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func serverLifecycleReceive[T any](t *testing.T, channel <-chan T, operation string) T {
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
