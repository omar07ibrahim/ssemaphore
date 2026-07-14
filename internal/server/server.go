package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

var (
	ErrServeAlreadyStarted = errors.New("inbound HTTP server may be served only once")
	ErrServeFailed         = errors.New("inbound HTTP server stopped unexpectedly")
	ErrShutdownIncomplete  = errors.New("inbound HTTP server shutdown was incomplete")
)

// ShutdownResult is a content-free account of the scheduler work affected by
// this server's terminal shutdown.
type ShutdownResult struct {
	Drain  admission.DrainResult
	Forced bool
	Force  admission.ForceCancelResult
}

type schedulerLifecycle interface {
	BeginDrain(context.Context) (admission.DrainResult, error)
	ForceCancelInflight(context.Context) (admission.ForceCancelResult, error)
	WaitDrained(context.Context) error
	Close(context.Context) error
}

type httpServerLifecycle interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
	Close() error
}

type idleConnectionCloser interface {
	CloseIdleConnections()
}

// Server owns an already-created listener, the admission scheduler, and their
// coordinated terminal lifecycle. It never creates or selects a network
// address. Serve and Shutdown may be called from separate goroutines.
type Server struct {
	config    validatedConfig
	listener  *boundedListener
	http      httpServerLifecycle
	handlers  *trackedHandler
	scheduler schedulerLifecycle
	upstream  idleConnectionCloser

	baseCancel context.CancelFunc
	serveOnce  atomic.Bool

	shutdownStarted atomic.Bool
	shutdownOnce    sync.Once
	shutdownDone    chan struct{}
	shutdownResult  ShutdownResult
	shutdownErr     error
}

// New validates all ownership and resource boundaries before taking ownership
// of listener and scheduler. TCP listeners must already be bound to a numeric
// loopback address; Unix listeners are also accepted.
func New(
	config Config,
	listener net.Listener,
	handler *httpapi.Handler,
	scheduler *admission.Scheduler,
) (*Server, error) {
	if handler == nil {
		return nil, errors.New("HTTP handler must not be nil")
	}
	if scheduler == nil {
		return nil, errors.New("admission scheduler must not be nil")
	}
	if !handler.UsesScheduler(scheduler) {
		return nil, errors.New("HTTP handler and server must own the same admission scheduler")
	}
	if err := validateListener(listener); err != nil {
		return nil, err
	}
	validated, err := validateConfig(config, handler.TimeoutPolicy())
	if err != nil {
		return nil, err
	}
	return newServer(validated, listener, handler, scheduler, handler), nil
}

func newServer(
	config validatedConfig,
	listener net.Listener,
	handler http.Handler,
	scheduler schedulerLifecycle,
	upstream idleConnectionCloser,
) *Server {
	baseContext, baseCancel := context.WithCancel(context.Background())
	tracked := newTrackedHandler(handler)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	bounded := newBoundedListener(listener, config.maxConnections)
	httpServer := &http.Server{
		Handler:                      tracked,
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            config.headerReadTimeout,
		ReadTimeout:                  config.readTimeout,
		WriteTimeout:                 config.writeTimeout,
		IdleTimeout:                  config.idleTimeout,
		MaxHeaderBytes:               config.netHTTPMaxHeaderBytes,
		Protocols:                    protocols,
		ErrorLog:                     log.New(io.Discard, "", 0),
		BaseContext: func(net.Listener) context.Context {
			return baseContext
		},
	}
	return &Server{
		config:       config,
		listener:     bounded,
		http:         httpServer,
		handlers:     tracked,
		scheduler:    scheduler,
		upstream:     upstream,
		baseCancel:   baseCancel,
		shutdownDone: make(chan struct{}),
	}
}

// Serve runs the configured HTTP/1 server exactly once. Unexpected listener
// failures start terminal cleanup but are reduced to a static public error.
func (s *Server) Serve() error {
	if !s.serveOnce.CompareAndSwap(false, true) {
		return ErrServeAlreadyStarted
	}
	err := s.http.Serve(s.listener)
	if errors.Is(err, http.ErrServerClosed) && s.shutdownStarted.Load() {
		return nil
	}
	s.startShutdown()
	return ErrServeFailed
}

// Shutdown starts one server-owned graceful-to-forced cleanup. ctx bounds only
// this caller's wait; once started, cleanup continues on independent deadlines.
// A context already canceled before the first call does not start shutdown.
func (s *Server) Shutdown(ctx context.Context) (ShutdownResult, error) {
	if ctx == nil {
		return ShutdownResult{}, errors.New("shutdown context must not be nil")
	}
	select {
	case <-s.shutdownDone:
		return s.shutdownResult, s.shutdownErr
	default:
	}
	if err := ctx.Err(); err != nil {
		return ShutdownResult{}, err
	}

	s.startShutdown()
	select {
	case <-s.shutdownDone:
		return s.shutdownResult, s.shutdownErr
	case <-ctx.Done():
		select {
		case <-s.shutdownDone:
			return s.shutdownResult, s.shutdownErr
		default:
			return ShutdownResult{}, ctx.Err()
		}
	}
}

func (s *Server) startShutdown() {
	s.shutdownOnce.Do(func() {
		s.shutdownStarted.Store(true)
		go func() {
			s.shutdownResult, s.shutdownErr = s.runShutdown()
			close(s.shutdownDone)
		}()
	})
}

func (s *Server) runShutdown() (ShutdownResult, error) {
	result := ShutdownResult{}
	graceContext, cancelGrace := context.WithTimeout(context.Background(), s.config.graceTimeout)
	graceful, drainStarted := s.runGraceful(graceContext, &result)
	cancelGrace()

	if graceful {
		s.handlers.seal()
		s.baseCancel()
		finalContext, cancelFinal := context.WithTimeout(context.Background(), s.config.forceTimeout)
		defer cancelFinal()
		if err := s.scheduler.Close(finalContext); err != nil {
			s.upstream.CloseIdleConnections()
			return result, ErrShutdownIncomplete
		}
		s.upstream.CloseIdleConnections()
		return result, nil
	}

	result.Forced = true
	s.handlers.seal()
	forceContext, cancelForce := context.WithTimeout(context.Background(), s.config.forceTimeout)
	defer cancelForce()
	complete := true
	if !drainStarted {
		drainResult, err := s.scheduler.BeginDrain(forceContext)
		if err != nil {
			complete = false
		} else {
			result.Drain = drainResult
		}
	}

	forceResult, err := s.scheduler.ForceCancelInflight(forceContext)
	if err != nil {
		complete = false
	} else {
		result.Force = forceResult
	}

	// Shutdown attribution is committed above before downstream cancellation can
	// be observed as a client-side cancellation by the admission scheduler.
	s.baseCancel()
	if !cleanCloseError(s.http.Close()) {
		complete = false
	}
	if !cleanCloseError(s.listener.Close()) {
		complete = false
	}
	s.upstream.CloseIdleConnections()
	handlersDrained := s.handlers.wait(forceContext) == nil
	if !handlersDrained {
		complete = false
	}
	schedulerDrained := s.scheduler.WaitDrained(forceContext) == nil
	if !schedulerDrained {
		complete = false
	}
	closeContext := forceContext
	cancelClose := func() {}
	if schedulerDrained {
		closeContext, cancelClose = context.WithTimeout(context.Background(), s.config.forceTimeout)
	}
	if err := s.scheduler.Close(closeContext); err != nil {
		complete = false
	}
	cancelClose()
	s.upstream.CloseIdleConnections()
	if !complete {
		return result, ErrShutdownIncomplete
	}
	return result, nil
}

func (s *Server) runGraceful(ctx context.Context, result *ShutdownResult) (graceful, drainStarted bool) {
	drainResult, err := s.scheduler.BeginDrain(ctx)
	if err != nil {
		return false, false
	}
	result.Drain = drainResult
	s.upstream.CloseIdleConnections()
	if err := s.http.Shutdown(ctx); err != nil {
		return false, true
	}
	if !cleanCloseError(s.listener.Close()) {
		return false, true
	}
	s.handlers.seal()
	if err := s.handlers.wait(ctx); err != nil {
		return false, true
	}
	if err := s.scheduler.WaitDrained(ctx); err != nil {
		return false, true
	}
	return true, true
}

func cleanCloseError(err error) bool {
	return err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed)
}
