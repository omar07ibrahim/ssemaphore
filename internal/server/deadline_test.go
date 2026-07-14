package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

const deadlineTestWatchdog = 3 * time.Second

func TestDeadlineFragmentedIncompleteHeadersExpireBeforeHandler(t *testing.T) {
	var handlerCalls atomic.Int64
	config := deadlineValidatedConfig(t, Config{
		HeaderReadTimeout:       100 * time.Millisecond,
		ResponseWriteTimeout:    100 * time.Millisecond,
		IdleTimeout:             100 * time.Millisecond,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}, httpapi.TimeoutPolicy{
		DefaultQueueTimeout: 100 * time.Millisecond,
		BodyReadTimeout:     100 * time.Millisecond,
		UpstreamTimeout:     100 * time.Millisecond,
	})
	client := deadlineStartPipeServer(t, config, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		handlerCalls.Add(1)
	}))

	deadlineWriteString(t, client, "GET /fragmented HTTP/1.1\r\n")
	deadlineWriteString(t, client, "Host: localhost")

	readDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, client)
		readDone <- err
	}()
	deadlineAwait(t, readDone, "incomplete header connection closure")

	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler calls after incomplete headers = %d, want 0", got)
	}
}

func TestDeadlineIncompleteBodyReadUsesDerivedReadTimeout(t *testing.T) {
	bodyResult := make(chan error, 1)
	config := deadlineValidatedConfig(t, Config{
		HeaderReadTimeout:       50 * time.Millisecond,
		ResponseWriteTimeout:    100 * time.Millisecond,
		IdleTimeout:             100 * time.Millisecond,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}, httpapi.TimeoutPolicy{
		DefaultQueueTimeout: 100 * time.Millisecond,
		BodyReadTimeout:     50 * time.Millisecond,
		UpstreamTimeout:     100 * time.Millisecond,
	})
	if got, want := config.readTimeout, 100*time.Millisecond; got != want {
		t.Fatalf("derived ReadTimeout = %v, want %v", got, want)
	}
	client := deadlineStartPipeServer(t, config, http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		_, err := io.ReadAll(request.Body)
		bodyResult <- err
	}))

	deadlineWriteString(t, client,
		"POST /body HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nx")
	err := deadlineAwait(t, bodyResult, "incomplete request body read timeout")
	deadlineRequireTimeout(t, err, "request body read")
}

func TestDeadlineBlockedResponseFlushUsesDerivedWriteTimeout(t *testing.T) {
	flushResult := make(chan error, 1)
	config := deadlineValidatedConfig(t, Config{
		HeaderReadTimeout:       100 * time.Millisecond,
		ResponseWriteTimeout:    70 * time.Millisecond,
		IdleTimeout:             100 * time.Millisecond,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}, httpapi.TimeoutPolicy{
		DefaultQueueTimeout: 10 * time.Millisecond,
		BodyReadTimeout:     10 * time.Millisecond,
		UpstreamTimeout:     10 * time.Millisecond,
	})
	if got, want := config.writeTimeout, 100*time.Millisecond; got != want {
		t.Fatalf("derived WriteTimeout = %v, want %v", got, want)
	}
	client := deadlineStartPipeServer(t, config, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(writer, "blocked response"); err != nil {
			flushResult <- err
			return
		}
		flushResult <- http.NewResponseController(writer).Flush()
	}))

	deadlineWriteString(t, client, "GET /blocked HTTP/1.1\r\nHost: localhost\r\n\r\n")
	err := deadlineAwait(t, flushResult, "blocked response flush timeout")
	deadlineRequireTimeout(t, err, "response flush")
}

func TestDeadlineIdleKeepAliveConnectionExpires(t *testing.T) {
	var handlerCalls atomic.Int64
	config := deadlineValidatedConfig(t, Config{
		HeaderReadTimeout:       100 * time.Millisecond,
		ResponseWriteTimeout:    250 * time.Millisecond,
		IdleTimeout:             100 * time.Millisecond,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}, httpapi.TimeoutPolicy{
		DefaultQueueTimeout: 250 * time.Millisecond,
		BodyReadTimeout:     250 * time.Millisecond,
		UpstreamTimeout:     250 * time.Millisecond,
	})
	client := deadlineStartPipeServer(t, config, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		handlerCalls.Add(1)
		_, _ = io.WriteString(writer, "ok")
	}))

	deadlineWriteString(t, client, "GET /keep-alive HTTP/1.1\r\nHost: localhost\r\n\r\n")
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read keep-alive response: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		response.Body.Close()
		t.Fatalf("read keep-alive response body: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close keep-alive response body: %v", err)
	}
	if got, want := string(body), "ok"; got != want {
		t.Fatalf("keep-alive response body = %q, want %q", got, want)
	}
	if response.Close {
		t.Fatal("server closed response explicitly, want an initially persistent connection")
	}

	idleRead := make(chan error, 1)
	go func() {
		_, err := reader.ReadByte()
		idleRead <- err
	}()
	if err := deadlineAwait(t, idleRead, "idle keep-alive connection closure"); err == nil {
		t.Fatal("idle keep-alive read error = nil, want connection closure")
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler calls after idle timeout = %d, want 1", got)
	}
}

func deadlineValidatedConfig(t *testing.T, config Config, policy httpapi.TimeoutPolicy) validatedConfig {
	t.Helper()
	validated, err := validateConfig(config, policy)
	if err != nil {
		t.Fatalf("validate deadline test config: %v", err)
	}
	return validated
}

func deadlineStartPipeServer(t *testing.T, config validatedConfig, handler http.Handler) net.Conn {
	t.Helper()
	serverConnection, clientConnection := net.Pipe()
	listener := newDeadlinePipeListener(serverConnection)
	server := newServer(config, listener, handler, deadlineSchedulerLifecycle{}, deadlineIdleCloser{})
	served := make(chan error, 1)
	go func() {
		served <- server.Serve()
	}()

	t.Cleanup(func() {
		_ = clientConnection.Close()
		ctx, cancel := context.WithTimeout(context.Background(), deadlineTestWatchdog)
		defer cancel()
		if _, err := server.Shutdown(ctx); err != nil {
			t.Errorf("deadline server Shutdown() error = %v", err)
		}
		if err := deadlineAwait(t, served, "deadline server Serve return"); err != nil {
			t.Errorf("deadline server Serve() error = %v", err)
		}
	})
	return clientConnection
}

func deadlineWriteString(t *testing.T, connection net.Conn, value string) {
	t.Helper()
	if err := connection.SetWriteDeadline(time.Now().Add(deadlineTestWatchdog)); err != nil {
		t.Fatalf("set deadline pipe write deadline: %v", err)
	}
	if _, err := io.WriteString(connection, value); err != nil {
		t.Fatalf("write deadline pipe request: %v", err)
	}
	if err := connection.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline pipe write deadline: %v", err)
	}
}

func deadlineRequireTimeout(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s error = nil, want timeout", operation)
	}
	var netError net.Error
	if !errors.As(err, &netError) || !netError.Timeout() {
		t.Fatalf("%s error = %v, want net.Error timeout", operation, err)
	}
}

func deadlineAwait[T any](t *testing.T, values <-chan T, operation string) T {
	t.Helper()
	timer := time.NewTimer(deadlineTestWatchdog)
	defer timer.Stop()
	select {
	case value := <-values:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", operation)
		var zero T
		return zero
	}
}

type deadlinePipeListener struct {
	mu         sync.Mutex
	connection net.Conn
	closed     chan struct{}
	closeOnce  sync.Once
}

func newDeadlinePipeListener(connection net.Conn) *deadlinePipeListener {
	return &deadlinePipeListener{
		connection: connection,
		closed:     make(chan struct{}),
	}
}

func (l *deadlinePipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.connection != nil {
		connection := l.connection
		l.connection = nil
		l.mu.Unlock()
		return connection, nil
	}
	closed := l.closed
	l.mu.Unlock()
	<-closed
	return nil, net.ErrClosed
}

func (l *deadlinePipeListener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		connection := l.connection
		l.connection = nil
		l.mu.Unlock()
		close(l.closed)
		if connection != nil {
			_ = connection.Close()
		}
	})
	return nil
}

func (*deadlinePipeListener) Addr() net.Addr { return deadlinePipeAddress{} }

type deadlinePipeAddress struct{}

func (deadlinePipeAddress) Network() string { return "deadline-pipe" }
func (deadlinePipeAddress) String() string  { return "deadline-pipe" }

type deadlineSchedulerLifecycle struct{}

func (deadlineSchedulerLifecycle) BeginDrain(context.Context) (admission.DrainResult, error) {
	return admission.DrainResult{}, nil
}

func (deadlineSchedulerLifecycle) ForceCancelInflight(context.Context) (admission.ForceCancelResult, error) {
	return admission.ForceCancelResult{}, nil
}

func (deadlineSchedulerLifecycle) WaitDrained(context.Context) error { return nil }
func (deadlineSchedulerLifecycle) Close(context.Context) error       { return nil }

type deadlineIdleCloser struct{}

func (deadlineIdleCloser) CloseIdleConnections() {}
