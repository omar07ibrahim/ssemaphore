package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
)

const wirePanicCanary = "SECRET_CANARY_WIRE_HANDLER_PANIC"

func TestWireGeneralOptionsAsteriskReachesApplicationHandler(t *testing.T) {
	observed := make(chan wireObservedRequest, 1)
	running := startWireServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		observed <- wireObservedRequest{
			method:     request.Method,
			requestURI: request.RequestURI,
			protocol:   request.Proto,
		}
		writer.WriteHeader(http.StatusNoContent)
	}), absoluteMinHeaderReadEnvelopeBytes)

	response := exchangeWireRequest(t, running.address, http.MethodOptions,
		"OPTIONS * HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if response.statusCode != http.StatusNoContent {
		t.Fatalf("OPTIONS * status = %d, want %d", response.statusCode, http.StatusNoContent)
	}
	got := receiveWireValue(t, observed, "OPTIONS * application handler")
	if got.method != http.MethodOptions || got.requestURI != "*" || got.protocol != "HTTP/1.1" {
		t.Fatalf("observed OPTIONS request = %#v", got)
	}
}

func TestWireHTTP2PriorKnowledgeDoesNotReachApplicationHandler(t *testing.T) {
	var applicationCalls atomic.Int64
	running := startWireServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		applicationCalls.Add(1)
		writer.Header().Set("Upgrade", "h2c")
		writer.WriteHeader(http.StatusSwitchingProtocols)
	}), absoluteMinHeaderReadEnvelopeBytes)

	response := exchangeWireRequest(t, running.address, "PRI",
		"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
	if response.statusCode == http.StatusSwitchingProtocols {
		t.Fatalf("HTTP/2 prior-knowledge status = %d, must not switch protocols", response.statusCode)
	}
	if got := applicationCalls.Load(); got != 0 {
		t.Fatalf("application handler calls for HTTP/2 prior knowledge = %d, want 0", got)
	}
}

func TestWireH2CUpgradeRemainsOrdinaryHTTP1Request(t *testing.T) {
	observed := make(chan wireObservedRequest, 1)
	running := startWireServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		observed <- wireObservedRequest{
			method:     request.Method,
			requestURI: request.RequestURI,
			protocol:   request.Proto,
			upgrade:    request.Header.Get("Upgrade"),
		}
		writer.Header().Set("X-Wire-Handler", "reached")
		writer.WriteHeader(http.StatusAccepted)
	}), absoluteMinHeaderReadEnvelopeBytes)

	request := "GET /h2c HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Connection: Upgrade, HTTP2-Settings, close\r\n" +
		"Upgrade: h2c\r\n" +
		"HTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n"
	response := exchangeWireRequest(t, running.address, http.MethodGet, request)
	if response.statusCode != http.StatusAccepted {
		t.Fatalf("h2c upgrade status = %d, want ordinary handler status %d", response.statusCode, http.StatusAccepted)
	}
	if got := response.header.Get("X-Wire-Handler"); got != "reached" {
		t.Fatalf("X-Wire-Handler = %q, want %q", got, "reached")
	}
	if got := response.header.Get("Upgrade"); got != "" {
		t.Fatalf("response Upgrade header = %q, want empty", got)
	}

	got := receiveWireValue(t, observed, "ordinary HTTP/1 h2c request")
	if got.method != http.MethodGet || got.requestURI != "/h2c" || got.protocol != "HTTP/1.1" || got.upgrade != "h2c" {
		t.Fatalf("observed h2c request = %#v", got)
	}
}

func TestWireHeaderReadEnvelopeIsExact(t *testing.T) {
	const envelope = absoluteMinHeaderReadEnvelopeBytes
	var applicationCalls atomic.Int64
	running := startWireServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		applicationCalls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}), envelope)

	exactRequest := makeWireHeaderRequest(t, envelope)
	exactResponse := exchangeWireRequest(t, running.address, http.MethodGet, exactRequest)
	if exactResponse.statusCode != http.StatusNoContent {
		t.Fatalf("exact-envelope status = %d, want %d", exactResponse.statusCode, http.StatusNoContent)
	}
	if got := applicationCalls.Load(); got != 1 {
		t.Fatalf("application calls after exact envelope = %d, want 1", got)
	}

	overRequest := makeWireHeaderRequest(t, envelope+1)
	overResponse := exchangeWireRequest(t, running.address, http.MethodGet, overRequest)
	if overResponse.statusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("over-envelope status = %d, want %d", overResponse.statusCode, http.StatusRequestHeaderFieldsTooLarge)
	}
	if got := applicationCalls.Load(); got != 1 {
		t.Fatalf("application calls after over-envelope request = %d, want unchanged count 1", got)
	}
}

func TestWireHandlerPanicCanaryDoesNotReachGlobalLogger(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	var globalLog bytes.Buffer
	log.SetOutput(&globalLog)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	log.Print("wire global logger capture active")
	if !strings.Contains(globalLog.String(), "wire global logger capture active") {
		t.Fatal("global logger capture did not receive its control message")
	}
	globalLog.Reset()

	panicked := make(chan struct{})
	running := startWireServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(panicked)
		panic(wirePanicCanary)
	}), absoluteMinHeaderReadEnvelopeBytes)
	httpServer, ok := running.server.http.(*http.Server)
	if !ok {
		t.Fatalf("HTTP lifecycle type = %T, want *http.Server", running.server.http)
	}
	if httpServer.ErrorLog == nil || httpServer.ErrorLog == log.Default() {
		t.Fatal("server ErrorLog must be a private logger")
	}
	if httpServer.ErrorLog.Writer() != io.Discard {
		t.Fatalf("server ErrorLog writer = %T, want io.Discard", httpServer.ErrorLog.Writer())
	}

	connection := dialWireServer(t, running.address)
	if _, err := io.WriteString(connection, "GET /panic HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"); err != nil {
		connection.Close()
		t.Fatalf("write panic request: %v", err)
	}
	_, readErr := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
	if closeErr := connection.Close(); closeErr != nil {
		t.Fatalf("close panic request connection: %v", closeErr)
	}
	if readErr == nil {
		t.Fatal("panic request unexpectedly returned an HTTP response")
	}
	receiveWireValue(t, panicked, "panicking application handler")
	if got := globalLog.String(); strings.Contains(got, wirePanicCanary) {
		t.Fatalf("global logger leaked handler panic canary: %q", got)
	}
}

type wireObservedRequest struct {
	method     string
	requestURI string
	protocol   string
	upgrade    string
}

type wireResponse struct {
	statusCode int
	header     http.Header
}

type wireRunningServer struct {
	server  *Server
	address string
	served  chan error
}

func startWireServer(t *testing.T, handler http.Handler, envelope uint64) wireRunningServer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral IPv4 loopback: %v", err)
	}
	config := validatedConfig{
		headerReadTimeout:     3 * time.Second,
		readTimeout:           3 * time.Second,
		writeTimeout:          3 * time.Second,
		idleTimeout:           3 * time.Second,
		graceTimeout:          3 * time.Second,
		forceTimeout:          3 * time.Second,
		headerReadEnvelope:    envelope,
		netHTTPMaxHeaderBytes: int(envelope - netHTTPHeaderReadSlopBytes),
		maxConnections:        8,
	}
	server := newServer(config, listener, handler, wireSchedulerLifecycle{}, wireIdleConnectionCloser{})
	running := wireRunningServer{
		server:  server,
		address: listener.Addr().String(),
		served:  make(chan error, 1),
	}
	go func() {
		running.served <- server.Serve()
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := server.Shutdown(ctx); err != nil {
			t.Errorf("wire server Shutdown() error = %v", err)
		}
		if err := receiveWireValue(t, running.served, "wire server Serve return"); err != nil {
			t.Errorf("wire server Serve() error = %v", err)
		}
	})
	return running
}

func exchangeWireRequest(t *testing.T, address, method, request string) wireResponse {
	t.Helper()
	connection := dialWireServer(t, address)
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close wire connection: %v", err)
		}
	}()
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write wire request: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: method})
	if err != nil {
		t.Fatalf("read wire response: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close wire response body: %v", err)
	}
	return wireResponse{
		statusCode: response.StatusCode,
		header:     response.Header.Clone(),
	}
}

func dialWireServer(t *testing.T, address string) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp4", address, 3*time.Second)
	if err != nil {
		t.Fatalf("dial wire server: %v", err)
	}
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		connection.Close()
		t.Fatalf("set wire connection deadline: %v", err)
	}
	return connection
}

func makeWireHeaderRequest(t *testing.T, size uint64) string {
	t.Helper()
	const prefix = "GET /headers HTTP/1.1\r\nHost: localhost\r\nX-Pad: "
	const suffix = "\r\nConnection: close\r\n\r\n"
	fixedSize := uint64(len(prefix) + len(suffix))
	if size < fixedSize {
		t.Fatalf("requested wire header size %d is below fixed request size %d", size, fixedSize)
	}
	request := prefix + strings.Repeat("x", int(size-fixedSize)) + suffix
	if got := uint64(len(request)); got != size {
		t.Fatalf("wire request size = %d, want %d", got, size)
	}
	return request
}

func receiveWireValue[T any](t *testing.T, channel <-chan T, operation string) T {
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

type wireSchedulerLifecycle struct{}

func (wireSchedulerLifecycle) BeginDrain(context.Context) (admission.DrainResult, error) {
	return admission.DrainResult{}, nil
}

func (wireSchedulerLifecycle) ForceCancelInflight(context.Context) (admission.ForceCancelResult, error) {
	return admission.ForceCancelResult{}, nil
}

func (wireSchedulerLifecycle) WaitDrained(context.Context) error { return nil }
func (wireSchedulerLifecycle) Close(context.Context) error       { return nil }

type wireIdleConnectionCloser struct{}

func (wireIdleConnectionCloser) CloseIdleConnections() {}
