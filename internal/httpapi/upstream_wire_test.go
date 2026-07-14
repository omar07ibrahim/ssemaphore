package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	upstreamWireToken  = "upstream-wire-token"
	upstreamWireCanary = "UPSTREAM_WIRE_SECRET_CANARY"
	upstreamWireBody   = `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8}`
)

type upstreamWireRoundTripFunc func(*http.Request) (*http.Response, error)

func (f upstreamWireRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type upstreamWireTrackedBody struct {
	reader io.Reader
	closes atomic.Int32
}

func (b *upstreamWireTrackedBody) Read(destination []byte) (int, error) {
	return b.reader.Read(destination)
}

func (b *upstreamWireTrackedBody) Close() error {
	b.closes.Add(1)
	return nil
}

type upstreamWireTimeoutError struct{}

func (upstreamWireTimeoutError) Error() string   { return upstreamWireCanary }
func (upstreamWireTimeoutError) Timeout() bool   { return true }
func (upstreamWireTimeoutError) Temporary() bool { return true }

type upstreamWireObservation struct {
	method           string
	path             string
	rawQuery         string
	protocolMajor    int
	contentLength    int64
	transferEncoding []string
	header           http.Header
	body             []byte
}

func upstreamWireConfig(endpoint string) HTTPUpstreamConfig {
	return HTTPUpstreamConfig{
		Endpoint:               endpoint,
		ConnectTimeout:         time.Second,
		TLSHandshakeTimeout:    time.Second,
		ResponseHeaderTimeout:  time.Second,
		IdleConnectionTimeout:  time.Second,
		MaxResponseHeaderBytes: 4096,
		MaxConnections:         4,
	}
}

func upstreamWireNew(t *testing.T, config HTTPUpstreamConfig, token string) *HTTPUpstream {
	t.Helper()
	upstream, err := NewHTTPUpstream(config, token)
	if err != nil {
		t.Fatalf("NewHTTPUpstream() error = %v", err)
	}
	t.Cleanup(upstream.CloseIdleConnections)
	return upstream
}

func upstreamWireRequest(t *testing.T) contract.Request {
	t.Helper()
	parser, err := contract.NewParser("local-model", contract.Limits{
		MaxBodyBytes:        2048,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 512,
		MaxCompletionTokens: 512,
		CompletionWeight:    4,
		MaxRequestUnits:     4096,
	})
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	request, err := parser.Parse(context.Background(), strings.NewReader(upstreamWireBody))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	return request
}

func upstreamWireContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func upstreamWireEndpoint(server *httptest.Server) string {
	return server.URL + chatCompletionsPath
}

func upstreamWireObserve(request *http.Request) upstreamWireObservation {
	body, _ := io.ReadAll(request.Body)
	return upstreamWireObservation{
		method:           request.Method,
		path:             request.URL.Path,
		rawQuery:         request.URL.RawQuery,
		protocolMajor:    request.ProtoMajor,
		contentLength:    request.ContentLength,
		transferEncoding: append([]string(nil), request.TransferEncoding...),
		header:           request.Header.Clone(),
		body:             body,
	}
}

func upstreamWireRequireFixedRequest(t *testing.T, observation upstreamWireObservation) {
	t.Helper()
	if observation.method != http.MethodPost {
		t.Fatalf("method = %q, want POST", observation.method)
	}
	if observation.path != chatCompletionsPath || observation.rawQuery != "" {
		t.Fatalf("URL path/query = %q?%q, want exact path without query", observation.path, observation.rawQuery)
	}
	if observation.protocolMajor != 1 {
		t.Fatalf("HTTP protocol major = %d, want 1", observation.protocolMajor)
	}
	if observation.contentLength != int64(len(upstreamWireBody)) {
		t.Fatalf("Content-Length = %d, want %d", observation.contentLength, len(upstreamWireBody))
	}
	if len(observation.transferEncoding) != 0 {
		t.Fatalf("Transfer-Encoding = %v, want none", observation.transferEncoding)
	}
	if !bytes.Equal(observation.body, []byte(upstreamWireBody)) {
		t.Fatalf("body = %q, want exact validated request", observation.body)
	}
	wantHeader := http.Header{
		"Accept":         []string{"application/json"},
		"Authorization":  []string{"Bearer " + upstreamWireToken},
		"Content-Length": []string{strconv.Itoa(len(upstreamWireBody))},
		"Content-Type":   []string{"application/json"},
		"User-Agent":     []string{upstreamUserAgent},
	}
	if !reflect.DeepEqual(observation.header, wantHeader) {
		t.Fatalf("headers = %#v, want exact %#v", observation.header, wantHeader)
	}
}

func upstreamWireAwait(t *testing.T, channel <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestHTTPUpstreamBuildsOneNonReplayableFixedRequest(t *testing.T) {
	upstream := upstreamWireNew(t, upstreamWireConfig("https://fixed.example/v1/chat/completions"), upstreamWireToken)
	request := upstreamWireRequest(t)

	sourceHeader := http.Header{
		"Content-Type":      []string{"application/json"},
		"X-Upstream-Canary": []string{upstreamWireCanary},
	}
	sourceBody := &upstreamWireTrackedBody{reader: strings.NewReader(`{"object":"chat.completion"}`)}
	var calls atomic.Int32
	var captured *http.Request
	var capturedBody []byte
	upstream.client.Transport = upstreamWireRoundTripFunc(func(outbound *http.Request) (*http.Response, error) {
		calls.Add(1)
		captured = outbound
		capturedBody, _ = io.ReadAll(outbound.Body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     sourceHeader,
			Body:       sourceBody,
			Request:    outbound,
		}, nil
	})

	response, err := upstream.Complete(upstreamWireContext(t), request)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("RoundTrip() calls = %d, want 1", calls.Load())
	}
	if captured == nil {
		t.Fatal("RoundTrip() did not receive a request")
	}
	if captured.Method != http.MethodPost || captured.URL.String() != "https://fixed.example/v1/chat/completions" {
		t.Fatalf("outbound method/URL = %s %s", captured.Method, captured.URL)
	}
	if captured.ContentLength != int64(len(upstreamWireBody)) || !bytes.Equal(capturedBody, []byte(upstreamWireBody)) {
		t.Fatal("outbound body or Content-Length differs from the validated request")
	}
	if captured.GetBody != nil {
		t.Fatal("outbound request exposes GetBody and is replayable")
	}
	if len(captured.TransferEncoding) != 0 {
		t.Fatalf("outbound Transfer-Encoding = %v, want none", captured.TransferEncoding)
	}
	if _, hasDeadline := captured.Context().Deadline(); !hasDeadline {
		t.Fatal("outbound context has no deadline")
	}
	wantHeader := http.Header{
		"Accept":        []string{"application/json"},
		"Authorization": []string{"Bearer " + upstreamWireToken},
		"Content-Type":  []string{"application/json"},
		"User-Agent":    []string{upstreamUserAgent},
	}
	if !reflect.DeepEqual(captured.Header, wantHeader) {
		t.Fatalf("outbound headers = %#v, want exact %#v", captured.Header, wantHeader)
	}
	if captured.Header.Get("Idempotency-Key") != "" || captured.Header.Get("X-Client-Canary") != "" {
		t.Fatal("outbound request contains replay or client-derived headers")
	}

	if response.StatusCode != http.StatusOK || response.Body != sourceBody {
		t.Fatal("successful response status or body ownership was not transferred")
	}
	if sourceBody.closes.Load() != 0 {
		t.Fatal("Complete() closed a successfully transferred response body")
	}
	sourceHeader.Set("X-Upstream-Canary", "mutated-source")
	if got := response.Header.Get("X-Upstream-Canary"); got != upstreamWireCanary {
		t.Fatalf("returned header changed with source mutation: %q", got)
	}
	response.Header.Set("Content-Type", "text/plain")
	if got := sourceHeader.Get("Content-Type"); got != "application/json" {
		t.Fatalf("source header changed with returned clone mutation: %q", got)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("response Body.Close() error = %v", err)
	}
	if sourceBody.closes.Load() != 1 {
		t.Fatalf("response body close count = %d, want 1", sourceBody.closes.Load())
	}
}

func TestHTTPUpstreamUsesHTTP1AndNeverFollowsRedirects(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+chatCompletionsPath)
		writer.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(writer, upstreamWireCanary)
	}))
	defer source.Close()

	upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(source)), upstreamWireToken)
	if upstream.transport.Protocols == nil || !upstream.transport.Protocols.HTTP1() {
		t.Fatal("transport does not explicitly enable HTTP/1")
	}
	if upstream.transport.Protocols.HTTP2() {
		t.Fatal("transport enables HTTP/2 despite the no-automatic-retry policy")
	}
	if upstream.transport.Proxy != nil {
		t.Fatal("transport inherits a proxy function")
	}
	if !upstream.transport.DisableCompression {
		t.Fatal("transport permits transparent compression")
	}

	response, err := upstream.Complete(upstreamWireContext(t), upstreamWireRequest(t))
	if err != nil {
		t.Fatalf("Complete() redirect error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTemporaryRedirect)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target calls = %d, want 0", targetCalls.Load())
	}
}

func TestHTTPUpstreamPreservesEncodedResponseBytes(t *testing.T) {
	encoded := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef}
	observed := make(chan upstreamWireObservation, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		observed <- upstreamWireObserve(request)
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Content-Encoding", "gzip")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(encoded)
	}))
	defer server.Close()

	upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(server)), upstreamWireToken)
	response, err := upstream.Complete(upstreamWireContext(t), upstreamWireRequest(t))
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	defer response.Body.Close()
	got, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("response body read error = %v", readErr)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("response body = %x, want exact encoded bytes %x", got, encoded)
	}
	if response.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", response.Header.Get("Content-Encoding"))
	}
	upstreamWireRequireFixedRequest(t, <-observed)
}

func TestHTTPUpstreamBoundsHeadersAndHeaderLatency(t *testing.T) {
	t.Run("response header bytes", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("X-Oversized-Canary", strings.Repeat(upstreamWireCanary, 128))
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"object":"chat.completion"}`)
		}))
		defer server.Close()

		config := upstreamWireConfig(upstreamWireEndpoint(server))
		config.MaxResponseHeaderBytes = 512
		upstream := upstreamWireNew(t, config, upstreamWireToken)
		response, err := upstream.Complete(upstreamWireContext(t), upstreamWireRequest(t))
		if response.Body != nil {
			_ = response.Body.Close()
			t.Fatal("header-limit failure transferred a response body")
		}
		if !errors.Is(err, errUpstreamRequestFailed) {
			t.Fatalf("Complete() error = %v, want sanitized request failure", err)
		}
		if strings.Contains(err.Error(), upstreamWireCanary) || strings.Contains(err.Error(), server.URL) {
			t.Fatalf("header-limit error leaks upstream data: %q", err)
		}
	})

	t.Run("response header timeout", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"object":"chat.completion"}`)
		}))
		defer server.Close()
		defer close(release)

		config := upstreamWireConfig(upstreamWireEndpoint(server))
		config.ResponseHeaderTimeout = 100 * time.Millisecond
		upstream := upstreamWireNew(t, config, upstreamWireToken)
		ctx := upstreamWireContext(t)
		request := upstreamWireRequest(t)
		result := make(chan error, 1)
		go func() {
			response, err := upstream.Complete(ctx, request)
			if response.Body != nil {
				_ = response.Body.Close()
			}
			result <- err
		}()

		upstreamWireAwait(t, entered, "upstream request before withholding headers")
		select {
		case err := <-result:
			if !errors.Is(err, ErrUpstreamTimeout) {
				t.Fatalf("Complete() error = %v, want ErrUpstreamTimeout", err)
			}
			if err.Error() != ErrUpstreamTimeout.Error() || strings.Contains(err.Error(), upstreamWireCanary) {
				t.Fatalf("timeout error is not static: %q", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Complete() did not honor ResponseHeaderTimeout")
		}
	})
}

func TestHTTPUpstreamEnforcesConnectionLimit(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"object":"chat.completion"}`)
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	config := upstreamWireConfig(upstreamWireEndpoint(server))
	config.MaxConnections = 1
	upstream := upstreamWireNew(t, config, upstreamWireToken)
	request := upstreamWireRequest(t)
	firstContext := upstreamWireContext(t)
	secondContext := upstreamWireContext(t)
	results := make(chan error, 2)
	run := func(ctx context.Context) {
		response, err := upstream.Complete(ctx, request)
		if response.Body != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		results <- err
	}

	go run(firstContext)
	upstreamWireAwait(t, entered, "first bounded upstream connection")
	go run(secondContext)
	secondEnteredEarly := false
	select {
	case <-entered:
		secondEnteredEarly = true
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if !secondEnteredEarly {
		upstreamWireAwait(t, entered, "second connection after capacity release")
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("Complete() error = %v", err)
		}
	}
	if secondEnteredEarly {
		t.Fatal("second request reached the upstream before connection capacity was released")
	}
}

func TestHTTPUpstreamCancellationAbortsPendingHeaders(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	config := upstreamWireConfig(upstreamWireEndpoint(server))
	config.ResponseHeaderTimeout = 2 * time.Second
	upstream := upstreamWireNew(t, config, upstreamWireToken)
	var traceCalls atomic.Int32
	var tracedAuthorization atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteHeaderField: func(key string, values []string) {
			traceCalls.Add(1)
			if strings.EqualFold(key, "Authorization") || strings.Contains(strings.Join(values, ""), upstreamWireToken) {
				tracedAuthorization.Store(true)
			}
		},
	}
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	ctx := httptrace.WithClientTrace(parent, trace)
	request := upstreamWireRequest(t)
	result := make(chan error, 1)
	go func() {
		response, err := upstream.Complete(ctx, request)
		if response.Body != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()

	upstreamWireAwait(t, entered, "upstream request before cancellation")
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Complete() error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Complete() remained blocked after context cancellation")
	}
	close(release)
	if traceCalls.Load() != 0 || tracedAuthorization.Load() {
		t.Fatalf("caller HTTP trace observed outbound headers: calls=%d authorization=%t", traceCalls.Load(), tracedAuthorization.Load())
	}
}

func TestHTTPUpstreamDoesNotRetryAbruptHTTP1Failure(t *testing.T) {
	var calls atomic.Int32
	var firstRemote atomic.Value
	var reusedConnection atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			firstRemote.Store(request.RemoteAddr)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"object":"chat.completion"}`)
			return
		}
		reusedConnection.Store(request.RemoteAddr == firstRemote.Load().(string))
		hijacker, ok := writer.(http.Hijacker)
		if !ok {
			t.Error("httptest ResponseWriter does not implement http.Hijacker")
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("Hijack() error = %v", err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()

	upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(server)), upstreamWireToken)
	warmup, err := upstream.Complete(upstreamWireContext(t), upstreamWireRequest(t))
	if err != nil {
		t.Fatalf("warm-up Complete() error = %v", err)
	}
	if _, err := io.Copy(io.Discard, warmup.Body); err != nil {
		t.Fatalf("warm-up response read error = %v", err)
	}
	if err := warmup.Body.Close(); err != nil {
		t.Fatalf("warm-up response close error = %v", err)
	}

	response, err := upstream.Complete(upstreamWireContext(t), upstreamWireRequest(t))
	if response.Body != nil {
		_ = response.Body.Close()
		t.Fatal("abrupt transport failure transferred a response body")
	}
	if !errors.Is(err, errUpstreamRequestFailed) {
		t.Fatalf("Complete() error = %v, want sanitized request failure", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("upstream attempts including warm-up = %d, want exactly 2", calls.Load())
	}
	if !reusedConnection.Load() {
		t.Fatal("second request did not exercise an already-used HTTP/1 connection")
	}
}

func TestHTTPUpstreamRequiresDeadlineAndSanitizesErrors(t *testing.T) {
	upstream := upstreamWireNew(
		t,
		upstreamWireConfig("https://upstream-wire-url-canary.example/v1/chat/completions"),
		upstreamWireToken,
	)
	var calls atomic.Int32
	upstream.client.Transport = upstreamWireRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New(upstreamWireCanary + " " + upstreamWireToken)
	})
	request := upstreamWireRequest(t)

	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "nil", ctx: nil, wantErr: errUpstreamDeadlineRequired},
		{name: "missing", ctx: context.Background(), wantErr: errUpstreamDeadlineRequired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := upstream.Complete(test.ctx, request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Complete() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("deadline validation reached transport %d times, want 0", calls.Load())
	}

	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	_, err := upstream.Complete(expired, request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete() expired-context error = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("expired context reached transport %d times, want 0", calls.Load())
	}

	_, err = upstream.Complete(upstreamWireContext(t), contract.Request{})
	if !errors.Is(err, errUpstreamRequestInvalid) {
		t.Fatalf("Complete() zero-request error = %v, want invalid request", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("zero request reached transport %d times, want 0", calls.Load())
	}

	_, err = upstream.Complete(upstreamWireContext(t), request)
	if !errors.Is(err, errUpstreamRequestFailed) || err.Error() != errUpstreamRequestFailed.Error() {
		t.Fatalf("Complete() transport error = %v, want exact sanitized failure", err)
	}
	for _, secret := range []string{upstreamWireCanary, upstreamWireToken, upstream.endpoint} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("transport error leaks %q: %q", secret, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("failing RoundTrip() calls = %d, want 1", calls.Load())
	}

	upstream.client.Transport = upstreamWireRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, upstreamWireTimeoutError{}
	})
	_, err = upstream.Complete(upstreamWireContext(t), request)
	if !errors.Is(err, ErrUpstreamTimeout) || err.Error() != ErrUpstreamTimeout.Error() {
		t.Fatalf("Complete() timeout error = %v, want exact ErrUpstreamTimeout", err)
	}
	if strings.Contains(err.Error(), upstreamWireCanary) {
		t.Fatalf("timeout error leaks source error: %q", err)
	}
}

func TestHTTPUpstreamHandlerIntegration(t *testing.T) {
	t.Run("success strips inbound headers and uses separate credential", func(t *testing.T) {
		observed := make(chan upstreamWireObservation, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			observed <- upstreamWireObserve(request)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"id":"chatcmpl-wire","object":"chat.completion","choices":[]}`)
		}))
		defer server.Close()

		upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(server)), upstreamWireToken)
		handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
		request := responseTestNewRequest()
		request.Header.Set("X-Client-Canary", upstreamWireCanary)
		request.Header.Set("User-Agent", upstreamWireCanary)
		request.Header.Set("Accept-Encoding", "gzip")
		request.Header.Set("Idempotency-Key", upstreamWireCanary)
		recorder := httptest.NewRecorder()

		handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %q", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Body.String(); got != `{"id":"chatcmpl-wire","object":"chat.completion","choices":[]}` {
			t.Fatalf("body = %q, want exact validated upstream response", got)
		}
		upstreamWireRequireFixedRequest(t, <-observed)
		responseTestRequireOutcome(t, permit, admission.ServingCompleted)
	})

	t.Run("redirect becomes static bad gateway without target call", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			targetCalls.Add(1)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Location", target.URL+chatCompletionsPath)
			writer.WriteHeader(http.StatusTemporaryRedirect)
			_, _ = io.WriteString(writer, upstreamWireCanary)
		}))
		defer source.Close()

		upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(source)), upstreamWireToken)
		handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, responseTestNewRequest())

		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body = %q", recorder.Code, recorder.Body.String())
		}
		if targetCalls.Load() != 0 {
			t.Fatalf("redirect target calls = %d, want 0", targetCalls.Load())
		}
		if strings.Contains(recorder.Body.String(), upstreamWireCanary) {
			t.Fatal("bad-gateway response leaks redirect body")
		}
		responseTestRequireOutcome(t, permit, admission.ServingUpstreamFailed)
	})

	t.Run("encoded response becomes static bad gateway", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			writer.Header().Set("Content-Encoding", "gzip")
			_, _ = io.WriteString(writer, upstreamWireCanary)
		}))
		defer server.Close()

		upstream := upstreamWireNew(t, upstreamWireConfig(upstreamWireEndpoint(server)), upstreamWireToken)
		handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, responseTestNewRequest())

		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502; body = %q", recorder.Code, recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), upstreamWireCanary) || recorder.Header().Get("Content-Encoding") != "" {
			t.Fatal("bad-gateway response leaks encoded upstream data or metadata")
		}
		responseTestRequireOutcome(t, permit, admission.ServingUpstreamFailed)
	})
}

func TestHTTPUpstreamHeaderTimeoutMapsToStaticHandlerFailure(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, upstreamWireCanary)
	}))
	defer server.Close()
	defer close(release)

	config := upstreamWireConfig(upstreamWireEndpoint(server))
	config.ResponseHeaderTimeout = 100 * time.Millisecond
	upstream := upstreamWireNew(t, config, upstreamWireToken)
	handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, responseTestNewRequest())

	upstreamWireAwait(t, entered, "upstream request whose headers timed out")
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body = %q", recorder.Code, recorder.Body.String())
	}
	const wantBody = `{"error":{"code":"upstream_timeout","message":"The upstream did not complete before its deadline."}}` + "\n"
	if recorder.Body.String() != wantBody {
		t.Fatalf("body = %q, want exact %q", recorder.Body.String(), wantBody)
	}
	if strings.Contains(recorder.Body.String(), upstreamWireCanary) || strings.Contains(recorder.Body.String(), server.URL) {
		t.Fatal("timeout response leaks upstream details")
	}
	responseTestRequireOutcome(t, permit, admission.ServingUpstreamFailed)
}

func TestHandlerMarksTransportTimeoutWriteFailureAsDownstreamFailure(t *testing.T) {
	upstream := responseTestUpstreamFunc(func(context.Context, contract.Request) (UpstreamResponse, error) {
		return UpstreamResponse{}, ErrUpstreamTimeout
	})
	handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
	writer := newResponseTestFailWriter()
	writer.writeErr = errors.New(upstreamWireCanary)

	handler.ServeHTTP(writer, responseTestNewRequest())

	if writer.status != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", writer.status)
	}
	if writer.body.Len() != 0 {
		t.Fatalf("failed downstream writer retained body %q", writer.body.String())
	}
	responseTestRequireOutcome(t, permit, admission.ServingDownstreamFailed)
}

func TestHTTPUpstreamClientCancellationProducesNoHandlerResponse(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	config := upstreamWireConfig(upstreamWireEndpoint(server))
	config.ResponseHeaderTimeout = 2 * time.Second
	upstream := upstreamWireNew(t, config, upstreamWireToken)
	handler, permit, _ := responseTestNewHandler(t, upstream, context.Background(), 512, time.Second)
	requestContext, cancelRequest := context.WithCancel(context.Background())
	request := responseTestNewRequest().WithContext(requestContext)
	writer := newResponseTestFailWriter()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, request)
		close(done)
	}()

	upstreamWireAwait(t, entered, "upstream request before client cancellation")
	cancelRequest()
	upstreamWireAwait(t, done, "handler completion after client cancellation")
	close(release)
	if writer.status != 0 || writer.body.Len() != 0 {
		t.Fatalf("canceled client received status/body %d %q", writer.status, writer.body.String())
	}
	responseTestRequireOutcome(t, permit, admission.ServingCanceled)
}
