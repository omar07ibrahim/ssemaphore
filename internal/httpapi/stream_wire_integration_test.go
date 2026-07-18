package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
)

const (
	streamWireTenantToken   = "stream-wire-tenant-token"
	streamWireUpstreamToken = "stream-wire-upstream-token"
)

type streamWireObservation struct {
	header        http.Header
	body          string
	contentLength int64
	protocolMajor int
}

func TestStreamingLoopbackDeliversFirstChunkBeforeUpstreamCompletion(t *testing.T) {
	observations := make(chan streamWireObservation, 1)
	releaseUpstream := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpstream) }) }
	defer release()
	var upstreamCalls atomic.Int32

	upstreamServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		body, _ := io.ReadAll(io.LimitReader(request.Body, int64(len(streamIntegrationRequest))+1))
		observations <- streamWireObservation{
			header:        request.Header.Clone(),
			body:          string(body),
			contentLength: request.ContentLength,
			protocolMajor: request.ProtoMajor,
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("X-Upstream-Secret", "must-not-cross-the-relay")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, relayTestChunkOne)
		_ = http.NewResponseController(writer).Flush()
		select {
		case <-releaseUpstream:
		case <-request.Context().Done():
			return
		}
		_, _ = io.WriteString(writer, relayTestChunkTwo+relayTestDone)
		_ = http.NewResponseController(writer).Flush()
	}))
	t.Cleanup(upstreamServer.Close)

	upstreamURL, err := url.Parse(upstreamServer.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	address, err := netip.ParseAddr(upstreamURL.Hostname())
	if err != nil || !address.IsLoopback() || address.Zone() != "" {
		t.Fatalf("httptest upstream host = %q, want numeric loopback", upstreamURL.Hostname())
	}
	upstream, err := NewHTTPUpstream(HTTPUpstreamConfig{
		Endpoint:               upstreamServer.URL + chatCompletionsPath,
		ConnectTimeout:         time.Second,
		TLSHandshakeTimeout:    time.Second,
		ResponseHeaderTimeout:  time.Second,
		IdleConnectionTimeout:  time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		MaxConnections:         2,
	}, streamWireUpstreamToken)
	if err != nil {
		t.Fatalf("NewHTTPUpstream() error = %v", err)
	}
	t.Cleanup(upstream.CloseIdleConnections)

	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	config := configTestBaseHandlerConfig()
	config.Credentials = []Credential{
		{Tenant: configTestTenantOne, Token: streamWireTenantToken},
		{Tenant: configTestTenantTwo, Token: "tenant-two-primary"},
	}
	handler, err := NewHandler(config, parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	gateway := httptest.NewServer(handler)
	t.Cleanup(gateway.Close)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy:              nil,
		DisableKeepAlives:  true,
		DisableCompression: true,
		Protocols:          protocols,
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}
	requestContext, cancelRequest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodPost,
		gateway.URL+chatCompletionsPath,
		strings.NewReader(streamIntegrationRequest),
	)
	if err != nil {
		t.Fatalf("build gateway request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+streamWireTenantToken)
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("streaming gateway request: %v", err)
	}
	defer response.Body.Close()
	first := make([]byte, len(relayTestChunkOne))
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatalf("read first relayed chunk before upstream release: %v", err)
	}
	if string(first) != relayTestChunkOne {
		t.Fatalf("first relayed bytes = %q, want exact first chunk", first)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("upstream calls before completion = %d, want exactly 1", upstreamCalls.Load())
	}
	if response.StatusCode != http.StatusOK || response.ProtoMajor != 1 {
		t.Fatalf("gateway response = status:%d HTTP/%d, want 200 over HTTP/1", response.StatusCode, response.ProtoMajor)
	}
	if response.ContentLength != -1 || response.Header.Get("Content-Length") != "" {
		t.Fatalf("stream response content length = %d/%q, want absent", response.ContentLength, response.Header.Get("Content-Length"))
	}
	if response.Header.Get("Content-Type") != "text/event-stream" ||
		response.Header.Get("Cache-Control") != "no-store" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" ||
		response.Header.Get("X-Upstream-Secret") != "" ||
		!validRequestID(response.Header.Get(requestIDHeader)) {
		t.Fatalf("gateway stream headers are not the exact safe set: %#v", response.Header)
	}

	release()
	rest, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close remaining stream errors = (%v, %v)", readErr, closeErr)
	}
	if want := relayTestChunkTwo + relayTestDone; string(rest) != want {
		t.Fatalf("remaining relayed stream = %q, want exact %q", rest, want)
	}

	select {
	case observation := <-observations:
		if observation.protocolMajor != 1 || observation.contentLength != int64(len(streamIntegrationRequest)) ||
			observation.body != streamIntegrationRequest {
			t.Fatalf("upstream request framing = HTTP/%d length:%d body:%q", observation.protocolMajor, observation.contentLength, observation.body)
		}
		requireSingleHeaderValue(t, observation.header, "Accept", "text/event-stream")
		requireSingleHeaderValue(t, observation.header, "Authorization", "Bearer "+streamWireUpstreamToken)
		requireSingleHeaderValue(t, observation.header, "Content-Type", "application/json")
		if headerContainsValue(observation.header, streamWireTenantToken) {
			t.Fatal("upstream request disclosed the tenant credential")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not observe the upstream request")
	}

	terminal := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global == (admission.Counters{})
	})
	if terminal.Global != (admission.Counters{}) || upstreamCalls.Load() != 1 {
		t.Fatalf("terminal state = counters:%+v calls:%d, want zero/one", terminal.Global, upstreamCalls.Load())
	}
}

func requireSingleHeaderValue(t *testing.T, header http.Header, name, want string) {
	t.Helper()
	values := header.Values(name)
	if len(values) != 1 || values[0] != want {
		t.Fatalf("%s values = %v, want [%q]", name, values, want)
	}
}

func headerContainsValue(header http.Header, canary string) bool {
	for name, values := range header {
		if strings.Contains(name, canary) || strings.Contains(strings.Join(values, "\n"), canary) {
			return true
		}
	}
	return false
}
