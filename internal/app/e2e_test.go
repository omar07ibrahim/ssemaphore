package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const (
	e2eTestTenantEnvironment   = "SSEMAPHORE_E2E_TENANT_TOKEN"
	e2eTestUpstreamEnvironment = "SSEMAPHORE_E2E_UPSTREAM_TOKEN"
	e2eTestTenantToken         = "tenant-e2e-private-canary"
	e2eTestUpstreamToken       = "upstream-e2e-private-canary"
	e2eTestRequestBody         = `{"model":"portfolio-model","messages":[{"role":"user","content":"E2E_REQUEST_BODY_CANARY"}],"max_completion_tokens":8}`
	e2eTestResponseBody        = `{"id":"chatcmpl-e2e","object":"chat.completion","choices":[]}`
	e2eTestPath                = "/v1/chat/completions"
)

type e2eTestSecretSource struct {
	mu     sync.Mutex
	values map[string]string
	events []string
}

func (source *e2eTestSecretSource) LookupEnv(name string) (string, bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.events = append(source.events, "lookup:"+name)
	value, present := source.values[name]
	return value, present
}

func (source *e2eTestSecretSource) Unsetenv(name string) error {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.events = append(source.events, "unset:"+name)
	delete(source.values, name)
	return nil
}

func (source *e2eTestSecretSource) snapshot() (map[string]string, []string) {
	source.mu.Lock()
	defer source.mu.Unlock()
	values := make(map[string]string, len(source.values))
	for name, value := range source.values {
		values[name] = value
	}
	return values, append([]string(nil), source.events...)
}

type e2eTestUpstreamObservation struct {
	method           string
	path             string
	rawQuery         string
	protocolMajor    int
	contentLength    int64
	transferEncoding []string
	header           http.Header
	body             []byte
	readErr          error
}

type e2eTestListenRequest struct {
	network string
	address *net.TCPAddr
}

func TestRunnableGatewayEndToEndTerminatesCleanly(t *testing.T) {
	if e2eTestTenantToken == e2eTestUpstreamToken {
		t.Fatal("test credentials must be distinct")
	}

	upstreamObservations := make(chan e2eTestUpstreamObservation, 2)
	var upstreamCalls atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		body, readErr := io.ReadAll(io.LimitReader(request.Body, int64(len(e2eTestRequestBody))+1))
		upstreamObservations <- e2eTestUpstreamObservation{
			method:           request.Method,
			path:             request.URL.Path,
			rawQuery:         request.URL.RawQuery,
			protocolMajor:    request.ProtoMajor,
			contentLength:    request.ContentLength,
			transferEncoding: append([]string(nil), request.TransferEncoding...),
			header:           request.Header.Clone(),
			body:             body,
			readErr:          readErr,
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, e2eTestResponseBody)
	}))
	upstream.Start()
	t.Cleanup(upstream.Close)

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse httptest upstream URL: %v", err)
	}
	upstreamAddress, err := netip.ParseAddr(upstreamURL.Hostname())
	if err != nil || !upstreamAddress.IsLoopback() || upstreamAddress.Zone() != "" || upstreamURL.Scheme != "http" {
		t.Fatalf("httptest upstream = %q, want numeric plaintext loopback", upstream.URL)
	}

	gatewayListener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("reserve gateway loopback listener: %v", err)
	}
	t.Cleanup(func() {
		_ = gatewayListener.Close()
	})
	gatewayAddress, ok := gatewayListener.Addr().(*net.TCPAddr)
	if !ok || gatewayAddress == nil || !gatewayAddress.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		gatewayAddress.Port <= 0 || gatewayAddress.Port > 65_535 {
		t.Fatalf("reserved gateway address = %#v, want exact IPv4 loopback", gatewayListener.Addr())
	}

	document := canonicalPolicyDocument()
	document.Listener.Host = "127.0.0.1"
	document.Listener.Port = uint64(gatewayAddress.Port)
	document.Admission.Tenants = document.Admission.Tenants[:1]
	document.Admission.Tenants[0].BearerTokenEnvs = []string{e2eTestTenantEnvironment}
	document.Upstream.Endpoint = upstream.URL + e2eTestPath
	document.Upstream.BearerTokenEnv = e2eTestUpstreamEnvironment
	policyPath := writePolicyFixture(t, marshalPolicyDocument(t, document))

	secrets := &e2eTestSecretSource{values: map[string]string{
		e2eTestTenantEnvironment:   e2eTestTenantToken,
		e2eTestUpstreamEnvironment: e2eTestUpstreamToken,
	}}
	events := make(chan os.Signal, 1)
	listenRequests := make(chan e2eTestListenRequest, 2)
	var subscribeCalls atomic.Int32
	var stopCalls atomic.Int32
	dependencies := commandDependencies{
		secrets: secrets,
		listenTCP: func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
			var copied *net.TCPAddr
			if address != nil {
				copied = &net.TCPAddr{
					IP:   append(net.IP(nil), address.IP...),
					Port: address.Port,
					Zone: address.Zone,
				}
			}
			listenRequests <- e2eTestListenRequest{network: network, address: copied}
			return gatewayListener, nil
		},
		subscribe: func() (<-chan os.Signal, func(), error) {
			subscribeCalls.Add(1)
			return events, func() {
				stopCalls.Add(1)
			}, nil
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCodes := make(chan int, 1)
	go func() {
		exitCodes <- runCommand(
			[]string{"serve", "--config", policyPath},
			&stdout,
			&stderr,
			dependencies,
		)
	}()

	listenRequest := e2eTestAwait(t, listenRequests, "gateway listener transfer")
	if listenRequest.network != "tcp4" || listenRequest.address == nil ||
		!listenRequest.address.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		listenRequest.address.Port != gatewayAddress.Port || listenRequest.address.Zone != "" {
		t.Fatalf("listener request = (%q, %#v), want exact configured IPv4 loopback", listenRequest.network, listenRequest.address)
	}
	if subscribeCalls.Load() != 1 {
		t.Fatalf("signal subscriptions = %d, want 1", subscribeCalls.Load())
	}
	remainingSecrets, secretEvents := secrets.snapshot()
	if len(remainingSecrets) != 0 {
		t.Fatal("fake source retained consumed secrets")
	}
	wantSecretEvents := []string{
		"lookup:" + e2eTestTenantEnvironment,
		"unset:" + e2eTestTenantEnvironment,
		"lookup:" + e2eTestUpstreamEnvironment,
		"unset:" + e2eTestUpstreamEnvironment,
	}
	if strings.Join(secretEvents, "\n") != strings.Join(wantSecretEvents, "\n") {
		t.Fatalf("secret events = %v, want %v", secretEvents, wantSecretEvents)
	}

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		Protocols:         protocols,
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}
	requestContext, cancelRequest := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodPost,
		"http://"+gatewayAddress.String()+e2eTestPath,
		strings.NewReader(e2eTestRequestBody),
	)
	if err != nil {
		t.Fatalf("build gateway request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+e2eTestTenantToken)
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("gateway loopback request: %v", err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read/close gateway response errors = (%v, %v)", readErr, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("gateway status = %d, want 200", response.StatusCode)
	}
	if response.ProtoMajor != 1 {
		t.Fatalf("gateway response HTTP protocol major = %d, want 1", response.ProtoMajor)
	}
	if string(responseBody) != e2eTestResponseBody {
		t.Fatalf("gateway body differs from the exact upstream response")
	}
	if e2eTestHeaderContains(response.Header, e2eTestUpstreamToken) ||
		bytes.Contains(responseBody, []byte(e2eTestUpstreamToken)) {
		t.Fatal("gateway response disclosed the upstream credential")
	}

	observation := e2eTestAwait(t, upstreamObservations, "upstream request")
	e2eTestRequireUpstreamRequest(t, observation)

	events <- syscall.SIGTERM
	exitCode := e2eTestAwait(t, exitCodes, "terminal command cleanup")
	if exitCode != exitSuccess {
		t.Fatalf("serve exit code = %d, want 0", exitCode)
	}
	if stopCalls.Load() != 1 {
		t.Fatalf("signal stop calls = %d, want 1", stopCalls.Load())
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want exactly 1", upstreamCalls.Load())
	}
	select {
	case <-upstreamObservations:
		t.Fatal("received more than one upstream request observation")
	default:
	}
	select {
	case extra := <-listenRequests:
		t.Fatalf("received a second listener request: (%q, %#v)", extra.network, extra.address)
	default:
	}

	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("successful serve command wrote to stdout or stderr")
	}
	for _, canary := range []string{
		e2eTestTenantToken,
		e2eTestUpstreamToken,
		e2eTestRequestBody,
		"E2E_REQUEST_BODY_CANARY",
		policyPath,
	} {
		if strings.Contains(stdout.String(), canary) || strings.Contains(stderr.String(), canary) {
			t.Fatalf("serve output disclosed a private canary")
		}
	}

	if err := gatewayListener.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("gateway listener remained open: SetDeadline error = %v", err)
	}
	rebound, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: gatewayAddress.Port,
	})
	if err != nil {
		t.Fatalf("rebind exact released gateway port: %v", err)
	}
	reboundAddress, ok := rebound.Addr().(*net.TCPAddr)
	if !ok || reboundAddress == nil || !reboundAddress.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		reboundAddress.Port != gatewayAddress.Port {
		_ = rebound.Close()
		t.Fatalf("rebound address = %#v, want exact released loopback port", rebound.Addr())
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("close rebound listener: %v", err)
	}
}

func e2eTestRequireUpstreamRequest(t *testing.T, observation e2eTestUpstreamObservation) {
	t.Helper()
	if observation.readErr != nil {
		t.Fatalf("read upstream request body: %v", observation.readErr)
	}
	if observation.method != http.MethodPost || observation.path != e2eTestPath || observation.rawQuery != "" {
		t.Fatalf("upstream target = %s %q?%q, want exact POST path", observation.method, observation.path, observation.rawQuery)
	}
	if observation.protocolMajor != 1 {
		t.Fatalf("upstream HTTP protocol major = %d, want 1", observation.protocolMajor)
	}
	if observation.contentLength != int64(len(e2eTestRequestBody)) || len(observation.transferEncoding) != 0 {
		t.Fatalf("upstream framing = length %d, transfer %v", observation.contentLength, observation.transferEncoding)
	}
	if string(observation.body) != e2eTestRequestBody {
		t.Fatal("upstream body differs from the exact validated request")
	}
	e2eTestRequireSingleHeader(t, observation.header, "Accept", "application/json")
	e2eTestRequireSingleHeader(t, observation.header, "Authorization", "Bearer "+e2eTestUpstreamToken)
	e2eTestRequireSingleHeader(t, observation.header, "Content-Type", "application/json")
	e2eTestRequireSingleHeader(t, observation.header, "User-Agent", "ssemaphore")
	if e2eTestHeaderContains(observation.header, e2eTestTenantToken) {
		t.Fatal("upstream headers disclosed the tenant credential")
	}
}

func e2eTestRequireSingleHeader(t *testing.T, header http.Header, name, want string) {
	t.Helper()
	values := header.Values(name)
	if len(values) != 1 || values[0] != want {
		t.Fatalf("upstream %s values = %v, want exact singleton", name, values)
	}
}

func e2eTestHeaderContains(header http.Header, canary string) bool {
	for name, values := range header {
		if strings.Contains(name, canary) || strings.Contains(strings.Join(values, "\n"), canary) {
			return true
		}
	}
	return false
}

func e2eTestAwait[T any](t *testing.T, channel <-chan T, operation string) T {
	t.Helper()
	watchdog := time.NewTimer(5 * time.Second)
	defer watchdog.Stop()
	select {
	case value := <-channel:
		return value
	case <-watchdog.C:
		t.Fatalf("timed out waiting for %s", operation)
		var zero T
		return zero
	}
}
