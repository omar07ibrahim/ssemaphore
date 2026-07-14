package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

func TestPrepareGatewayBuildsAndClosesOwnedResourcesExactlyOnce(t *testing.T) {
	const canary = "prepare-success-private-canary"
	policy := buildTestPolicy(t, 43117)
	source := buildTestSecrets(canary)

	prepared, err := prepareGateway(policy, source)
	if err != nil {
		t.Fatalf("prepareGateway() error = %v", err)
	}
	t.Cleanup(func() {
		_ = prepared.close()
	})
	if prepared.state != preparedReady || prepared.handler == nil || prepared.scheduler == nil || prepared.upstream == nil {
		t.Fatalf("prepared gateway is incomplete: %#v", prepared)
	}
	scheduler := prepared.scheduler
	buildTestAssertRedacted(t, prepared, canary)
	if len(source.values) != 0 {
		t.Fatalf("secret source retained consumed values: %v", source.events)
	}

	if err := prepared.close(); err != nil {
		t.Fatalf("first close() error = %v", err)
	}
	if err := prepared.close(); err != nil {
		t.Fatalf("second close() error = %v", err)
	}
	if prepared.state != preparedClosed || prepared.handler != nil || prepared.scheduler != nil || prepared.upstream != nil {
		t.Fatalf("closed gateway retained runtime ownership: %#v", prepared)
	}
	buildTestAssertSchedulerClosed(t, scheduler)
}

func TestPrepareGatewayCleansSchedulerAfterHandlerRejectsBearerGrammar(t *testing.T) {
	const canary = "handler-stage-private-canary"
	policy := buildTestPolicy(t, 43118)
	source := buildTestSecrets(canary)
	source.values["TENANT_TOKEN"] = "invalid tenant token " + canary

	var scheduler *admission.Scheduler
	events := make([]string, 0, 4)
	factories := buildTestFactories()
	factories.newUpstream = func(config httpapi.HTTPUpstreamConfig, token string) (*httpapi.HTTPUpstream, error) {
		events = append(events, "upstream")
		if len(source.values) != 0 {
			t.Fatal("upstream construction started before every secret was consumed")
		}
		return httpapi.NewHTTPUpstream(config, token)
	}
	factories.newScheduler = func(config admission.Config) (*admission.Scheduler, error) {
		events = append(events, "scheduler")
		var err error
		scheduler, err = admission.New(config)
		return scheduler, err
	}
	factories.newHandler = func(
		config httpapi.Config,
		parser *contract.Parser,
		owner *admission.Scheduler,
		upstream httpapi.NonStreamingUpstream,
	) (*httpapi.Handler, error) {
		events = append(events, "handler")
		if owner != scheduler {
			t.Fatal("handler received a scheduler other than the captured owner")
		}
		return httpapi.NewHandler(config, parser, owner, upstream)
	}
	factories.validateServer = func(config server.Config, policy httpapi.TimeoutPolicy) error {
		events = append(events, "server")
		return server.ValidateConfig(config, policy)
	}

	prepared, err := prepareGatewayWith(policy, source, factories)
	if prepared != nil {
		t.Fatalf("prepareGatewayWith() gateway = %#v, want nil", prepared)
	}
	buildTestAssertStaticError(t, err, errGatewayPreparationFailed, canary)
	if got, want := strings.Join(events, ","), "upstream,scheduler,handler"; got != want {
		t.Fatalf("construction order = %q, want %q", got, want)
	}
	if scheduler == nil {
		t.Fatal("handler-stage failure did not create the scheduler under test")
	}
	buildTestAssertSchedulerClosed(t, scheduler)
}

func TestPrepareGatewayRejectsInvalidUpstreamBearerBeforeScheduler(t *testing.T) {
	const canary = "upstream-stage-private-canary"
	policy := buildTestPolicy(t, 43119)
	source := buildTestSecrets(canary)
	source.values["UPSTREAM_TOKEN"] = "invalid upstream token " + canary

	factories := buildTestFactories()
	upstreamCalls := 0
	schedulerCalls := 0
	factories.newUpstream = func(config httpapi.HTTPUpstreamConfig, token string) (*httpapi.HTTPUpstream, error) {
		upstreamCalls++
		return httpapi.NewHTTPUpstream(config, token)
	}
	factories.newScheduler = func(admission.Config) (*admission.Scheduler, error) {
		schedulerCalls++
		return nil, errors.New("scheduler must not be constructed")
	}

	prepared, err := prepareGatewayWith(policy, source, factories)
	if prepared != nil {
		t.Fatalf("prepareGatewayWith() gateway = %#v, want nil", prepared)
	}
	buildTestAssertStaticError(t, err, errGatewayPreparationFailed, canary)
	if upstreamCalls != 1 || schedulerCalls != 0 {
		t.Fatalf("constructor calls = (upstream %d, scheduler %d), want (1, 0)", upstreamCalls, schedulerCalls)
	}
}

func TestPreparedGatewayStartFailureConsumesResourcesAndCannotRetry(t *testing.T) {
	const canary = "listen-failure-private-canary"
	prepared := buildTestPreparedGateway(t, 43120, canary)
	scheduler := prepared.scheduler
	listenCalls := 0

	runtime, err := prepared.start(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		listenCalls++
		return nil, errors.New(canary)
	})
	if runtime != nil {
		t.Fatalf("start() runtime = %#v, want nil", runtime)
	}
	buildTestAssertStaticError(t, err, errGatewayStartFailed, canary)
	if listenCalls != 1 {
		t.Fatalf("listen calls = %d, want 1", listenCalls)
	}
	if prepared.state != preparedClosed || prepared.handler != nil || prepared.scheduler != nil || prepared.upstream != nil {
		t.Fatalf("failed start retained runtime ownership: %#v", prepared)
	}
	buildTestAssertSchedulerClosed(t, scheduler)

	runtime, err = prepared.start(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		panic("listener called for a consumed prepared gateway")
	})
	if runtime != nil {
		t.Fatalf("second start() runtime = %#v, want nil", runtime)
	}
	buildTestAssertStaticError(t, err, errGatewayStartFailed, canary)
	if err := prepared.close(); err != nil {
		t.Fatalf("close() after failed start error = %v", err)
	}
}

func TestPreparedGatewayRejectsAndClosesMismatchedBoundListener(t *testing.T) {
	const canary = "listener-mismatch-private-canary"
	wrong := buildTestReserveLoopback(t)
	expected := buildTestReserveLoopback(t)
	expectedPort := buildTestListenerPort(t, expected)
	prepared := buildTestPreparedGateway(t, expectedPort, canary)
	scheduler := prepared.scheduler

	runtime, err := prepared.start(func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
		if network != "tcp4" || address == nil || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) || address.Port != int(expectedPort) {
			t.Fatalf("listener request = (%q, %#v), want exact configured loopback", network, address)
		}
		return wrong, nil
	})
	if runtime != nil {
		t.Fatalf("start() runtime = %#v, want nil", runtime)
	}
	buildTestAssertStaticError(t, err, errGatewayStartFailed, canary)
	buildTestAssertListenerClosed(t, wrong)
	buildTestAssertSchedulerClosed(t, scheduler)
	if err := prepared.close(); err != nil {
		t.Fatalf("close() after mismatch error = %v", err)
	}
}

func TestPreparedGatewayTransfersOwnershipThroughServeAndShutdown(t *testing.T) {
	const canary = "serve-transfer-private-canary"
	prepared, runtime, listener, scheduler := buildTestTransferredGateway(t, canary)
	if err := prepared.close(); err != nil {
		t.Fatalf("close() after ownership transfer error = %v", err)
	}
	buildTestAssertRedacted(t, prepared, canary)
	buildTestAssertSchedulerOpen(t, scheduler)

	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runtime.Serve()
	}()

	requestContext, cancelRequest := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, "http://"+listener.Addr().String()+"/build-test", nil)
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	client := &http.Client{Transport: transport}
	t.Cleanup(transport.CloseIdleConnections)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("loopback request error = %v", err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		_ = response.Body.Close()
		t.Fatalf("read loopback response error = %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close loopback response error = %v", err)
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := runtime.Shutdown(shutdownContext)
	cancelShutdown()
	if err != nil {
		t.Fatalf("runtime.Shutdown() error = %v", err)
	}
	if result.Forced {
		t.Fatalf("runtime.Shutdown() unexpectedly forced cleanup: %#v", result)
	}
	if err := buildTestAwaitServe(t, serveResult); err != nil {
		t.Fatalf("runtime.Serve() error = %v", err)
	}
	buildTestAssertListenerClosed(t, listener)
	buildTestAssertSchedulerClosed(t, scheduler)
	if err := prepared.close(); err != nil {
		t.Fatalf("second close() after ownership transfer error = %v", err)
	}
}

func TestPreparedGatewayTransferredRuntimeCanShutdownBeforeServe(t *testing.T) {
	const canary = "pre-serve-shutdown-private-canary"
	prepared, runtime, listener, scheduler := buildTestTransferredGateway(t, canary)

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := runtime.Shutdown(shutdownContext)
	cancelShutdown()
	if err != nil {
		t.Fatalf("runtime.Shutdown() before Serve error = %v", err)
	}
	if result.Forced {
		t.Fatalf("runtime.Shutdown() before Serve unexpectedly forced cleanup: %#v", result)
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runtime.Serve()
	}()
	if err := buildTestAwaitServe(t, serveResult); err != nil {
		t.Fatalf("runtime.Serve() after Shutdown error = %v", err)
	}
	buildTestAssertListenerClosed(t, listener)
	buildTestAssertSchedulerClosed(t, scheduler)
	if err := prepared.close(); err != nil {
		t.Fatalf("close() after transferred shutdown error = %v", err)
	}
}

func TestGatewayBuildBoundariesReturnOnlyStaticErrors(t *testing.T) {
	const canary = "build-boundary-private-canary"
	source := buildTestSecrets(canary)
	prepared, err := prepareGateway(nil, source)
	if prepared != nil {
		t.Fatalf("prepareGateway(nil) gateway = %#v, want nil", prepared)
	}
	buildTestAssertStaticError(t, err, errGatewayPreparationFailed, canary)
	if len(source.values) == 0 || len(source.events) != 0 {
		t.Fatal("nil policy consumed the secret source")
	}

	policy := buildTestPolicy(t, 43121)
	prepared, err = prepareGatewayWith(policy, source, gatewayFactories{})
	if prepared != nil {
		t.Fatalf("prepareGatewayWith(empty factories) gateway = %#v, want nil", prepared)
	}
	buildTestAssertStaticError(t, err, errGatewayPreparationFailed, canary)
	if len(source.events) != 0 {
		t.Fatal("invalid factories consumed the secret source")
	}

	var nilPrepared *preparedGateway
	runtime, err := nilPrepared.start(nil)
	if runtime != nil {
		t.Fatalf("nil start() runtime = %#v, want nil", runtime)
	}
	buildTestAssertStaticError(t, err, errGatewayStartFailed, canary)
	buildTestAssertStaticError(t, nilPrepared.close(), errGatewayCleanupFailed, canary)
	buildTestAssertRedacted(t, nilPrepared, canary)

	empty := &preparedGateway{}
	buildTestAssertStaticError(t, empty.close(), errGatewayCleanupFailed, canary)
}

type buildTestSecretSource struct {
	values map[string]string
	events []string
}

func (source *buildTestSecretSource) LookupEnv(name string) (string, bool) {
	source.events = append(source.events, "lookup:"+name)
	value, present := source.values[name]
	return value, present
}

func (source *buildTestSecretSource) Unsetenv(name string) error {
	source.events = append(source.events, "unset:"+name)
	delete(source.values, name)
	return nil
}

func buildTestSecrets(canary string) *buildTestSecretSource {
	return &buildTestSecretSource{values: map[string]string{
		"TENANT_TOKEN":   "tenant-" + canary,
		"UPSTREAM_TOKEN": "upstream-" + canary,
	}}
}

func buildTestFactories() gatewayFactories {
	return gatewayFactories{
		newUpstream:    httpapi.NewHTTPUpstream,
		newScheduler:   admission.New,
		newHandler:     httpapi.NewHandler,
		validateServer: server.ValidateConfig,
	}
}

func buildTestPolicy(t *testing.T, port uint16) *validatedPolicy {
	t.Helper()
	parser, err := contract.NewParser("portfolio-model", contract.Limits{
		MaxBodyBytes:        512,
		MaxMessageCount:     4,
		MaxMessageTextBytes: 128,
		MaxCompletionTokens: 64,
		CompletionWeight:    1,
		MaxRequestUnits:     1024,
	})
	if err != nil {
		t.Fatalf("contract.NewParser() error = %v", err)
	}
	return &validatedPolicy{
		listener: listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: port},
		parser:   parser,
		admission: admission.Config{
			MaxBodyBytes:    512,
			MaxRequestUnits: 1024,
			BaseQuantum:     1024,
			DeficitCap:      2048,
			GlobalQueue:     admission.QueueLimits{Count: 2, Bytes: 1024, Work: 2048},
			GlobalInflight:  admission.InflightLimits{Count: 1, Work: 1024},
			Tenants: []admission.TenantConfig{{
				ID:       1,
				Weight:   1,
				Queue:    admission.QueueLimits{Count: 2, Bytes: 1024, Work: 2048},
				Inflight: admission.InflightLimits{Count: 1, Work: 1024},
			}},
		},
		http: httpapi.Config{
			DefaultQueueTimeout:    time.Second,
			BodyReadTimeout:        time.Second,
			UpstreamTimeout:        time.Second,
			MaxResponseBodyBytes:   512,
			GlobalPreDispatchLimit: 1,
			TenantPreDispatch: []httpapi.TenantPreDispatchLimit{{
				Tenant: 1,
				Count:  1,
			}},
		},
		upstream: httpapi.HTTPUpstreamConfig{
			Endpoint:               "https://api.example.com/v1/chat/completions",
			ConnectTimeout:         time.Second,
			TLSHandshakeTimeout:    time.Second,
			ResponseHeaderTimeout:  time.Second,
			IdleConnectionTimeout:  time.Second,
			MaxResponseHeaderBytes: 64 << 10,
			MaxConnections:         1,
		},
		server: server.Config{
			HeaderReadTimeout:       time.Second,
			ResponseWriteTimeout:    time.Second,
			IdleTimeout:             time.Second,
			GraceTimeout:            time.Second,
			ForceTimeout:            time.Second,
			HeaderReadEnvelopeBytes: 8 << 10,
			MaxConnections:          1,
		},
		credentials:      []credentialReference{{tenant: 1, env: "TENANT_TOKEN"}},
		upstreamTokenEnv: "UPSTREAM_TOKEN",
		shutdownWait:     8 * time.Second,
	}
}

func buildTestPreparedGateway(t *testing.T, port uint16, canary string) *preparedGateway {
	t.Helper()
	prepared, err := prepareGateway(buildTestPolicy(t, port), buildTestSecrets(canary))
	if err != nil {
		t.Fatalf("prepareGateway() error = %v", err)
	}
	t.Cleanup(func() {
		if err := prepared.close(); err != nil {
			t.Errorf("prepared.close() cleanup error = %v", err)
		}
	})
	return prepared
}

func buildTestTransferredGateway(
	t *testing.T,
	canary string,
) (*preparedGateway, *server.Server, *net.TCPListener, *admission.Scheduler) {
	t.Helper()
	listener := buildTestReserveLoopback(t)
	port := buildTestListenerPort(t, listener)
	prepared := buildTestPreparedGateway(t, port, canary)
	scheduler := prepared.scheduler
	runtime, err := prepared.start(func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
		if network != "tcp4" || address == nil || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) || address.Port != int(port) {
			t.Fatalf("listener request = (%q, %#v), want exact configured loopback", network, address)
		}
		return listener, nil
	})
	if err != nil {
		t.Fatalf("prepared.start() error = %v", err)
	}
	if runtime == nil {
		t.Fatal("prepared.start() runtime is nil")
	}
	if prepared.state != preparedTransferred || prepared.handler != nil || prepared.scheduler != nil || prepared.upstream != nil {
		t.Fatalf("transferred gateway retained runtime ownership: %#v", prepared)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("runtime.Shutdown() cleanup error = %v", err)
		}
	})
	return prepared, runtime, listener, scheduler
}

func buildTestReserveLoopback(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("reserve loopback listener error = %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})
	return listener
}

func buildTestListenerPort(t *testing.T, listener *net.TCPListener) uint16 {
	t.Helper()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || address.Port <= 0 || address.Port > 65535 {
		t.Fatalf("listener address = %#v, want bounded TCP port", listener.Addr())
	}
	return uint16(address.Port)
}

func buildTestAssertListenerClosed(t *testing.T, listener *net.TCPListener) {
	t.Helper()
	if listener == nil {
		t.Fatal("listener under test is nil")
	}
	if err := listener.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed listener SetDeadline() error = %v, want net.ErrClosed", err)
	}
}

func buildTestAssertSchedulerClosed(t *testing.T, scheduler *admission.Scheduler) {
	t.Helper()
	if scheduler == nil {
		t.Fatal("scheduler under test is nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.Close(ctx); err != nil {
		t.Fatalf("scheduler remained live after ownership cleanup: canceled Close() error = %v", err)
	}
}

func buildTestAssertSchedulerOpen(t *testing.T, scheduler *admission.Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := scheduler.Snapshot(ctx); err != nil {
		t.Fatalf("scheduler is not live before runtime shutdown: %v", err)
	}
}

func buildTestAwaitServe(t *testing.T, result <-chan error) error {
	t.Helper()
	watchdog, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case err := <-result:
		return err
	case <-watchdog.Done():
		t.Fatal("timed out waiting for Serve to return")
		return watchdog.Err()
	}
}

func buildTestAssertStaticError(t *testing.T, got, want error, canaries ...string) {
	t.Helper()
	if got != want || !errors.Is(got, want) {
		t.Fatalf("error = %#v, want exact static error %#v", got, want)
	}
	for _, formatted := range []string{got.Error(), fmt.Sprint(got), fmt.Sprintf("%#v", got)} {
		for _, canary := range canaries {
			if strings.Contains(formatted, canary) {
				t.Fatalf("static error leaked canary %q: %q", canary, formatted)
			}
		}
	}
}

func buildTestAssertRedacted(t *testing.T, value any, canaries ...string) {
	t.Helper()
	for _, formatted := range []string{fmt.Sprint(value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
		if !strings.Contains(formatted, "redacted") {
			t.Fatalf("formatted value is not explicitly redacted: %q", formatted)
		}
		for _, canary := range canaries {
			if strings.Contains(formatted, canary) {
				t.Fatalf("formatted value leaked canary %q: %q", canary, formatted)
			}
		}
	}
}
