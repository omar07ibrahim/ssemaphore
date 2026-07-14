package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

const startupCleanupTimeout = 5 * time.Second

var (
	errGatewayPreparationFailed = errors.New("gateway preparation failed")
	errGatewayStartFailed       = errors.New("gateway start failed")
	errGatewayCleanupFailed     = errors.New("gateway cleanup failed")
)

type preparedState uint8

const (
	preparedReady preparedState = iota + 1
	preparedTransferred
	preparedClosed
)

type preparedGateway struct {
	mu    sync.Mutex
	state preparedState

	listener     listenerPlan
	serverConfig server.Config
	shutdownWait time.Duration

	handler   *httpapi.Handler
	scheduler *admission.Scheduler
	upstream  *httpapi.HTTPUpstream
}

type gatewayFactories struct {
	newUpstream    func(httpapi.HTTPUpstreamConfig, string) (*httpapi.HTTPUpstream, error)
	newScheduler   func(admission.Config) (*admission.Scheduler, error)
	newHandler     func(httpapi.Config, *contract.Parser, *admission.Scheduler, httpapi.NonStreamingUpstream) (*httpapi.Handler, error)
	validateServer func(server.Config, httpapi.TimeoutPolicy) error
}

func (*preparedGateway) String() string   { return "app.preparedGateway{redacted}" }
func (*preparedGateway) GoString() string { return "app.preparedGateway{redacted}" }

func prepareGateway(policy *validatedPolicy, source secretSource) (*preparedGateway, error) {
	return prepareGatewayWith(policy, source, gatewayFactories{
		newUpstream:    httpapi.NewHTTPUpstream,
		newScheduler:   admission.New,
		newHandler:     httpapi.NewHandler,
		validateServer: server.ValidateConfig,
	})
}

func prepareGatewayWith(
	policy *validatedPolicy,
	source secretSource,
	factories gatewayFactories,
) (*preparedGateway, error) {
	if policy == nil {
		return nil, errGatewayPreparationFailed
	}
	if factories.newUpstream == nil || factories.newScheduler == nil ||
		factories.newHandler == nil || factories.validateServer == nil {
		return nil, errGatewayPreparationFailed
	}

	secrets, err := resolveSecrets(policy, source)
	if err != nil {
		return nil, errGatewayPreparationFailed
	}
	defer secrets.clear()

	upstream, err := factories.newUpstream(policy.upstream, secrets.upstream)
	if err != nil {
		if upstream != nil {
			upstream.CloseIdleConnections()
		}
		return nil, errGatewayPreparationFailed
	}
	if upstream == nil {
		return nil, errGatewayPreparationFailed
	}

	scheduler, err := factories.newScheduler(policy.admission)
	if err != nil {
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayPreparationFailed
	}
	if scheduler == nil {
		upstream.CloseIdleConnections()
		return nil, errGatewayPreparationFailed
	}

	httpConfig := policy.http
	httpConfig.Credentials = secrets.credentials
	handler, err := factories.newHandler(httpConfig, policy.parser, scheduler, upstream)
	if err != nil {
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayPreparationFailed
	}
	if handler == nil {
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayPreparationFailed
	}
	if err := factories.validateServer(policy.server, handler.TimeoutPolicy()); err != nil {
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayPreparationFailed
	}

	return &preparedGateway{
		state:        preparedReady,
		listener:     policy.listener,
		serverConfig: policy.server,
		shutdownWait: policy.shutdownWait,
		handler:      handler,
		scheduler:    scheduler,
		upstream:     upstream,
	}, nil
}

// start consumes the prepared gateway on every path. On success, the returned
// Server exclusively owns the listener, scheduler, handler, and upstream. On
// failure, start closes every resource it opened before returning.
func (prepared *preparedGateway) start(listenTCP listenTCPFunc) (*server.Server, error) {
	if prepared == nil {
		return nil, errGatewayStartFailed
	}

	prepared.mu.Lock()
	if prepared.state != preparedReady {
		prepared.mu.Unlock()
		return nil, errGatewayStartFailed
	}

	listener, err := prepared.listener.listen(listenTCP)
	if err != nil {
		scheduler, upstream := prepared.consumeForClose()
		prepared.mu.Unlock()
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayStartFailed
	}

	runtime, err := server.New(
		prepared.serverConfig,
		listener,
		prepared.handler,
		prepared.scheduler,
	)
	if err != nil {
		_ = listener.Close()
		scheduler, upstream := prepared.consumeForClose()
		prepared.mu.Unlock()
		cleanupPreparedResources(scheduler, upstream)
		return nil, errGatewayStartFailed
	}

	prepared.state = preparedTransferred
	prepared.handler = nil
	prepared.scheduler = nil
	prepared.upstream = nil
	prepared.mu.Unlock()
	return runtime, nil
}

func (prepared *preparedGateway) close() error {
	if prepared == nil {
		return errGatewayCleanupFailed
	}

	prepared.mu.Lock()
	if prepared.state == preparedTransferred || prepared.state == preparedClosed {
		prepared.mu.Unlock()
		return nil
	}
	if prepared.state != preparedReady {
		prepared.mu.Unlock()
		return errGatewayCleanupFailed
	}
	scheduler, upstream := prepared.consumeForClose()
	prepared.mu.Unlock()
	if !cleanupPreparedResources(scheduler, upstream) {
		return errGatewayCleanupFailed
	}
	return nil
}

// consumeForClose requires prepared.mu and moves all runtime resources into
// local cleanup ownership.
func (prepared *preparedGateway) consumeForClose() (*admission.Scheduler, *httpapi.HTTPUpstream) {
	prepared.state = preparedClosed
	scheduler := prepared.scheduler
	upstream := prepared.upstream
	prepared.handler = nil
	prepared.scheduler = nil
	prepared.upstream = nil
	return scheduler, upstream
}

func cleanupPreparedResources(scheduler *admission.Scheduler, upstream *httpapi.HTTPUpstream) bool {
	complete := true
	if scheduler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), startupCleanupTimeout)
		if err := scheduler.Close(ctx); err != nil {
			complete = false
		}
		cancel()
	}
	if upstream != nil {
		upstream.CloseIdleConnections()
	}
	return complete
}
