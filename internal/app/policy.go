package app

import (
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

const (
	policySchemaVersion       = 2
	maximumPolicyTenants      = 1024
	maximumPolicyCredentials  = 4096
	maximumPolicyTimeoutMS    = 3_600_000
	maximumForceTimeoutMS     = 60_000
	maximumEnvironmentNameLen = 128
	shutdownSchedulingMargin  = 5 * time.Second
)

var errPolicyRejected = errors.New("gateway policy is rejected")

type policyDocument struct {
	SchemaVersion uint64          `json:"schema_version"`
	Listener      listenerPolicy  `json:"listener"`
	Contract      contractPolicy  `json:"contract"`
	Admission     admissionPolicy `json:"admission"`
	HTTP          httpPolicy      `json:"http"`
	Upstream      upstreamPolicy  `json:"upstream"`
	Server        serverPolicy    `json:"server"`
}

type contractPolicy struct {
	PublicModel         string `json:"public_model"`
	MaxBodyBytes        uint64 `json:"max_body_bytes"`
	MaxMessageCount     uint64 `json:"max_message_count"`
	MaxMessageTextBytes uint64 `json:"max_message_text_bytes"`
	MaxCompletionTokens uint64 `json:"max_completion_tokens"`
	CompletionWeight    uint64 `json:"completion_weight"`
	MaxRequestUnits     uint64 `json:"max_request_units"`
}

type admissionPolicy struct {
	BaseQuantum    uint64         `json:"base_quantum"`
	DeficitCap     uint64         `json:"deficit_cap"`
	GlobalQueue    queuePolicy    `json:"global_queue"`
	GlobalInflight inflightPolicy `json:"global_inflight"`
	Tenants        []tenantPolicy `json:"tenants"`
}

type queuePolicy struct {
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes"`
	Work  uint64 `json:"work"`
}

type inflightPolicy struct {
	Count uint64 `json:"count"`
	Work  uint64 `json:"work"`
}

type tenantPolicy struct {
	ID               uint64         `json:"id"`
	Weight           uint64         `json:"weight"`
	Queue            queuePolicy    `json:"queue"`
	Inflight         inflightPolicy `json:"inflight"`
	PreDispatchCount uint64         `json:"pre_dispatch_count"`
	BearerTokenEnvs  []string       `json:"bearer_token_envs"`
}

type httpPolicy struct {
	DefaultQueueTimeoutMS  uint64 `json:"default_queue_timeout_ms"`
	BodyReadTimeoutMS      uint64 `json:"body_read_timeout_ms"`
	UpstreamTimeoutMS      uint64 `json:"upstream_timeout_ms"`
	StreamReadTimeoutMS    uint64 `json:"stream_read_timeout_ms"`
	StreamEventTimeoutMS   uint64 `json:"stream_event_timeout_ms"`
	MaxResponseBodyBytes   uint64 `json:"max_response_body_bytes"`
	MaxStreamEventBytes    uint64 `json:"max_stream_event_bytes"`
	MaxStreamEvents        uint64 `json:"max_stream_events"`
	GlobalPreDispatchCount uint64 `json:"global_pre_dispatch_count"`
}

type upstreamPolicy struct {
	Endpoint                string `json:"endpoint"`
	BearerTokenEnv          string `json:"bearer_token_env"`
	ConnectTimeoutMS        uint64 `json:"connect_timeout_ms"`
	TLSHandshakeTimeoutMS   uint64 `json:"tls_handshake_timeout_ms"`
	ResponseHeaderTimeoutMS uint64 `json:"response_header_timeout_ms"`
	IdleConnectionTimeoutMS uint64 `json:"idle_connection_timeout_ms"`
	MaxResponseHeaderBytes  uint64 `json:"max_response_header_bytes"`
	MaxConnections          uint64 `json:"max_connections"`
}

type serverPolicy struct {
	HeaderReadTimeoutMS     uint64 `json:"header_read_timeout_ms"`
	ResponseWriteTimeoutMS  uint64 `json:"response_write_timeout_ms"`
	IdleTimeoutMS           uint64 `json:"idle_timeout_ms"`
	GraceTimeoutMS          uint64 `json:"grace_timeout_ms"`
	ForceTimeoutMS          uint64 `json:"force_timeout_ms"`
	HeaderReadEnvelopeBytes uint64 `json:"header_read_envelope_bytes"`
	MaxConnections          uint64 `json:"max_connections"`
}

type credentialReference struct {
	tenant admission.TenantID
	env    string
}

type validatedPolicy struct {
	listener listenerPlan
	parser   *contract.Parser

	admission admission.Config
	http      httpapi.Config
	upstream  httpapi.HTTPUpstreamConfig
	server    server.Config

	credentials      []credentialReference
	upstreamTokenEnv string
	shutdownWait     time.Duration
}

func (*validatedPolicy) String() string   { return "app.validatedPolicy{redacted}" }
func (*validatedPolicy) GoString() string { return "app.validatedPolicy{redacted}" }

func loadPolicy(path string) (*validatedPolicy, error) {
	data, err := readPolicyFile(path)
	if err != nil {
		return nil, errPolicyReadFailed
	}
	return parsePolicy(data)
}

func parsePolicy(data []byte) (*validatedPolicy, error) {
	document := policyDocument{}
	if err := decodePolicyJSON(data, &document); err != nil {
		return nil, errPolicyRejected
	}
	return validatePolicy(document)
}

func validatePolicy(document policyDocument) (*validatedPolicy, error) {
	if document.SchemaVersion != policySchemaVersion {
		return nil, errPolicyRejected
	}

	listener, err := makeListenerPlan(document.Listener)
	if err != nil {
		return nil, errPolicyRejected
	}

	parser, err := contract.NewParser(document.Contract.PublicModel, contract.Limits{
		MaxBodyBytes:        document.Contract.MaxBodyBytes,
		MaxMessageCount:     document.Contract.MaxMessageCount,
		MaxMessageTextBytes: document.Contract.MaxMessageTextBytes,
		MaxCompletionTokens: document.Contract.MaxCompletionTokens,
		CompletionWeight:    document.Contract.CompletionWeight,
		MaxRequestUnits:     document.Contract.MaxRequestUnits,
	})
	if err != nil {
		return nil, errPolicyRejected
	}

	admissionConfig, tenantLimits, credentials, err := validateAdmissionPolicy(document)
	if err != nil || admission.ValidateConfig(admissionConfig) != nil {
		return nil, errPolicyRejected
	}

	defaultQueueTimeout, ok := policyDuration(document.HTTP.DefaultQueueTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return nil, errPolicyRejected
	}
	bodyReadTimeout, ok := policyDuration(document.HTTP.BodyReadTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return nil, errPolicyRejected
	}
	upstreamTimeout, ok := policyDuration(document.HTTP.UpstreamTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return nil, errPolicyRejected
	}
	streamReadTimeout, ok := policyDuration(document.HTTP.StreamReadTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return nil, errPolicyRejected
	}
	streamEventTimeout, ok := policyDuration(document.HTTP.StreamEventTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return nil, errPolicyRejected
	}
	httpConfig := httpapi.Config{
		DefaultQueueTimeout:    defaultQueueTimeout,
		BodyReadTimeout:        bodyReadTimeout,
		UpstreamTimeout:        upstreamTimeout,
		StreamReadTimeout:      streamReadTimeout,
		StreamEventTimeout:     streamEventTimeout,
		MaxResponseBodyBytes:   document.HTTP.MaxResponseBodyBytes,
		MaxStreamEventBytes:    document.HTTP.MaxStreamEventBytes,
		MaxStreamEvents:        document.HTTP.MaxStreamEvents,
		GlobalPreDispatchLimit: document.HTTP.GlobalPreDispatchCount,
		TenantPreDispatch:      tenantLimits,
	}
	preflightHTTPConfig := httpConfig
	preflightHTTPConfig.Credentials = make([]httpapi.Credential, 0, len(credentials))
	for index, credential := range credentials {
		preflightHTTPConfig.Credentials = append(preflightHTTPConfig.Credentials, httpapi.Credential{
			Tenant: credential.tenant,
			Token:  "policy-preflight-" + strconv.Itoa(index),
		})
	}
	if err := httpapi.ValidateConfig(preflightHTTPConfig, parser, admissionConfig); err != nil {
		return nil, errPolicyRejected
	}

	upstreamConfig, err := validateUpstreamPolicy(document.Upstream)
	if err != nil {
		return nil, errPolicyRejected
	}
	if !validEnvironmentName(document.Upstream.BearerTokenEnv) {
		return nil, errPolicyRejected
	}
	for _, credential := range credentials {
		if credential.env == document.Upstream.BearerTokenEnv {
			return nil, errPolicyRejected
		}
	}

	serverConfig, shutdownWait, err := validateServerPolicy(document.Server, httpapi.TimeoutPolicy{
		DefaultQueueTimeout: defaultQueueTimeout,
		BodyReadTimeout:     bodyReadTimeout,
		UpstreamTimeout:     upstreamTimeout,
	})
	if err != nil {
		return nil, errPolicyRejected
	}

	return &validatedPolicy{
		listener:         listener,
		parser:           parser,
		admission:        admissionConfig,
		http:             httpConfig,
		upstream:         upstreamConfig,
		server:           serverConfig,
		credentials:      credentials,
		upstreamTokenEnv: document.Upstream.BearerTokenEnv,
		shutdownWait:     shutdownWait,
	}, nil
}

func validateAdmissionPolicy(document policyDocument) (
	admission.Config,
	[]httpapi.TenantPreDispatchLimit,
	[]credentialReference,
	error,
) {
	if len(document.Admission.Tenants) == 0 || len(document.Admission.Tenants) > maximumPolicyTenants {
		return admission.Config{}, nil, nil, errPolicyRejected
	}

	tenants := make([]admission.TenantConfig, 0, len(document.Admission.Tenants))
	limits := make([]httpapi.TenantPreDispatchLimit, 0, len(document.Admission.Tenants))
	credentials := make([]credentialReference, 0, len(document.Admission.Tenants))
	environmentNames := make(map[string]struct{})
	for _, configured := range document.Admission.Tenants {
		if configured.ID == 0 || configured.ID > math.MaxUint32 || len(configured.BearerTokenEnvs) == 0 {
			return admission.Config{}, nil, nil, errPolicyRejected
		}
		id := admission.TenantID(configured.ID)
		tenants = append(tenants, admission.TenantConfig{
			ID:       id,
			Weight:   configured.Weight,
			Queue:    admission.QueueLimits(configured.Queue),
			Inflight: admission.InflightLimits(configured.Inflight),
		})
		limits = append(limits, httpapi.TenantPreDispatchLimit{
			Tenant: id,
			Count:  configured.PreDispatchCount,
		})
		for _, name := range configured.BearerTokenEnvs {
			if !validEnvironmentName(name) {
				return admission.Config{}, nil, nil, errPolicyRejected
			}
			if _, duplicate := environmentNames[name]; duplicate {
				return admission.Config{}, nil, nil, errPolicyRejected
			}
			environmentNames[name] = struct{}{}
			credentials = append(credentials, credentialReference{tenant: id, env: name})
			if len(credentials) > maximumPolicyCredentials {
				return admission.Config{}, nil, nil, errPolicyRejected
			}
		}
	}

	return admission.Config{
		MaxBodyBytes:    document.Contract.MaxBodyBytes,
		MaxRequestUnits: document.Contract.MaxRequestUnits,
		BaseQuantum:     document.Admission.BaseQuantum,
		DeficitCap:      document.Admission.DeficitCap,
		GlobalQueue:     admission.QueueLimits(document.Admission.GlobalQueue),
		GlobalInflight:  admission.InflightLimits(document.Admission.GlobalInflight),
		Tenants:         tenants,
	}, limits, credentials, nil
}

func validateUpstreamPolicy(configured upstreamPolicy) (httpapi.HTTPUpstreamConfig, error) {
	connectTimeout, ok := policyDuration(configured.ConnectTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return httpapi.HTTPUpstreamConfig{}, errPolicyRejected
	}
	tlsTimeout, ok := policyDuration(configured.TLSHandshakeTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return httpapi.HTTPUpstreamConfig{}, errPolicyRejected
	}
	headerTimeout, ok := policyDuration(configured.ResponseHeaderTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return httpapi.HTTPUpstreamConfig{}, errPolicyRejected
	}
	idleTimeout, ok := policyDuration(configured.IdleConnectionTimeoutMS, maximumPolicyTimeoutMS)
	if !ok || configured.MaxResponseHeaderBytes > math.MaxInt64 {
		return httpapi.HTTPUpstreamConfig{}, errPolicyRejected
	}

	config := httpapi.HTTPUpstreamConfig{
		Endpoint:               configured.Endpoint,
		ConnectTimeout:         connectTimeout,
		TLSHandshakeTimeout:    tlsTimeout,
		ResponseHeaderTimeout:  headerTimeout,
		IdleConnectionTimeout:  idleTimeout,
		MaxResponseHeaderBytes: int64(configured.MaxResponseHeaderBytes),
		MaxConnections:         configured.MaxConnections,
	}
	preflight, err := httpapi.NewHTTPUpstream(config, "policy-preflight")
	if err != nil {
		return httpapi.HTTPUpstreamConfig{}, errPolicyRejected
	}
	preflight.CloseIdleConnections()
	return config, nil
}

func validateServerPolicy(
	configured serverPolicy,
	timeouts httpapi.TimeoutPolicy,
) (server.Config, time.Duration, error) {
	headerTimeout, ok := policyDuration(configured.HeaderReadTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return server.Config{}, 0, errPolicyRejected
	}
	writeTimeout, ok := policyDuration(configured.ResponseWriteTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return server.Config{}, 0, errPolicyRejected
	}
	idleTimeout, ok := policyDuration(configured.IdleTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return server.Config{}, 0, errPolicyRejected
	}
	graceTimeout, ok := policyDuration(configured.GraceTimeoutMS, maximumPolicyTimeoutMS)
	if !ok {
		return server.Config{}, 0, errPolicyRejected
	}
	forceTimeout, ok := policyDuration(configured.ForceTimeoutMS, maximumForceTimeoutMS)
	if !ok {
		return server.Config{}, 0, errPolicyRejected
	}

	config := server.Config{
		HeaderReadTimeout:       headerTimeout,
		ResponseWriteTimeout:    writeTimeout,
		IdleTimeout:             idleTimeout,
		GraceTimeout:            graceTimeout,
		ForceTimeout:            forceTimeout,
		HeaderReadEnvelopeBytes: configured.HeaderReadEnvelopeBytes,
		MaxConnections:          configured.MaxConnections,
	}
	if err := server.ValidateConfig(config, timeouts); err != nil {
		return server.Config{}, 0, errPolicyRejected
	}
	shutdownWait := graceTimeout + 2*forceTimeout + shutdownSchedulingMargin
	if shutdownWait <= 0 || shutdownWait > maximumShutdownWait {
		return server.Config{}, 0, errPolicyRejected
	}
	return config, shutdownWait, nil
}

func policyDuration(milliseconds, maximum uint64) (time.Duration, bool) {
	if milliseconds == 0 || milliseconds > maximum {
		return 0, false
	}
	return time.Duration(milliseconds) * time.Millisecond, true
}

func validEnvironmentName(name string) bool {
	if len(name) == 0 || len(name) > maximumEnvironmentNameLen {
		return false
	}
	if !environmentNameStart(name[0]) {
		return false
	}
	for index := 1; index < len(name); index++ {
		if !environmentNameContinue(name[index]) {
			return false
		}
	}
	return true
}

func environmentNameStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z'
}

func environmentNameContinue(value byte) bool {
	return environmentNameStart(value) || value >= '0' && value <= '9'
}
