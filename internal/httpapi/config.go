package httpapi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	absoluteMaxCredentials        = 4096
	absoluteMaxCredentialBytes    = 4096
	absoluteMaxPreDispatchTenants = 1024
	absoluteMaxPreDispatchCount   = 4096
	absoluteMaxPolicyTimeout      = time.Hour
)

type storedCredential struct {
	tenant admission.TenantID
	digest [sha256.Size]byte
}

type validatedHandlerConfig struct {
	defaultQueueTimeout time.Duration
	bodyReadTimeout     time.Duration
	upstreamTimeout     time.Duration
	responseValidator   *contract.ResponseValidator
	globalSlots         chan struct{}
	tenantSlots         map[admission.TenantID]chan struct{}
	credentials         []storedCredential
}

func validateHandlerConfig(
	config Config,
	parser *contract.Parser,
	scheduler *admission.Scheduler,
) (validatedHandlerConfig, error) {
	if parser == nil {
		return validatedHandlerConfig{}, errors.New("request parser must not be nil")
	}
	if scheduler == nil {
		return validatedHandlerConfig{}, errors.New("admission scheduler must not be nil")
	}
	if parser.MaxBodyBytes() != scheduler.MaxBodyBytes() {
		return validatedHandlerConfig{}, errors.New("parser and scheduler body limits must match")
	}
	if parser.MaxRequestUnits() != scheduler.MaxRequestUnits() {
		return validatedHandlerConfig{}, errors.New("parser and scheduler work limits must match")
	}
	if err := validatePolicyTimeout("default queue", config.DefaultQueueTimeout, admission.MaximumQueueTimeout); err != nil {
		return validatedHandlerConfig{}, err
	}
	if err := validatePolicyTimeout("body read", config.BodyReadTimeout, absoluteMaxPolicyTimeout); err != nil {
		return validatedHandlerConfig{}, err
	}
	if err := validatePolicyTimeout("upstream", config.UpstreamTimeout, absoluteMaxPolicyTimeout); err != nil {
		return validatedHandlerConfig{}, err
	}

	responseValidator, err := contract.NewResponseValidator(contract.ResponseLimits{
		MaxBodyBytes: config.MaxResponseBodyBytes,
	})
	if err != nil {
		return validatedHandlerConfig{}, fmt.Errorf("response limits: %w", err)
	}
	if config.GlobalPreDispatchLimit == 0 || config.GlobalPreDispatchLimit > absoluteMaxPreDispatchCount {
		return validatedHandlerConfig{}, errors.New("global pre-dispatch count is outside the hard safety bounds")
	}
	if len(config.TenantPreDispatch) == 0 || len(config.TenantPreDispatch) > absoluteMaxPreDispatchTenants {
		return validatedHandlerConfig{}, errors.New("tenant pre-dispatch count is outside the hard safety bounds")
	}
	if len(config.Credentials) == 0 || len(config.Credentials) > absoluteMaxCredentials {
		return validatedHandlerConfig{}, errors.New("credential count is outside the hard safety bounds")
	}

	globalQueue := scheduler.GlobalQueueLimits()
	if err := validatePreDispatchEnvelope(
		"global",
		config.GlobalPreDispatchLimit,
		globalQueue,
		parser.MaxBodyBytes(),
		parser.MaxRequestUnits(),
	); err != nil {
		return validatedHandlerConfig{}, err
	}

	tenantSlots := make(map[admission.TenantID]chan struct{}, len(config.TenantPreDispatch))
	for index, limit := range config.TenantPreDispatch {
		if limit.Tenant == 0 {
			return validatedHandlerConfig{}, fmt.Errorf("tenant pre-dispatch limit %d has a zero tenant", index)
		}
		if limit.Count == 0 || limit.Count > config.GlobalPreDispatchLimit || limit.Count > absoluteMaxPreDispatchCount {
			return validatedHandlerConfig{}, fmt.Errorf("tenant pre-dispatch limit %d is outside its safety bounds", index)
		}
		if _, duplicate := tenantSlots[limit.Tenant]; duplicate {
			return validatedHandlerConfig{}, fmt.Errorf("tenant pre-dispatch limit %d repeats a tenant", index)
		}
		tenantQueue, exists := scheduler.TenantQueueLimits(limit.Tenant)
		if !exists {
			return validatedHandlerConfig{}, fmt.Errorf("tenant pre-dispatch limit %d names an unknown tenant", index)
		}
		if err := validatePreDispatchEnvelope(
			fmt.Sprintf("tenant pre-dispatch limit %d", index),
			limit.Count,
			tenantQueue,
			parser.MaxBodyBytes(),
			parser.MaxRequestUnits(),
		); err != nil {
			return validatedHandlerConfig{}, err
		}
		tenantSlots[limit.Tenant] = make(chan struct{}, int(limit.Count))
	}

	credentials := make([]storedCredential, 0, len(config.Credentials))
	credentialTenants := make(map[admission.TenantID]struct{}, len(config.TenantPreDispatch))
	digests := make(map[[sha256.Size]byte]struct{}, len(config.Credentials))
	for index, credential := range config.Credentials {
		if _, exists := tenantSlots[credential.Tenant]; !exists {
			return validatedHandlerConfig{}, fmt.Errorf("credential %d names a tenant without a pre-dispatch limit", index)
		}
		if !validBearerToken(credential.Token) {
			return validatedHandlerConfig{}, fmt.Errorf("credential %d is not a valid bounded bearer token", index)
		}
		digest := sha256.Sum256([]byte(credential.Token))
		if _, duplicate := digests[digest]; duplicate {
			return validatedHandlerConfig{}, fmt.Errorf("credential %d repeats a bearer token", index)
		}
		digests[digest] = struct{}{}
		credentialTenants[credential.Tenant] = struct{}{}
		credentials = append(credentials, storedCredential{tenant: credential.Tenant, digest: digest})
	}
	for index, limit := range config.TenantPreDispatch {
		if _, exists := credentialTenants[limit.Tenant]; !exists {
			return validatedHandlerConfig{}, fmt.Errorf("tenant pre-dispatch limit %d has no credential", index)
		}
	}

	return validatedHandlerConfig{
		defaultQueueTimeout: config.DefaultQueueTimeout,
		bodyReadTimeout:     config.BodyReadTimeout,
		upstreamTimeout:     config.UpstreamTimeout,
		responseValidator:   responseValidator,
		globalSlots:         make(chan struct{}, int(config.GlobalPreDispatchLimit)),
		tenantSlots:         tenantSlots,
		credentials:         credentials,
	}, nil
}

func validatePolicyTimeout(name string, value, maximum time.Duration) error {
	if value <= 0 || value > maximum {
		return fmt.Errorf("%s timeout is outside its safety bounds", name)
	}
	return nil
}

func validatePreDispatchEnvelope(
	scope string,
	count uint64,
	queue admission.QueueLimits,
	maxBodyBytes uint64,
	maxRequestUnits uint64,
) error {
	if count > queue.Count {
		return fmt.Errorf("%s pre-dispatch count exceeds its scheduler queue count", scope)
	}
	bodyBytes, bodyOverflow := multiply(count, maxBodyBytes)
	if bodyOverflow || bodyBytes > queue.Bytes {
		return fmt.Errorf("%s pre-dispatch bodies exceed its scheduler queue byte envelope", scope)
	}
	workUnits, workOverflow := multiply(count, maxRequestUnits)
	if workOverflow || workUnits > queue.Work {
		return fmt.Errorf("%s pre-dispatch work exceeds its scheduler queue work envelope", scope)
	}
	return nil
}

func multiply(left, right uint64) (uint64, bool) {
	hi, low := bits.Mul64(left, right)
	return low, hi != 0
}

func validBearerToken(token string) bool {
	if token == "" || len(token) > absoluteMaxCredentialBytes {
		return false
	}
	padding := false
	nonPadding := false
	for index := range len(token) {
		character := token[index]
		if character == '=' {
			padding = true
			continue
		}
		if padding || !bearerTokenCharacter(character) {
			return false
		}
		nonPadding = true
	}
	return nonPadding
}

func bearerTokenCharacter(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		character == '-' || character == '.' || character == '_' ||
		character == '~' || character == '+' || character == '/'
}
