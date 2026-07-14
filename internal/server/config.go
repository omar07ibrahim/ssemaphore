// Package server owns the bounded inbound HTTP listener and its coordinated
// admission shutdown lifecycle. It never creates or selects a listener.
package server

import (
	"errors"
	"math"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

const (
	netHTTPHeaderReadSlopBytes = 4096

	absoluteMinHeaderReadEnvelopeBytes = 8 << 10
	absoluteMaxHeaderReadEnvelopeBytes = 64 << 10
	absoluteMaxConnections             = 1024
	absoluteMaxHeaderReadReservation   = 64 << 20
	absoluteMaxServerTimeout           = time.Hour
	absoluteMaxForceTimeout            = time.Minute
)

// Config contains only finite server-owned resource and shutdown bounds.
// ReadTimeout and WriteTimeout are derived from these values and the validated
// Handler policy so inconsistent total deadlines cannot be configured.
type Config struct {
	HeaderReadTimeout    time.Duration
	ResponseWriteTimeout time.Duration
	IdleTimeout          time.Duration
	GraceTimeout         time.Duration
	ForceTimeout         time.Duration

	HeaderReadEnvelopeBytes uint64
	MaxConnections          uint64
}

type validatedConfig struct {
	headerReadTimeout     time.Duration
	readTimeout           time.Duration
	writeTimeout          time.Duration
	idleTimeout           time.Duration
	graceTimeout          time.Duration
	forceTimeout          time.Duration
	headerReadEnvelope    uint64
	netHTTPMaxHeaderBytes int
	maxConnections        int
}

func validateConfig(config Config, policy httpapi.TimeoutPolicy) (validatedConfig, error) {
	if err := validateTimeout("header read", config.HeaderReadTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("response write", config.ResponseWriteTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("idle", config.IdleTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("grace", config.GraceTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("force", config.ForceTimeout, absoluteMaxForceTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("handler default queue", policy.DefaultQueueTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("handler body read", policy.BodyReadTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if err := validateTimeout("handler upstream", policy.UpstreamTimeout, absoluteMaxServerTimeout); err != nil {
		return validatedConfig{}, err
	}
	if config.HeaderReadEnvelopeBytes < absoluteMinHeaderReadEnvelopeBytes ||
		config.HeaderReadEnvelopeBytes > absoluteMaxHeaderReadEnvelopeBytes {
		return validatedConfig{}, errors.New("header read envelope is outside its safety bounds")
	}
	if config.MaxConnections == 0 || config.MaxConnections > absoluteMaxConnections {
		return validatedConfig{}, errors.New("connection count is outside its safety bounds")
	}
	if config.HeaderReadEnvelopeBytes > absoluteMaxHeaderReadReservation/config.MaxConnections {
		return validatedConfig{}, errors.New("connection and header envelope exceed their combined safety bound")
	}

	readTimeout, ok := addDurations(config.HeaderReadTimeout, policy.BodyReadTimeout)
	if !ok {
		return validatedConfig{}, errors.New("total request read timeout overflows")
	}
	writeTimeout, ok := addDurations(
		policy.BodyReadTimeout,
		policy.DefaultQueueTimeout,
		policy.UpstreamTimeout,
		config.ResponseWriteTimeout,
	)
	if !ok {
		return validatedConfig{}, errors.New("total response write timeout overflows")
	}

	// Go 1.26 reads Server.MaxHeaderBytes plus 4 KiB of parser slop. Store
	// the adjusted field so the public value remains the hard wire envelope.
	adjustedHeaderBytes := config.HeaderReadEnvelopeBytes - netHTTPHeaderReadSlopBytes
	if adjustedHeaderBytes > uint64(math.MaxInt) {
		return validatedConfig{}, errors.New("adjusted header limit does not fit this architecture")
	}

	return validatedConfig{
		headerReadTimeout:     config.HeaderReadTimeout,
		readTimeout:           readTimeout,
		writeTimeout:          writeTimeout,
		idleTimeout:           config.IdleTimeout,
		graceTimeout:          config.GraceTimeout,
		forceTimeout:          config.ForceTimeout,
		headerReadEnvelope:    config.HeaderReadEnvelopeBytes,
		netHTTPMaxHeaderBytes: int(adjustedHeaderBytes),
		maxConnections:        int(config.MaxConnections),
	}, nil
}

func validateTimeout(name string, value, maximum time.Duration) error {
	if value <= 0 || value > maximum {
		return errors.New(name + " timeout is outside its safety bounds")
	}
	return nil
}

func addDurations(values ...time.Duration) (time.Duration, bool) {
	var total time.Duration
	for _, value := range values {
		if value <= 0 || value > time.Duration(math.MaxInt64)-total {
			return 0, false
		}
		total += value
	}
	return total, true
}
