package server

import (
	"math"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

func TestValidateConfigAcceptsTimeoutBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config, *httpapi.TimeoutPolicy)
	}{
		{
			name: "header read minimum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.HeaderReadTimeout = time.Nanosecond
			},
		},
		{
			name: "header read maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.HeaderReadTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "response write minimum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ResponseWriteTimeout = time.Nanosecond
			},
		},
		{
			name: "response write maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ResponseWriteTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "idle minimum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.IdleTimeout = time.Nanosecond
			},
		},
		{
			name: "idle maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.IdleTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "grace minimum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.GraceTimeout = time.Nanosecond
			},
		},
		{
			name: "grace maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.GraceTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "force minimum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ForceTimeout = time.Nanosecond
			},
		},
		{
			name: "force one minute maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ForceTimeout = time.Minute
			},
		},
		{
			name: "handler default queue minimum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.DefaultQueueTimeout = time.Nanosecond
			},
		},
		{
			name: "handler default queue maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.DefaultQueueTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "handler body read minimum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.BodyReadTimeout = time.Nanosecond
			},
		},
		{
			name: "handler body read maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.BodyReadTimeout = absoluteMaxServerTimeout
			},
		},
		{
			name: "handler upstream minimum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.UpstreamTimeout = time.Nanosecond
			},
		},
		{
			name: "handler upstream maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.UpstreamTimeout = absoluteMaxServerTimeout
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, policy := validConfigAndPolicy()
			test.mutate(&config, &policy)

			if _, err := validateConfig(config, policy); err != nil {
				t.Fatalf("validateConfig() error = %v", err)
			}
		})
	}
}

func TestValidateConfigRejectsTimeoutsOutsideBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config, *httpapi.TimeoutPolicy)
	}{
		{
			name: "header read zero",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.HeaderReadTimeout = 0
			},
		},
		{
			name: "header read negative",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.HeaderReadTimeout = -time.Nanosecond
			},
		},
		{
			name: "header read above maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.HeaderReadTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "response write zero",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ResponseWriteTimeout = 0
			},
		},
		{
			name: "response write negative",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ResponseWriteTimeout = -time.Nanosecond
			},
		},
		{
			name: "response write above maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ResponseWriteTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "idle zero",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.IdleTimeout = 0
			},
		},
		{
			name: "idle negative",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.IdleTimeout = -time.Nanosecond
			},
		},
		{
			name: "idle above maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.IdleTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "grace zero",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.GraceTimeout = 0
			},
		},
		{
			name: "grace negative",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.GraceTimeout = -time.Nanosecond
			},
		},
		{
			name: "grace above maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.GraceTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "force zero",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ForceTimeout = 0
			},
		},
		{
			name: "force negative",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ForceTimeout = -time.Nanosecond
			},
		},
		{
			name: "force above one minute maximum",
			mutate: func(config *Config, _ *httpapi.TimeoutPolicy) {
				config.ForceTimeout = time.Minute + time.Nanosecond
			},
		},
		{
			name: "handler default queue zero",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.DefaultQueueTimeout = 0
			},
		},
		{
			name: "handler default queue negative",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.DefaultQueueTimeout = -time.Nanosecond
			},
		},
		{
			name: "handler default queue above maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.DefaultQueueTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "handler body read zero",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.BodyReadTimeout = 0
			},
		},
		{
			name: "handler body read negative",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.BodyReadTimeout = -time.Nanosecond
			},
		},
		{
			name: "handler body read above maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.BodyReadTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
		{
			name: "handler upstream zero",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.UpstreamTimeout = 0
			},
		},
		{
			name: "handler upstream negative",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.UpstreamTimeout = -time.Nanosecond
			},
		},
		{
			name: "handler upstream above maximum",
			mutate: func(_ *Config, policy *httpapi.TimeoutPolicy) {
				policy.UpstreamTimeout = absoluteMaxServerTimeout + time.Nanosecond
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, policy := validConfigAndPolicy()
			test.mutate(&config, &policy)

			if _, err := validateConfig(config, policy); err == nil {
				t.Fatal("validateConfig() error = nil, want rejection")
			}
		})
	}
}

func TestValidateConfigHeaderEnvelopeBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		envelope uint64
		wantErr  bool
	}{
		{name: "below minimum", envelope: absoluteMinHeaderReadEnvelopeBytes - 1, wantErr: true},
		{name: "minimum", envelope: absoluteMinHeaderReadEnvelopeBytes},
		{name: "maximum", envelope: absoluteMaxHeaderReadEnvelopeBytes},
		{name: "above maximum", envelope: absoluteMaxHeaderReadEnvelopeBytes + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, policy := validConfigAndPolicy()
			config.HeaderReadEnvelopeBytes = test.envelope

			validated, err := validateConfig(config, policy)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateConfig() error = %v, wantErr %v", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if validated.headerReadEnvelope != test.envelope {
				t.Fatalf("headerReadEnvelope = %d, want %d", validated.headerReadEnvelope, test.envelope)
			}
			wantAdjusted := int(test.envelope - netHTTPHeaderReadSlopBytes)
			if validated.netHTTPMaxHeaderBytes != wantAdjusted {
				t.Fatalf("netHTTPMaxHeaderBytes = %d, want %d", validated.netHTTPMaxHeaderBytes, wantAdjusted)
			}
		})
	}
}

func TestValidateConfigConnectionBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		connections uint64
		wantErr     bool
	}{
		{name: "zero", connections: 0, wantErr: true},
		{name: "minimum", connections: 1},
		{name: "maximum", connections: absoluteMaxConnections},
		{name: "above maximum", connections: absoluteMaxConnections + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, policy := validConfigAndPolicy()
			config.MaxConnections = test.connections

			validated, err := validateConfig(config, policy)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateConfig() error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && validated.maxConnections != int(test.connections) {
				t.Fatalf("maxConnections = %d, want %d", validated.maxConnections, test.connections)
			}
		})
	}
}

func TestValidateConfigAcceptsExactCombinedHeaderReservation(t *testing.T) {
	config, policy := validConfigAndPolicy()
	config.HeaderReadEnvelopeBytes = absoluteMaxHeaderReadEnvelopeBytes
	config.MaxConnections = absoluteMaxConnections

	validated, err := validateConfig(config, policy)
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	reservation := validated.headerReadEnvelope * uint64(validated.maxConnections)
	if reservation != absoluteMaxHeaderReadReservation {
		t.Fatalf("combined header reservation = %d, want %d", reservation, uint64(absoluteMaxHeaderReadReservation))
	}
	if reservation != 64<<20 {
		t.Fatalf("combined header reservation = %d, want 64 MiB", reservation)
	}

	config.MaxConnections++
	if _, err := validateConfig(config, policy); err == nil {
		t.Fatal("validateConfig() error = nil above the 64 MiB combined header reservation")
	}
}

func TestValidateConfigComputesServerDeadlines(t *testing.T) {
	config := Config{
		HeaderReadTimeout:       2 * time.Second,
		ResponseWriteTimeout:    5 * time.Second,
		IdleTimeout:             13 * time.Second,
		GraceTimeout:            17 * time.Second,
		ForceTimeout:            19 * time.Second,
		HeaderReadEnvelopeBytes: 32 << 10,
		MaxConnections:          23,
	}
	policy := httpapi.TimeoutPolicy{
		DefaultQueueTimeout: 7 * time.Second,
		BodyReadTimeout:     3 * time.Second,
		UpstreamTimeout:     11 * time.Second,
	}

	validated, err := validateConfig(config, policy)
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}

	if validated.headerReadTimeout != 2*time.Second {
		t.Fatalf("headerReadTimeout = %s, want 2s", validated.headerReadTimeout)
	}
	if validated.readTimeout != 5*time.Second {
		t.Fatalf("readTimeout = %s, want 5s", validated.readTimeout)
	}
	if validated.writeTimeout != 26*time.Second {
		t.Fatalf("writeTimeout = %s, want 26s", validated.writeTimeout)
	}
	if validated.idleTimeout != 13*time.Second {
		t.Fatalf("idleTimeout = %s, want 13s", validated.idleTimeout)
	}
	if validated.graceTimeout != 17*time.Second {
		t.Fatalf("graceTimeout = %s, want 17s", validated.graceTimeout)
	}
	if validated.forceTimeout != 19*time.Second {
		t.Fatalf("forceTimeout = %s, want 19s", validated.forceTimeout)
	}
	if validated.netHTTPMaxHeaderBytes != 28<<10 {
		t.Fatalf("netHTTPMaxHeaderBytes = %d, want %d", validated.netHTTPMaxHeaderBytes, 28<<10)
	}
	if validated.maxConnections != 23 {
		t.Fatalf("maxConnections = %d, want 23", validated.maxConnections)
	}
}

func TestValidateConfigConversionsFit386(t *testing.T) {
	if absoluteMaxHeaderReadEnvelopeBytes-netHTTPHeaderReadSlopBytes > uint64(math.MaxInt32) {
		t.Fatal("maximum adjusted header field does not fit a 386 int")
	}
	if absoluteMaxConnections > uint64(math.MaxInt32) {
		t.Fatal("maximum connection count does not fit a 386 int")
	}

	config, policy := validConfigAndPolicy()
	config.HeaderReadEnvelopeBytes = absoluteMaxHeaderReadEnvelopeBytes
	config.MaxConnections = absoluteMaxConnections

	validated, err := validateConfig(config, policy)
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	if uint64(validated.netHTTPMaxHeaderBytes) != absoluteMaxHeaderReadEnvelopeBytes-netHTTPHeaderReadSlopBytes {
		t.Fatalf("netHTTPMaxHeaderBytes = %d, conversion changed the limit", validated.netHTTPMaxHeaderBytes)
	}
	if uint64(validated.maxConnections) != absoluteMaxConnections {
		t.Fatalf("maxConnections = %d, conversion changed the limit", validated.maxConnections)
	}
}

func TestAddDurations(t *testing.T) {
	tests := []struct {
		name      string
		values    []time.Duration
		want      time.Duration
		wantValid bool
	}{
		{name: "empty", wantValid: true},
		{name: "one nanosecond", values: []time.Duration{time.Nanosecond}, want: time.Nanosecond, wantValid: true},
		{name: "ordinary sum", values: []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}, want: 6 * time.Second, wantValid: true},
		{name: "exact maximum", values: []time.Duration{time.Duration(math.MaxInt64)}, want: time.Duration(math.MaxInt64), wantValid: true},
		{name: "sum reaches exact maximum", values: []time.Duration{time.Duration(math.MaxInt64 - 1), time.Nanosecond}, want: time.Duration(math.MaxInt64), wantValid: true},
		{name: "zero value", values: []time.Duration{time.Second, 0}},
		{name: "negative value", values: []time.Duration{time.Second, -time.Nanosecond}},
		{name: "sum overflows", values: []time.Duration{time.Duration(math.MaxInt64), time.Nanosecond}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, valid := addDurations(test.values...)
			if valid != test.wantValid {
				t.Fatalf("addDurations() valid = %v, want %v", valid, test.wantValid)
			}
			if got != test.want {
				t.Fatalf("addDurations() = %s, want %s", got, test.want)
			}
		})
	}
}

func validConfigAndPolicy() (Config, httpapi.TimeoutPolicy) {
	return Config{
			HeaderReadTimeout:       time.Second,
			ResponseWriteTimeout:    time.Second,
			IdleTimeout:             time.Second,
			GraceTimeout:            time.Second,
			ForceTimeout:            time.Second,
			HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
			MaxConnections:          1,
		}, httpapi.TimeoutPolicy{
			DefaultQueueTimeout: time.Second,
			BodyReadTimeout:     time.Second,
			UpstreamTimeout:     time.Second,
		}
}
