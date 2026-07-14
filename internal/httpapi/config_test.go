package httpapi

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	configTestMaxBodyBytes    = uint64(256)
	configTestMaxRequestUnits = uint64(512)
	configTestTenantOne       = admission.TenantID(1)
	configTestTenantTwo       = admission.TenantID(2)
)

type configTestUpstream struct{}

func (configTestUpstream) Complete(context.Context, contract.Request) (UpstreamResponse, error) {
	panic("config tests must not call the upstream")
}

func TestNewHandlerRejectsNilAndMismatchedIntegrationLimits(t *testing.T) {
	t.Run("nil parser", func(t *testing.T) {
		scheduler := configTestNewScheduler(t, nil)
		if _, err := NewHandler(configTestBaseHandlerConfig(), nil, scheduler, configTestUpstream{}); err == nil || !strings.Contains(err.Error(), "parser") {
			t.Fatalf("NewHandler() error = %v, want parser error", err)
		}
	})

	t.Run("nil scheduler", func(t *testing.T) {
		parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
		if _, err := NewHandler(configTestBaseHandlerConfig(), parser, nil, configTestUpstream{}); err == nil || !strings.Contains(err.Error(), "scheduler") {
			t.Fatalf("NewHandler() error = %v, want scheduler error", err)
		}
	})

	t.Run("body limit mismatch", func(t *testing.T) {
		parser := configTestNewParser(t, configTestMaxBodyBytes+1, configTestMaxRequestUnits)
		scheduler := configTestNewScheduler(t, nil)
		if _, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, configTestUpstream{}); err == nil || !strings.Contains(err.Error(), "body limits must match") {
			t.Fatalf("NewHandler() error = %v, want body-limit mismatch", err)
		}
	})

	t.Run("work limit mismatch", func(t *testing.T) {
		parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits+1)
		scheduler := configTestNewScheduler(t, nil)
		if _, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, configTestUpstream{}); err == nil || !strings.Contains(err.Error(), "work limits must match") {
			t.Fatalf("NewHandler() error = %v, want work-limit mismatch", err)
		}
	})

	t.Run("nil upstream", func(t *testing.T) {
		parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
		scheduler := configTestNewScheduler(t, nil)
		if _, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, nil); err == nil || !strings.Contains(err.Error(), "upstream") {
			t.Fatalf("NewHandler() error = %v, want upstream error", err)
		}
	})
}

func TestValidateHandlerConfigRejectsTimeoutAndResponseBounds(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)

	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{name: "zero default queue timeout", mutate: func(c *Config) { c.DefaultQueueTimeout = 0 }, match: "default queue timeout"},
		{name: "negative default queue timeout", mutate: func(c *Config) { c.DefaultQueueTimeout = -time.Nanosecond }, match: "default queue timeout"},
		{name: "default queue timeout above maximum", mutate: func(c *Config) { c.DefaultQueueTimeout = admission.MaximumQueueTimeout + time.Nanosecond }, match: "default queue timeout"},
		{name: "zero body read timeout", mutate: func(c *Config) { c.BodyReadTimeout = 0 }, match: "body read timeout"},
		{name: "negative body read timeout", mutate: func(c *Config) { c.BodyReadTimeout = -time.Nanosecond }, match: "body read timeout"},
		{name: "body read timeout above maximum", mutate: func(c *Config) { c.BodyReadTimeout = absoluteMaxPolicyTimeout + time.Nanosecond }, match: "body read timeout"},
		{name: "zero upstream timeout", mutate: func(c *Config) { c.UpstreamTimeout = 0 }, match: "upstream timeout"},
		{name: "negative upstream timeout", mutate: func(c *Config) { c.UpstreamTimeout = -time.Nanosecond }, match: "upstream timeout"},
		{name: "upstream timeout above maximum", mutate: func(c *Config) { c.UpstreamTimeout = absoluteMaxPolicyTimeout + time.Nanosecond }, match: "upstream timeout"},
		{name: "zero response bytes", mutate: func(c *Config) { c.MaxResponseBodyBytes = 0 }, match: "response limits"},
		{name: "response bytes above maximum", mutate: func(c *Config) { c.MaxResponseBodyBytes = contract.AbsoluteMaxResponseBodyBytes + 1 }, match: "response limits"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := configTestBaseHandlerConfig()
			test.mutate(&config)
			if _, err := validateHandlerConfig(config, parser, scheduler); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateHandlerConfig() error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestValidateHandlerConfigAcceptsExactPolicyAndResponseMaximums(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	config := configTestBaseHandlerConfig()
	config.DefaultQueueTimeout = admission.MaximumQueueTimeout
	config.BodyReadTimeout = absoluteMaxPolicyTimeout
	config.UpstreamTimeout = absoluteMaxPolicyTimeout
	config.MaxResponseBodyBytes = contract.AbsoluteMaxResponseBodyBytes

	validated, err := validateHandlerConfig(config, parser, scheduler)
	if err != nil {
		t.Fatalf("validateHandlerConfig() error = %v", err)
	}
	if validated.defaultQueueTimeout != admission.MaximumQueueTimeout ||
		validated.bodyReadTimeout != absoluteMaxPolicyTimeout ||
		validated.upstreamTimeout != absoluteMaxPolicyTimeout {
		t.Fatalf("validated timeouts = (%s, %s, %s), want exact maxima", validated.defaultQueueTimeout, validated.bodyReadTimeout, validated.upstreamTimeout)
	}
}

func TestValidateHandlerConfigRejectsGlobalPreDispatchBounds(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)

	tests := []struct {
		name            string
		mutateConfig    func(*Config)
		mutateScheduler func(*admission.Config)
		match           string
	}{
		{name: "zero count", mutateConfig: func(c *Config) { c.GlobalPreDispatchLimit = 0 }, match: "global pre-dispatch count"},
		{name: "count above hard maximum", mutateConfig: func(c *Config) { c.GlobalPreDispatchLimit = absoluteMaxPreDispatchCount + 1 }, match: "global pre-dispatch count"},
		{name: "count above queue envelope", mutateConfig: func(c *Config) { c.GlobalPreDispatchLimit = 5 }, match: "global pre-dispatch count exceeds"},
		{
			name: "bytes above queue envelope",
			mutateScheduler: func(c *admission.Config) {
				c.GlobalQueue.Bytes = 2*configTestMaxBodyBytes - 1
				for index := range c.Tenants {
					c.Tenants[index].Queue.Bytes = configTestMaxBodyBytes
				}
			},
			match: "global pre-dispatch bodies exceed",
		},
		{
			name: "work above queue envelope",
			mutateScheduler: func(c *admission.Config) {
				c.GlobalQueue.Work = 2*configTestMaxRequestUnits - 1
				for index := range c.Tenants {
					c.Tenants[index].Queue.Work = configTestMaxRequestUnits
				}
			},
			match: "global pre-dispatch work exceeds",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := configTestBaseHandlerConfig()
			if test.mutateConfig != nil {
				test.mutateConfig(&config)
			}
			scheduler := configTestNewScheduler(t, test.mutateScheduler)
			if _, err := validateHandlerConfig(config, parser, scheduler); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateHandlerConfig() error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestValidateHandlerConfigRejectsTenantPreDispatchBounds(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)

	tests := []struct {
		name            string
		mutateConfig    func(*Config)
		mutateScheduler func(*admission.Config)
		match           string
	}{
		{name: "no tenant limits", mutateConfig: func(c *Config) { c.TenantPreDispatch = nil }, match: "tenant pre-dispatch count"},
		{name: "too many tenant limits", mutateConfig: func(c *Config) { c.TenantPreDispatch = make([]TenantPreDispatchLimit, absoluteMaxPreDispatchTenants+1) }, match: "tenant pre-dispatch count"},
		{name: "zero tenant", mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Tenant = 0 }, match: "zero tenant"},
		{name: "unknown tenant", mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Tenant = 99 }, match: "unknown tenant"},
		{name: "duplicate tenant", mutateConfig: func(c *Config) { c.TenantPreDispatch[1].Tenant = configTestTenantOne }, match: "repeats a tenant"},
		{name: "zero count", mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Count = 0 }, match: "outside its safety bounds"},
		{name: "count above global", mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Count = c.GlobalPreDispatchLimit + 1 }, match: "outside its safety bounds"},
		{
			name:         "count above tenant queue envelope",
			mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Count = 2 },
			mutateScheduler: func(c *admission.Config) {
				c.Tenants[0].Queue.Count = 1
			},
			match: "scheduler queue count",
		},
		{
			name:         "bytes above tenant queue envelope",
			mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Count = 2 },
			mutateScheduler: func(c *admission.Config) {
				c.Tenants[0].Queue.Bytes = 2*configTestMaxBodyBytes - 1
			},
			match: "scheduler queue byte envelope",
		},
		{
			name:         "work above tenant queue envelope",
			mutateConfig: func(c *Config) { c.TenantPreDispatch[0].Count = 2 },
			mutateScheduler: func(c *admission.Config) {
				c.Tenants[0].Queue.Work = 2*configTestMaxRequestUnits - 1
			},
			match: "scheduler queue work envelope",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := configTestBaseHandlerConfig()
			if test.mutateConfig != nil {
				test.mutateConfig(&config)
			}
			scheduler := configTestNewScheduler(t, test.mutateScheduler)
			if _, err := validateHandlerConfig(config, parser, scheduler); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateHandlerConfig() error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestValidateHandlerConfigRejectsUnsafeCredentials(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)

	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{name: "no credentials", mutate: func(c *Config) { c.Credentials = nil }, match: "credential count"},
		{name: "too many credentials", mutate: func(c *Config) { c.Credentials = make([]Credential, absoluteMaxCredentials+1) }, match: "credential count"},
		{name: "credential tenant without pre-dispatch limit", mutate: func(c *Config) { c.Credentials[0].Tenant = 99 }, match: "without a pre-dispatch limit"},
		{name: "pre-dispatch tenant without credential", mutate: func(c *Config) { c.Credentials = c.Credentials[:1] }, match: "has no credential"},
		{name: "duplicate token for same tenant", mutate: func(c *Config) { c.Credentials = append(c.Credentials, c.Credentials[0]) }, match: "repeats a bearer token"},
		{name: "duplicate token across tenants", mutate: func(c *Config) { c.Credentials[1].Token = c.Credentials[0].Token }, match: "repeats a bearer token"},
		{name: "empty token", mutate: func(c *Config) { c.Credentials[0].Token = "" }, match: "valid bounded bearer token"},
		{name: "padding before token data", mutate: func(c *Config) { c.Credentials[0].Token = "a=b" }, match: "valid bounded bearer token"},
		{name: "padding only", mutate: func(c *Config) { c.Credentials[0].Token = "==" }, match: "valid bounded bearer token"},
		{name: "space in token", mutate: func(c *Config) { c.Credentials[0].Token = "token one" }, match: "valid bounded bearer token"},
		{name: "non-ASCII token", mutate: func(c *Config) { c.Credentials[0].Token = "tøken" }, match: "valid bounded bearer token"},
		{name: "token above byte maximum", mutate: func(c *Config) { c.Credentials[0].Token = strings.Repeat("a", absoluteMaxCredentialBytes+1) }, match: "valid bounded bearer token"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := configTestBaseHandlerConfig()
			test.mutate(&config)
			if _, err := validateHandlerConfig(config, parser, scheduler); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateHandlerConfig() error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestNewHandlerCopiesCredentialAndPreDispatchConfiguration(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	config := configTestBaseHandlerConfig()
	config.Credentials = append(config.Credentials, Credential{
		Tenant: configTestTenantOne,
		Token:  "tenant-one-rotated==",
	})

	handler, err := NewHandler(config, parser, scheduler, configTestUpstream{})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	config.Credentials[0] = Credential{Tenant: configTestTenantTwo, Token: "mutated-token"}
	config.Credentials[2] = Credential{Tenant: configTestTenantTwo, Token: "also-mutated"}
	config.TenantPreDispatch[0] = TenantPreDispatchLimit{Tenant: 99, Count: 99}

	authenticationTests := []struct {
		token      string
		wantTenant admission.TenantID
		wantOK     bool
	}{
		{token: "tenant-one-primary", wantTenant: configTestTenantOne, wantOK: true},
		{token: "tenant-one-rotated==", wantTenant: configTestTenantOne, wantOK: true},
		{token: "tenant-two-primary", wantTenant: configTestTenantTwo, wantOK: true},
		{token: "mutated-token", wantOK: false},
		{token: "also-mutated", wantOK: false},
	}
	for _, test := range authenticationTests {
		gotTenant, gotOK := handler.authenticate([]string{"Bearer " + test.token})
		if gotTenant != test.wantTenant || gotOK != test.wantOK {
			t.Errorf("authenticate(%q) = (%d, %t), want (%d, %t)", test.token, gotTenant, gotOK, test.wantTenant, test.wantOK)
		}
	}

	if got := cap(handler.globalSlots); got != 2 {
		t.Fatalf("global slot capacity = %d, want 2", got)
	}
	if got := cap(handler.tenantSlots[configTestTenantOne]); got != 1 {
		t.Fatalf("tenant-one slot capacity = %d, want 1", got)
	}
	if _, exists := handler.tenantSlots[99]; exists {
		t.Fatal("handler retained a caller mutation to tenant pre-dispatch limits")
	}
	if len(handler.credentials) != 3 {
		t.Fatalf("stored credential count = %d, want 3", len(handler.credentials))
	}
}

func TestMultiplyReportsPortableUint64Overflow(t *testing.T) {
	tests := []struct {
		name         string
		left         uint64
		right        uint64
		want         uint64
		wantOverflow bool
	}{
		{name: "zero", left: 0, right: math.MaxUint64, want: 0},
		{name: "exact product", left: 4096, right: contract.AbsoluteMaxBodyBytes, want: 4096 * contract.AbsoluteMaxBodyBytes},
		{name: "largest exact product", left: math.MaxUint64, right: 1, want: math.MaxUint64},
		{name: "overflow", left: math.MaxUint64, right: 2, want: math.MaxUint64 - 1, wantOverflow: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, overflow := multiply(test.left, test.right)
			if got != test.want || overflow != test.wantOverflow {
				t.Fatalf("multiply(%d, %d) = (%d, %t), want (%d, %t)", test.left, test.right, got, overflow, test.want, test.wantOverflow)
			}
		})
	}
}

func configTestBaseHandlerConfig() Config {
	return Config{
		DefaultQueueTimeout:    5 * time.Second,
		BodyReadTimeout:        5 * time.Second,
		UpstreamTimeout:        5 * time.Second,
		MaxResponseBodyBytes:   512,
		GlobalPreDispatchLimit: 2,
		TenantPreDispatch: []TenantPreDispatchLimit{
			{Tenant: configTestTenantOne, Count: 1},
			{Tenant: configTestTenantTwo, Count: 1},
		},
		Credentials: []Credential{
			{Tenant: configTestTenantOne, Token: "tenant-one-primary"},
			{Tenant: configTestTenantTwo, Token: "tenant-two-primary"},
		},
	}
}

func configTestNewParser(t *testing.T, maxBodyBytes, maxRequestUnits uint64) *contract.Parser {
	t.Helper()
	parser, err := contract.NewParser("portfolio-model", contract.Limits{
		MaxBodyBytes:        maxBodyBytes,
		MaxMessageCount:     4,
		MaxMessageTextBytes: 128,
		MaxCompletionTokens: 64,
		CompletionWeight:    1,
		MaxRequestUnits:     maxRequestUnits,
	})
	if err != nil {
		t.Fatalf("contract.NewParser() error = %v", err)
	}
	return parser
}

func configTestNewScheduler(t *testing.T, mutate func(*admission.Config)) *admission.Scheduler {
	t.Helper()
	config := admission.Config{
		MaxBodyBytes:    configTestMaxBodyBytes,
		MaxRequestUnits: configTestMaxRequestUnits,
		BaseQuantum:     configTestMaxRequestUnits,
		DeficitCap:      2 * configTestMaxRequestUnits,
		GlobalQueue: admission.QueueLimits{
			Count: 4,
			Bytes: 4 * configTestMaxBodyBytes,
			Work:  4 * configTestMaxRequestUnits,
		},
		GlobalInflight: admission.InflightLimits{
			Count: 2,
			Work:  2 * configTestMaxRequestUnits,
		},
		Tenants: []admission.TenantConfig{
			{
				ID:     configTestTenantOne,
				Weight: 1,
				Queue: admission.QueueLimits{
					Count: 2,
					Bytes: 2 * configTestMaxBodyBytes,
					Work:  2 * configTestMaxRequestUnits,
				},
				Inflight: admission.InflightLimits{Count: 1, Work: configTestMaxRequestUnits},
			},
			{
				ID:     configTestTenantTwo,
				Weight: 1,
				Queue: admission.QueueLimits{
					Count: 2,
					Bytes: 2 * configTestMaxBodyBytes,
					Work:  2 * configTestMaxRequestUnits,
				},
				Inflight: admission.InflightLimits{Count: 1, Work: configTestMaxRequestUnits},
			},
		},
	}
	if mutate != nil {
		mutate(&config)
	}
	scheduler, err := admission.New(config)
	if err != nil {
		t.Fatalf("admission.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := scheduler.Close(ctx); err != nil {
			t.Errorf("Scheduler.Close() error = %v", err)
		}
	})
	return scheduler
}
