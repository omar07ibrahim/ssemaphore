package admission

import (
	"math"
	"strings"
	"testing"
)

func baseConfig() Config {
	return Config{
		MaxBodyBytes:    100,
		MaxRequestUnits: 100,
		BaseQuantum:     10,
		DeficitCap:      256,
		GlobalQueue:     QueueLimits{Count: 64, Bytes: 6_400, Work: 6_400},
		GlobalInflight:  InflightLimits{Count: 1, Work: 100},
		Tenants: []TenantConfig{
			{
				ID:       1,
				Weight:   1,
				Queue:    QueueLimits{Count: 32, Bytes: 3_200, Work: 3_200},
				Inflight: InflightLimits{Count: 1, Work: 100},
			},
			{
				ID:       2,
				Weight:   3,
				Queue:    QueueLimits{Count: 32, Bytes: 3_200, Work: 3_200},
				Inflight: InflightLimits{Count: 1, Work: 100},
			},
		},
	}
}

func TestValidateConfigRejectsUnsafeValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{name: "zero body", mutate: func(c *Config) { c.MaxBodyBytes = 0 }, match: "body bytes"},
		{name: "zero work", mutate: func(c *Config) { c.MaxRequestUnits = 0 }, match: "request units"},
		{name: "zero quantum", mutate: func(c *Config) { c.BaseQuantum = 0 }, match: "base quantum"},
		{name: "no tenants", mutate: func(c *Config) { c.Tenants = nil }, match: "tenant count"},
		{name: "global queue count", mutate: func(c *Config) { c.GlobalQueue.Count = 0 }, match: "global queue"},
		{name: "global queue bytes", mutate: func(c *Config) { c.GlobalQueue.Bytes = 99 }, match: "global queue"},
		{name: "global queue work", mutate: func(c *Config) { c.GlobalQueue.Work = 99 }, match: "global queue"},
		{name: "global inflight count", mutate: func(c *Config) { c.GlobalInflight.Count = 0 }, match: "global in-flight"},
		{name: "global inflight work", mutate: func(c *Config) { c.GlobalInflight.Work = 99 }, match: "global in-flight"},
		{name: "zero tenant ID", mutate: func(c *Config) { c.Tenants[0].ID = 0 }, match: "zero ID"},
		{name: "duplicate tenant", mutate: func(c *Config) { c.Tenants[1].ID = c.Tenants[0].ID }, match: "repeats"},
		{name: "zero weight", mutate: func(c *Config) { c.Tenants[0].Weight = 0 }, match: "zero weight"},
		{name: "quantum overflow", mutate: func(c *Config) { c.Tenants[0].Weight = math.MaxUint64 }, match: "quantum overflows"},
		{name: "tenant queue count", mutate: func(c *Config) { c.Tenants[0].Queue.Count = 0 }, match: "tenant 0 queue"},
		{name: "tenant queue above global", mutate: func(c *Config) { c.Tenants[0].Queue.Bytes = c.GlobalQueue.Bytes + 1 }, match: "exceed global"},
		{name: "tenant inflight work", mutate: func(c *Config) { c.Tenants[0].Inflight.Work = 99 }, match: "tenant 0 in-flight"},
		{name: "tenant inflight above global", mutate: func(c *Config) { c.Tenants[0].Inflight.Count = 2 }, match: "exceed global"},
		{name: "deficit cap", mutate: func(c *Config) { c.DeficitCap = 128 }, match: "deficit cap"},
		{name: "funding CPU bound", mutate: func(c *Config) {
			c.BaseQuantum = 1
			c.MaxRequestUnits = 65_536
			c.GlobalQueue.Work = 1_000_000
			c.GlobalInflight.Work = 65_536
			c.DeficitCap = 70_000
			for index := range c.Tenants {
				c.Tenants[index].Queue.Work = 1_000_000
				c.Tenants[index].Inflight.Work = 65_536
			}
		}, match: "CPU bound"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := baseConfig()
			test.mutate(&config)
			_, err := validateConfig(config)
			if err == nil {
				t.Fatal("validateConfig() unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %q, want substring %q", err, test.match)
			}
		})
	}
}

func TestValidateConfigCopiesTenantOrder(t *testing.T) {
	t.Parallel()
	config := baseConfig()
	validated, err := validateConfig(config)
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	config.Tenants[0].ID = 99
	config.Tenants[0].Weight = 99
	if validated.tenants[0].id != 1 || validated.tenants[0].quantum != 10 {
		t.Fatal("validated config retained mutable caller state")
	}
}

func TestValidateAdmission(t *testing.T) {
	t.Parallel()
	config, err := validateConfig(baseConfig())
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	tests := []struct {
		name      string
		admission Admission
		want      Decision
	}{
		{name: "valid", admission: Admission{Tenant: 1, BodyBytes: 1, WorkUnits: 1, QueueTimeout: 1}, want: Decision{}},
		{name: "zero tenant", admission: Admission{BodyBytes: 1, WorkUnits: 1, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid}},
		{name: "unknown tenant", admission: Admission{Tenant: 3, BodyBytes: 1, WorkUnits: 1, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid}},
		{name: "zero body", admission: Admission{Tenant: 1, WorkUnits: 1, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid, Resource: ResourceBytes}},
		{name: "body over limit", admission: Admission{Tenant: 1, BodyBytes: 101, WorkUnits: 1, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid, Resource: ResourceBytes}},
		{name: "zero work", admission: Admission{Tenant: 1, BodyBytes: 1, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid, Resource: ResourceWork}},
		{name: "work over limit", admission: Admission{Tenant: 1, BodyBytes: 1, WorkUnits: 101, QueueTimeout: 1}, want: Decision{Kind: DecisionInvalid, Resource: ResourceWork}},
		{name: "zero timeout", admission: Admission{Tenant: 1, BodyBytes: 1, WorkUnits: 1}, want: Decision{Kind: DecisionInvalid}},
		{name: "timeout over limit", admission: Admission{Tenant: 1, BodyBytes: 1, WorkUnits: 1, QueueTimeout: maxQueueTimeout + 1}, want: Decision{Kind: DecisionInvalid}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validateAdmission(config, test.admission); got != test.want {
				t.Fatalf("validateAdmission() = %+v, want %+v", got, test.want)
			}
		})
	}
}
