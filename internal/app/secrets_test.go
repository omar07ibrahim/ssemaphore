package app

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

type fakeSecretSource struct {
	values    map[string]string
	present   map[string]bool
	unsetErrs map[string]error
	events    []string
}

func (source *fakeSecretSource) LookupEnv(name string) (string, bool) {
	source.events = append(source.events, "lookup:"+name)
	value, exists := source.values[name]
	if source.present != nil {
		exists = source.present[name]
	}
	return value, exists
}

func (source *fakeSecretSource) Unsetenv(name string) error {
	source.events = append(source.events, "unset:"+name)
	return source.unsetErrs[name]
}

func secretTestPolicy() *validatedPolicy {
	return &validatedPolicy{
		credentials: []credentialReference{
			{tenant: admission.TenantID(11), env: "TENANT_ELEVEN"},
			{tenant: admission.TenantID(29), env: "TENANT_TWENTY_NINE"},
		},
		upstreamTokenEnv: "UPSTREAM_TOKEN",
	}
}

func TestResolveSecretsConsumesInPolicyOrderAndPreservesOpaqueValues(t *testing.T) {
	const opaque = " spaces-and-control\t\x01 "
	source := &fakeSecretSource{values: map[string]string{
		"TENANT_ELEVEN":      opaque,
		"TENANT_TWENTY_NINE": "second-token",
		"UPSTREAM_TOKEN":     "upstream-token",
	}}

	resolved, err := resolveSecrets(secretTestPolicy(), source)
	if err != nil {
		t.Fatalf("resolveSecrets() error = %v", err)
	}
	wantCredentials := []httpapi.Credential{
		{Tenant: admission.TenantID(11), Token: opaque},
		{Tenant: admission.TenantID(29), Token: "second-token"},
	}
	if fmt.Sprint(resolved.credentials) != fmt.Sprint(wantCredentials) {
		t.Fatalf("credentials = %#v, want %#v", resolved.credentials, wantCredentials)
	}
	if resolved.upstream != "upstream-token" {
		t.Fatalf("upstream token was not preserved")
	}
	wantEvents := []string{
		"lookup:TENANT_ELEVEN", "unset:TENANT_ELEVEN",
		"lookup:TENANT_TWENTY_NINE", "unset:TENANT_TWENTY_NINE",
		"lookup:UPSTREAM_TOKEN", "unset:UPSTREAM_TOKEN",
	}
	if fmt.Sprint(source.events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %v, want %v", source.events, wantEvents)
	}
}

func TestResolveSecretsContinuesConsumptionAndCleansPartialResult(t *testing.T) {
	dependencyCanary := "dependency-canary-private-detail"
	source := &fakeSecretSource{
		values: map[string]string{
			"TENANT_ELEVEN":      "first-token",
			"TENANT_TWENTY_NINE": "late-token",
			"UPSTREAM_TOKEN":     "upstream-token",
		},
		present: map[string]bool{
			"TENANT_ELEVEN":      true,
			"TENANT_TWENTY_NINE": false,
			"UPSTREAM_TOKEN":     true,
		},
		unsetErrs: map[string]error{
			"TENANT_ELEVEN": errors.New(dependencyCanary),
		},
	}

	resolved, err := resolveSecrets(secretTestPolicy(), source)
	assertSecretResolutionError(t, err, dependencyCanary, "first-token", "late-token", "upstream-token")
	if resolved.credentials != nil || resolved.upstream != "" {
		t.Fatalf("failed resolution retained secrets: %#v", resolved)
	}
	wantEvents := []string{
		"lookup:TENANT_ELEVEN", "unset:TENANT_ELEVEN",
		"lookup:TENANT_TWENTY_NINE", "unset:TENANT_TWENTY_NINE",
		"lookup:UPSTREAM_TOKEN", "unset:UPSTREAM_TOKEN",
	}
	if fmt.Sprint(source.events) != fmt.Sprint(wantEvents) {
		t.Fatalf("events = %v, want %v", source.events, wantEvents)
	}
}

func TestResolveSecretsRejectsDuplicateValuesAcrossEveryDomain(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
	}{
		{
			name: "tenant credentials",
			values: map[string]string{
				"TENANT_ELEVEN":      "duplicate",
				"TENANT_TWENTY_NINE": "duplicate",
				"UPSTREAM_TOKEN":     "upstream",
			},
		},
		{
			name: "tenant and upstream",
			values: map[string]string{
				"TENANT_ELEVEN":      "first",
				"TENANT_TWENTY_NINE": "cross-domain",
				"UPSTREAM_TOKEN":     "cross-domain",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &fakeSecretSource{values: test.values}
			resolved, err := resolveSecrets(secretTestPolicy(), source)
			assertSecretResolutionError(t, err)
			if resolved.credentials != nil || resolved.upstream != "" {
				t.Fatalf("failed resolution retained secrets: %#v", resolved)
			}
			if len(source.events) != 6 {
				t.Fatalf("events = %v, want all configured secrets consumed", source.events)
			}
		})
	}
}

func TestResolveSecretsEnforcesByteBoundsWithoutChangingValues(t *testing.T) {
	maximum := strings.Repeat("a", maximumSecretBytes)
	source := &fakeSecretSource{values: map[string]string{
		"TENANT_ELEVEN":      maximum,
		"TENANT_TWENTY_NINE": "second",
		"UPSTREAM_TOKEN":     "upstream",
	}}
	resolved, err := resolveSecrets(secretTestPolicy(), source)
	if err != nil {
		t.Fatalf("resolveSecrets(maximum) error = %v", err)
	}
	if resolved.credentials[0].Token != maximum {
		t.Fatal("maximum-sized secret was changed")
	}
	resolved.clear()

	source = &fakeSecretSource{values: map[string]string{
		"TENANT_ELEVEN":      strings.Repeat("b", maximumSecretBytes+1),
		"TENANT_TWENTY_NINE": "second",
		"UPSTREAM_TOKEN":     "upstream",
	}}
	failed, err := resolveSecrets(secretTestPolicy(), source)
	assertSecretResolutionError(t, err)
	if failed.credentials != nil || failed.upstream != "" {
		t.Fatalf("failed resolution retained secrets: %#v", failed)
	}
	if len(source.events) != 6 {
		t.Fatalf("events = %v, want all configured secrets consumed", source.events)
	}
}

func TestResolveSecretsRejectsEmptyMissingAndNilInputs(t *testing.T) {
	tests := []struct {
		name   string
		policy *validatedPolicy
		source secretSource
	}{
		{name: "nil policy", source: &fakeSecretSource{}},
		{name: "nil source", policy: secretTestPolicy()},
		{name: "typed nil source", policy: secretTestPolicy(), source: (*fakeSecretSource)(nil)},
		{
			name:   "missing",
			policy: secretTestPolicy(),
			source: &fakeSecretSource{values: map[string]string{
				"TENANT_ELEVEN":  "first",
				"UPSTREAM_TOKEN": "upstream",
			}},
		},
		{
			name:   "empty",
			policy: secretTestPolicy(),
			source: &fakeSecretSource{values: map[string]string{
				"TENANT_ELEVEN":      "first",
				"TENANT_TWENTY_NINE": "",
				"UPSTREAM_TOKEN":     "upstream",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolveSecrets(test.policy, test.source)
			assertSecretResolutionError(t, err)
			if resolved.credentials != nil || resolved.upstream != "" {
				t.Fatalf("failed resolution retained secrets: %#v", resolved)
			}
		})
	}
}

func TestResolvedSecretsFormattingAndClearAreRedacted(t *testing.T) {
	const canary = "formatting-secret-canary"
	resolved := resolvedSecrets{
		credentials: []httpapi.Credential{{Tenant: 7, Token: canary}},
		upstream:    "upstream-" + canary,
	}
	for _, formatted := range []string{
		fmt.Sprint(resolved),
		fmt.Sprintf("%#v", resolved),
		fmt.Sprint(&resolved),
		fmt.Sprintf("%#v", &resolved),
	} {
		if strings.Contains(formatted, canary) {
			t.Fatalf("formatted resolved secrets leaked canary: %q", formatted)
		}
		if !strings.Contains(formatted, "redacted") {
			t.Fatalf("formatted resolved secrets are not explicitly redacted: %q", formatted)
		}
	}

	alias := resolved.credentials
	resolved.clear()
	if resolved.credentials != nil || resolved.upstream != "" {
		t.Fatalf("clear retained fields: %#v", resolved)
	}
	if alias[0] != (httpapi.Credential{}) {
		t.Fatalf("clear did not overwrite the credential slice entry")
	}
	var nilResolved *resolvedSecrets
	nilResolved.clear()
}

func assertSecretResolutionError(t *testing.T, err error, canaries ...string) {
	t.Helper()
	if !errors.Is(err, errSecretResolutionFailed) || err != errSecretResolutionFailed {
		t.Fatalf("error = %#v, want exact errSecretResolutionFailed", err)
	}
	for _, formatted := range []string{err.Error(), fmt.Sprint(err), fmt.Sprintf("%#v", err)} {
		for _, canary := range canaries {
			if strings.Contains(formatted, canary) {
				t.Fatalf("error leaked canary %q: %q", canary, formatted)
			}
		}
	}
}
