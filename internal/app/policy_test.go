package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

func TestParsePolicyMapsEveryValidatedField(t *testing.T) {
	document := canonicalPolicyDocument()
	policy, err := parsePolicy(marshalPolicyDocument(t, document))
	if err != nil {
		t.Fatalf("parsePolicy() error = %v", err)
	}
	if policy == nil {
		t.Fatal("parsePolicy() policy is nil")
	}

	wantParser, err := contract.NewParser(document.Contract.PublicModel, contract.Limits{
		MaxBodyBytes:        document.Contract.MaxBodyBytes,
		MaxMessageCount:     document.Contract.MaxMessageCount,
		MaxMessageTextBytes: document.Contract.MaxMessageTextBytes,
		MaxCompletionTokens: document.Contract.MaxCompletionTokens,
		CompletionWeight:    document.Contract.CompletionWeight,
		MaxRequestUnits:     document.Contract.MaxRequestUnits,
	})
	if err != nil {
		t.Fatalf("construct expected parser: %v", err)
	}

	wantAdmission := admission.Config{
		MaxBodyBytes:    1_048_576,
		MaxRequestUnits: 2_097_152,
		BaseQuantum:     262_144,
		DeficitCap:      2_621_439,
		GlobalQueue: admission.QueueLimits{
			Count: 64,
			Bytes: 67_108_864,
			Work:  134_217_728,
		},
		GlobalInflight: admission.InflightLimits{Count: 8, Work: 16_777_216},
		Tenants: []admission.TenantConfig{
			{
				ID:       1,
				Weight:   1,
				Queue:    admission.QueueLimits{Count: 32, Bytes: 33_554_432, Work: 67_108_864},
				Inflight: admission.InflightLimits{Count: 4, Work: 8_388_608},
			},
			{
				ID:       2,
				Weight:   2,
				Queue:    admission.QueueLimits{Count: 16, Bytes: 16_777_216, Work: 33_554_432},
				Inflight: admission.InflightLimits{Count: 2, Work: 4_194_304},
			},
		},
	}
	wantHTTP := httpapi.Config{
		DefaultQueueTimeout:    5 * time.Second,
		BodyReadTimeout:        10 * time.Second,
		UpstreamTimeout:        2 * time.Minute,
		MaxResponseBodyBytes:   8_388_608,
		GlobalPreDispatchLimit: 16,
		TenantPreDispatch: []httpapi.TenantPreDispatchLimit{
			{Tenant: 1, Count: 8},
			{Tenant: 2, Count: 4},
		},
	}
	wantUpstream := httpapi.HTTPUpstreamConfig{
		Endpoint:               "https://api.example.com/v1/chat/completions",
		ConnectTimeout:         3 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  time.Minute,
		IdleConnectionTimeout:  30 * time.Second,
		MaxResponseHeaderBytes: 65_536,
		MaxConnections:         8,
	}
	wantServer := server.Config{
		HeaderReadTimeout:       2 * time.Second,
		ResponseWriteTimeout:    15 * time.Second,
		IdleTimeout:             30 * time.Second,
		GraceTimeout:            30 * time.Second,
		ForceTimeout:            5 * time.Second,
		HeaderReadEnvelopeBytes: 16_384,
		MaxConnections:          64,
	}
	wantCredentials := []credentialReference{
		{tenant: 1, env: "SSEMAPHORE_TENANT_1_PRIMARY"},
		{tenant: 1, env: "SSEMAPHORE_TENANT_1_ROTATION"},
		{tenant: 2, env: "SSEMAPHORE_TENANT_2_PRIMARY"},
	}

	if policy.listener != (listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 18_080}) {
		t.Fatalf("listener plan = %+v", policy.listener)
	}
	if !reflect.DeepEqual(policy.parser, wantParser) {
		t.Fatal("parser does not preserve the complete contract policy")
	}
	if !reflect.DeepEqual(policy.admission, wantAdmission) {
		t.Fatalf("admission config = %#v, want %#v", policy.admission, wantAdmission)
	}
	if !reflect.DeepEqual(policy.http, wantHTTP) {
		t.Fatalf("HTTP config = %#v, want %#v", policy.http, wantHTTP)
	}
	if !reflect.DeepEqual(policy.upstream, wantUpstream) {
		t.Fatal("upstream config does not preserve the complete transport policy")
	}
	if !reflect.DeepEqual(policy.server, wantServer) {
		t.Fatalf("server config = %#v, want %#v", policy.server, wantServer)
	}
	if !reflect.DeepEqual(policy.credentials, wantCredentials) {
		t.Fatalf("credential references = %#v, want %#v", policy.credentials, wantCredentials)
	}
	if policy.upstreamTokenEnv != "SSEMAPHORE_UPSTREAM_BEARER_TOKEN" {
		t.Fatalf("upstream token environment = %q", policy.upstreamTokenEnv)
	}
	if policy.shutdownWait != 45*time.Second {
		t.Fatalf("shutdown wait = %v, want 45s", policy.shutdownWait)
	}

	request, err := policy.parser.Parse(context.Background(), strings.NewReader(
		`{"model":"portfolio-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":2}`,
	))
	if err != nil {
		t.Fatalf("mapped parser rejected its configured public model: %v", err)
	}
	if request.Model() != document.Contract.PublicModel || request.MaxCompletionTokens() != 2 {
		t.Fatalf("parsed request = model %q, completion tokens %d", request.Model(), request.MaxCompletionTokens())
	}
}

func TestValidatedPolicyFormattingIsAlwaysRedacted(t *testing.T) {
	policy, err := parsePolicy(marshalPolicyDocument(t, canonicalPolicyDocument()))
	if err != nil {
		t.Fatalf("parsePolicy() error = %v", err)
	}

	const redacted = "app.validatedPolicy{redacted}"
	if got := policy.String(); got != redacted {
		t.Fatalf("String() = %q, want %q", got, redacted)
	}
	if got := policy.GoString(); got != redacted {
		t.Fatalf("GoString() = %q, want %q", got, redacted)
	}
	for verb, rendered := range map[string]string{
		"%v":  fmt.Sprintf("%v", policy),
		"%+v": fmt.Sprintf("%+v", policy),
		"%#v": fmt.Sprintf("%#v", policy),
	} {
		if rendered != redacted {
			t.Errorf("format %s rendered %q, want %q", verb, rendered, redacted)
		}
		for _, canary := range []string{
			"portfolio-model",
			"api.example.com",
			"SSEMAPHORE_TENANT_1_PRIMARY",
			"SSEMAPHORE_UPSTREAM_BEARER_TOKEN",
		} {
			if strings.Contains(rendered, canary) {
				t.Errorf("format %s disclosed %q", verb, canary)
			}
		}
	}
}

func TestParsePolicyRejectsSchemaListenerAndContractViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*policyDocument)
	}{
		{name: "unsupported schema version", mutate: func(d *policyDocument) { d.SchemaVersion = 2 }},
		{name: "unsupported listener type", mutate: func(d *policyDocument) { d.Listener.Type = "unix" }},
		{name: "wildcard listener", mutate: func(d *policyDocument) { d.Listener.Host = "0.0.0.0" }},
		{name: "DNS listener", mutate: func(d *policyDocument) { d.Listener.Host = "localhost" }},
		{name: "zero listener port", mutate: func(d *policyDocument) { d.Listener.Port = 0 }},
		{name: "listener port above range", mutate: func(d *policyDocument) { d.Listener.Port = 65_536 }},
		{name: "empty public model", mutate: func(d *policyDocument) { d.Contract.PublicModel = "" }},
		{name: "public model above bound", mutate: func(d *policyDocument) { d.Contract.PublicModel = strings.Repeat("m", 1_025) }},
		{name: "zero body bytes", mutate: func(d *policyDocument) { d.Contract.MaxBodyBytes = 0 }},
		{name: "body bytes above bound", mutate: func(d *policyDocument) { d.Contract.MaxBodyBytes = contract.AbsoluteMaxBodyBytes + 1 }},
		{name: "zero message count", mutate: func(d *policyDocument) { d.Contract.MaxMessageCount = 0 }},
		{name: "zero message text bytes", mutate: func(d *policyDocument) { d.Contract.MaxMessageTextBytes = 0 }},
		{name: "zero completion tokens", mutate: func(d *policyDocument) { d.Contract.MaxCompletionTokens = 0 }},
		{name: "zero completion weight", mutate: func(d *policyDocument) { d.Contract.CompletionWeight = 0 }},
		{name: "completion reservation overflow", mutate: func(d *policyDocument) { d.Contract.CompletionWeight = math.MaxUint64 }},
		{name: "zero request units", mutate: func(d *policyDocument) { d.Contract.MaxRequestUnits = 0 }},
		{name: "request units below minimum", mutate: func(d *policyDocument) { d.Contract.MaxRequestUnits = 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := canonicalPolicyDocument()
			test.mutate(&document)
			assertPolicyRejected(t, marshalPolicyDocument(t, document))
		})
	}
}

func TestParsePolicyRejectsEveryTimeoutOutsidePolicyBounds(t *testing.T) {
	fields := []struct {
		name    string
		maximum uint64
		set     func(*policyDocument, uint64)
	}{
		{name: "HTTP default queue", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.HTTP.DefaultQueueTimeoutMS = value }},
		{name: "HTTP body read", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.HTTP.BodyReadTimeoutMS = value }},
		{name: "HTTP upstream", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.HTTP.UpstreamTimeoutMS = value }},
		{name: "upstream connect", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Upstream.ConnectTimeoutMS = value }},
		{name: "upstream TLS handshake", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Upstream.TLSHandshakeTimeoutMS = value }},
		{name: "upstream response header", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Upstream.ResponseHeaderTimeoutMS = value }},
		{name: "upstream idle connection", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Upstream.IdleConnectionTimeoutMS = value }},
		{name: "server header read", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Server.HeaderReadTimeoutMS = value }},
		{name: "server response write", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Server.ResponseWriteTimeoutMS = value }},
		{name: "server idle", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Server.IdleTimeoutMS = value }},
		{name: "server grace", maximum: maximumPolicyTimeoutMS, set: func(d *policyDocument, value uint64) { d.Server.GraceTimeoutMS = value }},
		{name: "server force", maximum: maximumForceTimeoutMS, set: func(d *policyDocument, value uint64) { d.Server.ForceTimeoutMS = value }},
	}

	for _, field := range fields {
		for _, boundary := range []struct {
			name  string
			value uint64
		}{
			{name: "zero", value: 0},
			{name: "above maximum", value: field.maximum + 1},
		} {
			t.Run(field.name+"/"+boundary.name, func(t *testing.T) {
				document := canonicalPolicyDocument()
				field.set(&document, boundary.value)
				assertPolicyRejected(t, marshalPolicyDocument(t, document))
			})
		}
	}
}

func TestParsePolicyRejectsAdmissionAndPreDispatchViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*policyDocument)
	}{
		{name: "no tenants", mutate: func(d *policyDocument) { d.Admission.Tenants = nil }},
		{name: "too many tenants", mutate: func(d *policyDocument) {
			tenant := d.Admission.Tenants[0]
			d.Admission.Tenants = make([]tenantPolicy, maximumPolicyTenants+1)
			for index := range d.Admission.Tenants {
				d.Admission.Tenants[index] = tenant
			}
		}},
		{name: "zero tenant ID", mutate: func(d *policyDocument) { d.Admission.Tenants[0].ID = 0 }},
		{name: "tenant ID above range", mutate: func(d *policyDocument) { d.Admission.Tenants[0].ID = uint64(math.MaxUint32) + 1 }},
		{name: "duplicate tenant ID", mutate: func(d *policyDocument) { d.Admission.Tenants[1].ID = d.Admission.Tenants[0].ID }},
		{name: "zero tenant weight", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Weight = 0 }},
		{name: "zero base quantum", mutate: func(d *policyDocument) { d.Admission.BaseQuantum = 0 }},
		{name: "quantum overflow", mutate: func(d *policyDocument) { d.Admission.BaseQuantum = math.MaxUint64 }},
		{name: "deficit cap below required bound", mutate: func(d *policyDocument) { d.Admission.DeficitCap = 1 }},
		{name: "funding CPU bound", mutate: func(d *policyDocument) {
			d.Admission.BaseQuantum = 1
			d.Admission.DeficitCap = d.Contract.MaxRequestUnits + 1
		}},
		{name: "zero global queue count", mutate: func(d *policyDocument) { d.Admission.GlobalQueue.Count = 0 }},
		{name: "global queue body below one request", mutate: func(d *policyDocument) { d.Admission.GlobalQueue.Bytes = d.Contract.MaxBodyBytes - 1 }},
		{name: "global queue work below one request", mutate: func(d *policyDocument) { d.Admission.GlobalQueue.Work = d.Contract.MaxRequestUnits - 1 }},
		{name: "global queue count above hard bound", mutate: func(d *policyDocument) { d.Admission.GlobalQueue.Count = 65_537 }},
		{name: "zero global inflight count", mutate: func(d *policyDocument) { d.Admission.GlobalInflight.Count = 0 }},
		{name: "global inflight work below one request", mutate: func(d *policyDocument) { d.Admission.GlobalInflight.Work = d.Contract.MaxRequestUnits - 1 }},
		{name: "global inflight count above hard bound", mutate: func(d *policyDocument) { d.Admission.GlobalInflight.Count = 4_097 }},
		{name: "zero tenant queue count", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Queue.Count = 0 }},
		{name: "tenant queue bytes exceed global", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Queue.Bytes = d.Admission.GlobalQueue.Bytes + 1 }},
		{name: "tenant queue work exceeds global", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Queue.Work = d.Admission.GlobalQueue.Work + 1 }},
		{name: "zero tenant inflight count", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Inflight.Count = 0 }},
		{name: "tenant inflight work below one request", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Inflight.Work = d.Contract.MaxRequestUnits - 1 }},
		{name: "tenant inflight count exceeds global", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Inflight.Count = d.Admission.GlobalInflight.Count + 1 }},
		{name: "tenant inflight work exceeds global", mutate: func(d *policyDocument) { d.Admission.Tenants[0].Inflight.Work = d.Admission.GlobalInflight.Work + 1 }},
		{name: "zero global pre-dispatch count", mutate: func(d *policyDocument) { d.HTTP.GlobalPreDispatchCount = 0 }},
		{name: "global pre-dispatch count above hard bound", mutate: func(d *policyDocument) { d.HTTP.GlobalPreDispatchCount = 4_097 }},
		{name: "global pre-dispatch count exceeds queue count", mutate: func(d *policyDocument) { d.HTTP.GlobalPreDispatchCount = d.Admission.GlobalQueue.Count + 1 }},
		{name: "global pre-dispatch bodies exceed envelope", mutate: func(d *policyDocument) {
			d.HTTP.GlobalPreDispatchCount = 2
			d.Admission.GlobalQueue.Bytes = d.Contract.MaxBodyBytes
			for index := range d.Admission.Tenants {
				d.Admission.Tenants[index].Queue.Bytes = d.Contract.MaxBodyBytes
				d.Admission.Tenants[index].PreDispatchCount = 1
			}
		}},
		{name: "global pre-dispatch work exceeds envelope", mutate: func(d *policyDocument) {
			d.HTTP.GlobalPreDispatchCount = 2
			d.Admission.GlobalQueue.Work = d.Contract.MaxRequestUnits
			for index := range d.Admission.Tenants {
				d.Admission.Tenants[index].Queue.Work = d.Contract.MaxRequestUnits
				d.Admission.Tenants[index].PreDispatchCount = 1
			}
		}},
		{name: "zero tenant pre-dispatch count", mutate: func(d *policyDocument) { d.Admission.Tenants[0].PreDispatchCount = 0 }},
		{name: "tenant pre-dispatch count exceeds global", mutate: func(d *policyDocument) { d.Admission.Tenants[0].PreDispatchCount = d.HTTP.GlobalPreDispatchCount + 1 }},
		{name: "tenant pre-dispatch bodies exceed envelope", mutate: func(d *policyDocument) {
			d.Admission.Tenants[0].PreDispatchCount = 2
			d.Admission.Tenants[0].Queue.Bytes = d.Contract.MaxBodyBytes
		}},
		{name: "tenant pre-dispatch work exceeds envelope", mutate: func(d *policyDocument) {
			d.Admission.Tenants[0].PreDispatchCount = 2
			d.Admission.Tenants[0].Queue.Work = d.Contract.MaxRequestUnits
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := canonicalPolicyDocument()
			test.mutate(&document)
			assertPolicyRejected(t, marshalPolicyDocument(t, document))
		})
	}
}

func TestParsePolicyRejectsCredentialEnvironmentViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*policyDocument)
	}{
		{name: "tenant without credential reference", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs = nil }},
		{name: "empty tenant environment", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs[0] = "" }},
		{name: "lowercase tenant environment", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs[0] = "tenant_token" }},
		{name: "digit starts tenant environment", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs[0] = "1_TOKEN" }},
		{name: "punctuation in tenant environment", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs[0] = "TENANT-TOKEN" }},
		{name: "non-ASCII tenant environment", mutate: func(d *policyDocument) { d.Admission.Tenants[0].BearerTokenEnvs[0] = "TØKEN" }},
		{name: "tenant environment above length bound", mutate: func(d *policyDocument) {
			d.Admission.Tenants[0].BearerTokenEnvs[0] = strings.Repeat("A", maximumEnvironmentNameLen+1)
		}},
		{name: "duplicate tenant environment", mutate: func(d *policyDocument) {
			d.Admission.Tenants[0].BearerTokenEnvs[1] = d.Admission.Tenants[0].BearerTokenEnvs[0]
		}},
		{name: "duplicate environment across tenants", mutate: func(d *policyDocument) {
			d.Admission.Tenants[1].BearerTokenEnvs[0] = d.Admission.Tenants[0].BearerTokenEnvs[0]
		}},
		{name: "too many credentials", mutate: func(d *policyDocument) {
			names := make([]string, maximumPolicyCredentials+1)
			for index := range names {
				names[index] = fmt.Sprintf("TENANT_TOKEN_%04d", index)
			}
			d.Admission.Tenants[0].BearerTokenEnvs = names
		}},
		{name: "empty upstream environment", mutate: func(d *policyDocument) { d.Upstream.BearerTokenEnv = "" }},
		{name: "lowercase upstream environment", mutate: func(d *policyDocument) { d.Upstream.BearerTokenEnv = "upstream_token" }},
		{name: "upstream environment above length bound", mutate: func(d *policyDocument) { d.Upstream.BearerTokenEnv = strings.Repeat("U", maximumEnvironmentNameLen+1) }},
		{name: "upstream environment collides with tenant", mutate: func(d *policyDocument) { d.Upstream.BearerTokenEnv = d.Admission.Tenants[1].BearerTokenEnvs[0] }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := canonicalPolicyDocument()
			test.mutate(&document)
			assertPolicyRejected(t, marshalPolicyDocument(t, document))
		})
	}
}

func TestParsePolicyRejectsUpstreamResponseAndServerBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*policyDocument)
	}{
		{name: "empty upstream endpoint", mutate: func(d *policyDocument) { d.Upstream.Endpoint = "" }},
		{name: "plaintext remote upstream", mutate: func(d *policyDocument) { d.Upstream.Endpoint = "http://api.example.com/v1/chat/completions" }},
		{name: "upstream userinfo", mutate: func(d *policyDocument) { d.Upstream.Endpoint = "https://user@example.com/v1/chat/completions" }},
		{name: "upstream query", mutate: func(d *policyDocument) { d.Upstream.Endpoint = "https://example.com/v1/chat/completions?canary=1" }},
		{name: "upstream wrong path", mutate: func(d *policyDocument) { d.Upstream.Endpoint = "https://example.com/v1/responses" }},
		{name: "upstream endpoint above length bound", mutate: func(d *policyDocument) {
			d.Upstream.Endpoint = "https://" + strings.Repeat("a", 4_097) + "/v1/chat/completions"
		}},
		{name: "zero response body limit", mutate: func(d *policyDocument) { d.HTTP.MaxResponseBodyBytes = 0 }},
		{name: "response body above hard bound", mutate: func(d *policyDocument) { d.HTTP.MaxResponseBodyBytes = contract.AbsoluteMaxResponseBodyBytes + 1 }},
		{name: "zero upstream response header bytes", mutate: func(d *policyDocument) { d.Upstream.MaxResponseHeaderBytes = 0 }},
		{name: "upstream response header bytes above hard bound", mutate: func(d *policyDocument) { d.Upstream.MaxResponseHeaderBytes = (1 << 20) + 1 }},
		{name: "upstream response header bytes above int64", mutate: func(d *policyDocument) { d.Upstream.MaxResponseHeaderBytes = math.MaxUint64 }},
		{name: "zero upstream connections", mutate: func(d *policyDocument) { d.Upstream.MaxConnections = 0 }},
		{name: "upstream connections above hard bound", mutate: func(d *policyDocument) { d.Upstream.MaxConnections = 4_097 }},
		{name: "server header envelope below hard bound", mutate: func(d *policyDocument) { d.Server.HeaderReadEnvelopeBytes = (8 << 10) - 1 }},
		{name: "server header envelope above hard bound", mutate: func(d *policyDocument) { d.Server.HeaderReadEnvelopeBytes = (64 << 10) + 1 }},
		{name: "zero server connections", mutate: func(d *policyDocument) { d.Server.MaxConnections = 0 }},
		{name: "server connections above hard bound", mutate: func(d *policyDocument) { d.Server.MaxConnections = 1_025 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := canonicalPolicyDocument()
			test.mutate(&document)
			assertPolicyRejected(t, marshalPolicyDocument(t, document))
		})
	}
}

func TestParsePolicyReducesDocumentFailuresToStaticError(t *testing.T) {
	canonical := marshalPolicyDocument(t, canonicalPolicyDocument())
	var missing map[string]any
	if err := json.Unmarshal(canonical, &missing); err != nil {
		t.Fatalf("decode canonical policy: %v", err)
	}
	delete(missing, "server")
	missingServer, err := json.Marshal(missing)
	if err != nil {
		t.Fatalf("encode missing-field policy: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "top-level null", data: []byte(`null`)},
		{name: "null listener", data: []byte(`{"schema_version":1,"listener":null}`)},
		{name: "missing server", data: missingServer},
		{name: "unknown canary", data: []byte(`{"schema_version":1,"UNKNOWN_POLICY_CANARY":"SECRET_POLICY_CANARY"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertPolicyRejected(t, test.data)
		})
	}
}

func TestParsePolicyErrorNeverDisclosesInputCanary(t *testing.T) {
	const canary = "SECRET_POLICY_CANARY_DO_NOT_DISCLOSE"
	document := canonicalPolicyDocument()
	document.Upstream.Endpoint = "https://example.com/v1/chat/completions?token=" + canary

	policy, err := parsePolicy(marshalPolicyDocument(t, document))
	if policy != nil {
		t.Fatal("parsePolicy() returned a policy for an invalid endpoint")
	}
	if err != errPolicyRejected {
		t.Fatalf("parsePolicy() error = %v, want exact static error", err)
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("parsePolicy() error disclosed input canary: %q", err)
	}
}

func TestLoadPolicyAcceptsPrivateFixtureAndRejectsInvalidFiles(t *testing.T) {
	validPath := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))
	info, err := os.Stat(validPath)
	if err != nil {
		t.Fatalf("stat valid policy fixture: %v", err)
	}
	if info.Mode() != 0o600 {
		t.Fatalf("valid policy fixture mode = %v, want 0600 regular file", info.Mode())
	}

	policy, err := loadPolicy(validPath)
	if err != nil {
		t.Fatalf("loadPolicy(valid) error = %v", err)
	}
	if policy == nil || policy.listener.address != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("loadPolicy(valid) policy = %v", policy)
	}

	invalidContentPath := writePolicyFixture(t, []byte(`{"UNKNOWN_FILE_CANARY":"SECRET_FILE_CANARY"}`))
	if policy, err := loadPolicy(invalidContentPath); policy != nil || err != errPolicyRejected {
		t.Fatalf("loadPolicy(invalid content) = (%v, %v), want (nil, exact policy error)", policy, err)
	}

	invalidModePath := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))
	if err := os.Chmod(invalidModePath, 0o640); err != nil {
		t.Fatalf("chmod invalid policy fixture: %v", err)
	}
	if policy, err := loadPolicy(invalidModePath); policy != nil || err != errPolicyReadFailed {
		t.Fatalf("loadPolicy(invalid file) = (%v, %v), want (nil, exact read error)", policy, err)
	}
}

func TestCommittedExamplePolicyRemainsValid(t *testing.T) {
	data, err := os.ReadFile("../../configs/policy.example.json")
	if err != nil {
		t.Fatalf("read committed policy example: %v", err)
	}
	policy, err := parsePolicy(data)
	if err != nil {
		t.Fatalf("parse committed policy example: %v", err)
	}
	if policy == nil {
		t.Fatal("committed policy example produced a nil policy")
	}
}

func assertPolicyRejected(t *testing.T, data []byte) {
	t.Helper()
	policy, err := parsePolicy(data)
	if policy != nil {
		t.Fatalf("parsePolicy() policy = %v, want nil", policy)
	}
	if err != errPolicyRejected {
		t.Fatalf("parsePolicy() error = %v, want exact static error", err)
	}
	if err.Error() != "gateway policy is rejected" {
		t.Fatalf("parsePolicy() error text = %q", err.Error())
	}
	for _, canary := range []string{"UNKNOWN_POLICY_CANARY", "SECRET_POLICY_CANARY"} {
		if strings.Contains(err.Error(), canary) {
			t.Fatalf("parsePolicy() error disclosed %q", canary)
		}
	}
}

func marshalPolicyDocument(t *testing.T, document policyDocument) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal policy fixture: %v", err)
	}
	return data
}

func canonicalPolicyDocument() policyDocument {
	return policyDocument{
		SchemaVersion: 1,
		Listener: listenerPolicy{
			Type: "tcp",
			Host: "127.0.0.1",
			Port: 18_080,
		},
		Contract: contractPolicy{
			PublicModel:         "portfolio-model",
			MaxBodyBytes:        1_048_576,
			MaxMessageCount:     64,
			MaxMessageTextBytes: 262_144,
			MaxCompletionTokens: 4_096,
			CompletionWeight:    256,
			MaxRequestUnits:     2_097_152,
		},
		Admission: admissionPolicy{
			BaseQuantum: 262_144,
			DeficitCap:  2_621_439,
			GlobalQueue: queuePolicy{
				Count: 64,
				Bytes: 67_108_864,
				Work:  134_217_728,
			},
			GlobalInflight: inflightPolicy{Count: 8, Work: 16_777_216},
			Tenants: []tenantPolicy{
				{
					ID:               1,
					Weight:           1,
					Queue:            queuePolicy{Count: 32, Bytes: 33_554_432, Work: 67_108_864},
					Inflight:         inflightPolicy{Count: 4, Work: 8_388_608},
					PreDispatchCount: 8,
					BearerTokenEnvs: []string{
						"SSEMAPHORE_TENANT_1_PRIMARY",
						"SSEMAPHORE_TENANT_1_ROTATION",
					},
				},
				{
					ID:               2,
					Weight:           2,
					Queue:            queuePolicy{Count: 16, Bytes: 16_777_216, Work: 33_554_432},
					Inflight:         inflightPolicy{Count: 2, Work: 4_194_304},
					PreDispatchCount: 4,
					BearerTokenEnvs:  []string{"SSEMAPHORE_TENANT_2_PRIMARY"},
				},
			},
		},
		HTTP: httpPolicy{
			DefaultQueueTimeoutMS:  5_000,
			BodyReadTimeoutMS:      10_000,
			UpstreamTimeoutMS:      120_000,
			MaxResponseBodyBytes:   8_388_608,
			GlobalPreDispatchCount: 16,
		},
		Upstream: upstreamPolicy{
			Endpoint:                "https://api.example.com/v1/chat/completions",
			BearerTokenEnv:          "SSEMAPHORE_UPSTREAM_BEARER_TOKEN",
			ConnectTimeoutMS:        3_000,
			TLSHandshakeTimeoutMS:   5_000,
			ResponseHeaderTimeoutMS: 60_000,
			IdleConnectionTimeoutMS: 30_000,
			MaxResponseHeaderBytes:  65_536,
			MaxConnections:          8,
		},
		Server: serverPolicy{
			HeaderReadTimeoutMS:     2_000,
			ResponseWriteTimeoutMS:  15_000,
			IdleTimeoutMS:           30_000,
			GraceTimeoutMS:          30_000,
			ForceTimeoutMS:          5_000,
			HeaderReadEnvelopeBytes: 16_384,
			MaxConnections:          64,
		},
	}
}
