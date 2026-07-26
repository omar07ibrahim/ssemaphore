package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSaturationVisualTimeoutBudgetsKeepIndependentMargin(t *testing.T) {
	if visualSaturationProcessTimeout < 55*time.Second {
		t.Fatal("saturation process timeout does not cover execution and cleanup envelopes")
	}
	if visualSaturationOverallTimeout <
		visualSaturationBuildTimeout+visualSaturationProcessTimeout+5*time.Second {
		t.Fatal("overall saturation timeout has no margin beyond build and process bounds")
	}
}

func TestSaturationVisualProjectionExcludesDiagnosticTimingsAndBindsDigest(t *testing.T) {
	report := completeSaturationVisualReport()
	if err := validateSaturationVisualReport(report); err != nil {
		t.Fatalf("validateSaturationVisualReport(valid) error = %v", err)
	}
	evidence, err := saturationVisualEvidenceFromReport(report)
	if err != nil {
		t.Fatalf("saturationVisualEvidenceFromReport(valid) error = %v", err)
	}
	if err := validateSaturationVisualEvidence(evidence); err != nil {
		t.Fatalf("validateSaturationVisualEvidence(valid) error = %v", err)
	}
	digest, err := saturationVisualProjectionDigest(evidence.Projection)
	if err != nil || digest != visualSaturationDigest {
		t.Fatalf("projection digest = (%q, %v)", digest, err)
	}

	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("json.Marshal(evidence) error = %v", err)
	}
	for _, forbidden := range []string{
		"timing_hooks",
		"submit_to_upstream_dispatch_ns",
		"upstream_release_to_first_sse_event_ns",
		`"performance_claim":true`,
		"127.0.0.1",
		"Authorization",
		"synthetic-tenant",
	} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("deterministic evidence contains excluded field %q", forbidden)
		}
	}

	alteredTiming := report
	alteredTiming.Timing.Samples = append(
		[]saturationVisualTimingSample(nil),
		report.Timing.Samples...,
	)
	alteredTiming.Timing.Samples[0].SubmitToUpstreamDispatchNS++
	alteredEvidence, err := saturationVisualEvidenceFromReport(alteredTiming)
	if err != nil {
		t.Fatalf("timing-only mutation error = %v", err)
	}
	if !reflect.DeepEqual(evidence, alteredEvidence) {
		t.Fatal("timing-only mutation changed the published projection")
	}

	alteredCategory := report
	alteredCategory.Service.Observed = append(
		[]saturationVisualDispatch(nil),
		report.Service.Observed...,
	)
	alteredCategory.Service.Observed[0].WorkUnits++
	if err := validateSaturationVisualReport(alteredCategory); err == nil {
		t.Fatal("categorical mutation passed report validation")
	}
}

func TestSaturationVisualDecoderIsClosedAndOutputIsBounded(t *testing.T) {
	report := completeSaturationVisualReport()
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("json.Marshal(report) error = %v", err)
	}
	encoded = append(encoded, '\n')
	if _, err := decodeSaturationVisualReport(encoded); err != nil {
		t.Fatalf("decodeSaturationVisualReport(valid) error = %v", err)
	}

	withUnknown := bytes.Replace(
		encoded,
		[]byte(`"schema_version":3`),
		[]byte(`"schema_version":3,"unknown_canary":true`),
		1,
	)
	if _, err := decodeSaturationVisualReport(withUnknown); err == nil {
		t.Fatal("decoder accepted an unknown field")
	}
	withDuplicate := bytes.Replace(
		encoded,
		[]byte(`"schema_version":3`),
		[]byte(`"schema_version":3,"schema_version":3`),
		1,
	)
	if _, err := decodeSaturationVisualReport(withDuplicate); err == nil {
		t.Fatal("decoder accepted a duplicate known field")
	}
	if _, err := decodeSaturationVisualReport(append(encoded, []byte("{}\n")...)); err == nil {
		t.Fatal("decoder accepted a trailing JSON document")
	}
	if _, err := decodeSaturationVisualReport(bytes.TrimSuffix(encoded, []byte{'\n'})); err == nil {
		t.Fatal("decoder accepted output without its exact newline")
	}

	buffer := &saturationVisualBoundedBuffer{limit: 4}
	if written, err := buffer.Write([]byte("abcdef")); err != nil || written != 6 {
		t.Fatalf("bounded Write() = (%d, %v)", written, err)
	}
	if !buffer.exceeded || string(buffer.Bytes()) != "abcd" {
		t.Fatalf("bounded buffer = (%q, %t)", buffer.Bytes(), buffer.exceeded)
	}
}

func completeSaturationVisualEvidence() saturationVisualEvidence {
	evidence, err := saturationVisualEvidenceFromReport(completeSaturationVisualReport())
	if err != nil {
		panic(err)
	}
	return evidence
}

func completeSaturationVisualReport() saturationVisualReport {
	dispatch := []saturationVisualDispatch{
		{Position: 1, Tenant: "tenant-a", Weight: 1, WorkUnits: 110, Mode: "buffered"},
		{Position: 2, Tenant: "tenant-b", Weight: 3, WorkUnits: 106, Mode: "buffered"},
		{Position: 3, Tenant: "tenant-b", Weight: 3, WorkUnits: 161, Mode: "buffered"},
		{Position: 4, Tenant: "tenant-b", Weight: 3, WorkUnits: 137, Mode: "buffered"},
		{Position: 5, Tenant: "tenant-b", Weight: 3, WorkUnits: 134, Mode: "buffered"},
		{Position: 6, Tenant: "tenant-b", Weight: 3, WorkUnits: 117, Mode: "buffered"},
		{Position: 7, Tenant: "tenant-a", Weight: 1, WorkUnits: 152, Mode: "buffered"},
		{Position: 8, Tenant: "tenant-a", Weight: 1, WorkUnits: 121, Mode: "buffered"},
		{Position: 9, Tenant: "tenant-b", Weight: 3, WorkUnits: 158, Mode: "buffered"},
		{Position: 10, Tenant: "tenant-b", Weight: 3, WorkUnits: 133, Mode: "buffered"},
		{Position: 11, Tenant: "tenant-b", Weight: 3, WorkUnits: 125, Mode: "buffered"},
		{Position: 12, Tenant: "tenant-b", Weight: 3, WorkUnits: 119, Mode: "buffered"},
		{Position: 13, Tenant: "tenant-b", Weight: 3, WorkUnits: 141, Mode: "sse"},
		{Position: 14, Tenant: "tenant-a", Weight: 1, WorkUnits: 160, Mode: "buffered"},
		{Position: 15, Tenant: "tenant-a", Weight: 1, WorkUnits: 148, Mode: "buffered"},
		{Position: 16, Tenant: "tenant-a", Weight: 1, WorkUnits: 138, Mode: "buffered"},
		{Position: 17, Tenant: "tenant-a", Weight: 1, WorkUnits: 159, Mode: "buffered"},
		{Position: 18, Tenant: "tenant-a", Weight: 1, WorkUnits: 150, Mode: "buffered"},
		{Position: 19, Tenant: "tenant-a", Weight: 1, WorkUnits: 114, Mode: "buffered"},
		{Position: 20, Tenant: "tenant-a", Weight: 1, WorkUnits: 136, Mode: "sse"},
	}
	sseDiagnostic := uint64(1)
	timings := make([]saturationVisualTimingSample, len(dispatch))
	for index, record := range dispatch {
		timings[index] = saturationVisualTimingSample{
			Position:                   record.Position,
			Tenant:                     record.Tenant,
			Mode:                       record.Mode,
			SubmitToUpstreamDispatchNS: uint64(index + 1),
		}
		if record.Mode == "sse" {
			timings[index].UpstreamReleaseToSSEEventNS = &sseDiagnostic
		}
	}
	report := saturationVisualReport{
		SchemaVersion:   3,
		Profile:         visualSaturationProfile,
		Seed:            visualSaturationSeed,
		GoVersion:       visualExpectedGo,
		OperatingSystem: visualExpectedOS,
		Architecture:    "amd64",
		Claims: saturationVisualClaims{
			Scope:     visualSaturationScope,
			Timings:   "diagnostic intervals without thresholds or service-level claims",
			Streaming: "SSE wire-order observation only; no timeout or backpressure claim",
			Encoding:  "compact JSON with stable struct field order",
		},
		Topology: saturationVisualTopology{
			Gateway:                   "production parser, scheduler, HTTP relay, and server",
			Transport:                 "HTTP/1 over numeric loopback",
			ControlledUpstream:        true,
			SeededArrivalOrder:        true,
			SchedulerSnapshotBarriers: true,
			GlobalInflightRequests:    1,
			GlobalQueuedRequests:      20,
			ServiceRequestsPerTenant:  10,
		},
		ServiceTotals: saturationVisualOutcomes{
			Submitted:        26,
			Admitted:         24,
			Rejected:         2,
			Completed:        20,
			Canceled:         2,
			DeadlineExceeded: 2,
		},
		Control: saturationVisualControl{
			Submitted:        1,
			Admitted:         1,
			Completed:        1,
			UpstreamRequests: 1,
		},
		CapacityProbes: saturationVisualProbes{
			Tenant: saturationVisualProbe{
				Scope:            "two saturated service tenants",
				Submitted:        2,
				Rejected:         2,
				StatusCode:       429,
				ErrorCode:        "tenant_capacity_exhausted",
				UpstreamRequests: 0,
			},
			Global: saturationVisualProbe{
				Scope:            "dedicated tenant with available tenant capacity",
				Submitted:        1,
				Rejected:         1,
				StatusCode:       503,
				ErrorCode:        "overloaded",
				UpstreamRequests: 0,
			},
		},
		Accounting: saturationVisualAccounting{
			TotalJobs:              28,
			ServiceSubmissions:     26,
			ControlSubmissions:     1,
			GlobalProbeSubmissions: 1,
			TotalUpstreamRequests:  21,
			Reconciled:             true,
			Boundary: "service_totals include two tenant-capacity probes; " +
				"control and the global-capacity probe are separate",
		},
		Timeouts: saturationVisualTimeouts{
			ExecutionContextMS:      30_000,
			GracefulAttemptMS:       9_000,
			AbortReserveMS:          10_000,
			MaximumCleanupMS:        19_000,
			SeparateCleanupEnvelope: true,
			Boundary: "the execution context and subsequent cleanup waits use separate " +
				"bounded envelopes, not one wall-clock deadline",
		},
		Tenants: []saturationVisualTenant{
			{
				Tenant:              "tenant-a",
				Weight:              1,
				Submitted:           13,
				Admitted:            12,
				Rejected:            1,
				Completed:           10,
				Canceled:            1,
				DeadlineExceeded:    1,
				DispatchedRequests:  10,
				DispatchedWorkUnits: 1388,
			},
			{
				Tenant:              "tenant-b",
				Weight:              3,
				Submitted:           13,
				Admitted:            12,
				Rejected:            1,
				Completed:           10,
				Canceled:            1,
				DeadlineExceeded:    1,
				DispatchedRequests:  10,
				DispatchedWorkUnits: 1331,
			},
		},
		Service: saturationVisualService{
			Oracle:           "independent bounded weighted deficit round-robin state machine",
			OracleMatch:      true,
			Expected:         append([]saturationVisualDispatch(nil), dispatch...),
			Observed:         append([]saturationVisualDispatch(nil), dispatch...),
			UpstreamRequests: 20,
		},
		Timing: saturationVisualTiming{
			Clock:             "process monotonic intervals",
			OneRunDiagnostics: true,
			Boundary: "submit-to-dispatch includes the deliberate control hold plus ingress, " +
				"queue, and scheduler time",
			Samples: timings,
		},
		Categorical: saturationVisualDigest{
			Projection:       "seeded accounting and dispatch trace",
			DigestAlgorithm:  "SHA-256",
			Digest:           visualSaturationDigest,
			ExcludesTiming:   true,
			ByteReproducible: true,
		},
	}
	if digest, err := saturationVisualProjectionDigest(
		saturationVisualProjectionFromReport(report),
	); err != nil || !strings.EqualFold(digest, visualSaturationDigest) {
		panic("invalid saturation visual fixture")
	}
	return report
}
