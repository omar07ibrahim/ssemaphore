package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"time"
)

const (
	visualSaturationSeed           uint64 = 20260725
	visualSaturationProfile               = "ci"
	visualSaturationSchema                = "ssemaphore.saturation-evidence.v1"
	visualSaturationScope                 = "one bounded synthetic numeric-loopback request-count saturation run"
	visualSaturationDigest                = "7b6b4a3076eb811cd3c463e5fa6cee313769d4a1941242093d384f11ebce127f"
	visualSaturationOverallTimeout        = 90 * time.Second
	visualSaturationProcessTimeout        = 60 * time.Second
	visualSaturationBuildTimeout          = 25 * time.Second
	visualSaturationMaxStdout             = 128 << 10
	visualSaturationMaxStderr             = 8 << 10
	visualSaturationMaximumJobs    uint64 = 28
)

var errSaturationVisual = errors.New("saturation visual evidence failed")

type saturationVisualEvidence struct {
	Schema                    string                     `json:"schema"`
	Scope                     string                     `json:"scope"`
	Toolchain                 string                     `json:"toolchain"`
	Platform                  string                     `json:"platform"`
	Architecture              string                     `json:"architecture"`
	PerformanceClaim          bool                       `json:"performance_claim"`
	DiagnosticTimingsIncluded bool                       `json:"diagnostic_timings_included"`
	Projection                saturationVisualProjection `json:"categorical_projection"`
	Categorical               saturationVisualDigest     `json:"categorical_evidence"`
}

type saturationVisualReport struct {
	SchemaVersion   uint64                     `json:"schema_version"`
	Profile         string                     `json:"profile"`
	Seed            uint64                     `json:"seed"`
	GoVersion       string                     `json:"go_version"`
	OperatingSystem string                     `json:"operating_system"`
	Architecture    string                     `json:"architecture"`
	Claims          saturationVisualClaims     `json:"claims"`
	Topology        saturationVisualTopology   `json:"topology"`
	ServiceTotals   saturationVisualOutcomes   `json:"service_totals"`
	Control         saturationVisualControl    `json:"control"`
	CapacityProbes  saturationVisualProbes     `json:"capacity_probes"`
	Accounting      saturationVisualAccounting `json:"accounting"`
	Timeouts        saturationVisualTimeouts   `json:"timeout_envelopes"`
	Tenants         []saturationVisualTenant   `json:"tenants"`
	Service         saturationVisualService    `json:"service"`
	Timing          saturationVisualTiming     `json:"timing_hooks"`
	Categorical     saturationVisualDigest     `json:"categorical_evidence"`
}

type saturationVisualProjection struct {
	SchemaVersion  uint64                     `json:"schema_version"`
	Profile        string                     `json:"profile"`
	Seed           uint64                     `json:"seed"`
	Topology       saturationVisualTopology   `json:"topology"`
	ServiceTotals  saturationVisualOutcomes   `json:"service_totals"`
	Control        saturationVisualControl    `json:"control"`
	CapacityProbes saturationVisualProbes     `json:"capacity_probes"`
	Accounting     saturationVisualAccounting `json:"accounting"`
	Timeouts       saturationVisualTimeouts   `json:"timeout_envelopes"`
	Tenants        []saturationVisualTenant   `json:"tenants"`
	Service        saturationVisualService    `json:"service"`
}

type saturationVisualClaims struct {
	Performance            bool   `json:"performance"`
	ReportByteReproducible bool   `json:"report_byte_reproducible"`
	Scope                  string `json:"scope"`
	Timings                string `json:"timings"`
	Streaming              string `json:"streaming"`
	Encoding               string `json:"encoding"`
}

type saturationVisualTopology struct {
	Gateway                   string `json:"gateway"`
	Transport                 string `json:"transport"`
	ControlledUpstream        bool   `json:"controlled_upstream"`
	SeededArrivalOrder        bool   `json:"seeded_arrival_order"`
	SchedulerSnapshotBarriers bool   `json:"scheduler_snapshot_barriers"`
	GlobalInflightRequests    uint64 `json:"global_inflight_requests"`
	GlobalQueuedRequests      uint64 `json:"global_queued_requests"`
	ServiceRequestsPerTenant  uint64 `json:"service_requests_per_tenant"`
}

type saturationVisualOutcomes struct {
	Submitted        uint64 `json:"submitted"`
	Admitted         uint64 `json:"admitted"`
	Rejected         uint64 `json:"rejected"`
	Completed        uint64 `json:"completed"`
	Canceled         uint64 `json:"canceled"`
	DeadlineExceeded uint64 `json:"deadline_exceeded"`
}

type saturationVisualControl struct {
	Submitted        uint64 `json:"submitted"`
	Admitted         uint64 `json:"admitted"`
	Completed        uint64 `json:"completed"`
	UpstreamRequests uint64 `json:"upstream_requests"`
}

type saturationVisualProbe struct {
	Scope            string `json:"scope"`
	Submitted        uint64 `json:"submitted"`
	Rejected         uint64 `json:"rejected"`
	StatusCode       uint64 `json:"status_code"`
	ErrorCode        string `json:"error_code"`
	UpstreamRequests uint64 `json:"upstream_requests"`
}

type saturationVisualProbes struct {
	Tenant saturationVisualProbe `json:"tenant_capacity"`
	Global saturationVisualProbe `json:"global_capacity"`
}

type saturationVisualAccounting struct {
	TotalJobs              uint64 `json:"total_jobs"`
	ServiceSubmissions     uint64 `json:"service_submissions"`
	ControlSubmissions     uint64 `json:"control_submissions"`
	GlobalProbeSubmissions uint64 `json:"global_probe_submissions"`
	TotalUpstreamRequests  uint64 `json:"total_upstream_requests"`
	Reconciled             bool   `json:"reconciled"`
	Boundary               string `json:"boundary"`
}

type saturationVisualTimeouts struct {
	ExecutionContextMS      uint64 `json:"execution_context_ms"`
	GracefulAttemptMS       uint64 `json:"graceful_cleanup_attempt_ms"`
	AbortReserveMS          uint64 `json:"abort_cleanup_reserve_ms"`
	MaximumCleanupMS        uint64 `json:"maximum_cleanup_ms"`
	SeparateCleanupEnvelope bool   `json:"separate_cleanup_envelope"`
	Boundary                string `json:"boundary"`
}

type saturationVisualTenant struct {
	Tenant              string `json:"tenant"`
	Weight              uint64 `json:"weight"`
	Submitted           uint64 `json:"submitted"`
	Admitted            uint64 `json:"admitted"`
	Rejected            uint64 `json:"rejected"`
	Completed           uint64 `json:"completed"`
	Canceled            uint64 `json:"canceled"`
	DeadlineExceeded    uint64 `json:"deadline_exceeded"`
	DispatchedRequests  uint64 `json:"dispatched_requests"`
	DispatchedWorkUnits uint64 `json:"dispatched_work_units"`
}

type saturationVisualDispatch struct {
	Position  uint64 `json:"position"`
	Tenant    string `json:"tenant"`
	Weight    uint64 `json:"weight"`
	WorkUnits uint64 `json:"work_units"`
	Mode      string `json:"mode"`
}

type saturationVisualService struct {
	Oracle           string                     `json:"oracle"`
	OracleMatch      bool                       `json:"oracle_match"`
	Expected         []saturationVisualDispatch `json:"expected_dispatch"`
	Observed         []saturationVisualDispatch `json:"observed_dispatch"`
	UpstreamRequests uint64                     `json:"upstream_requests"`
}

type saturationVisualTimingSample struct {
	Position                    uint64  `json:"position"`
	Tenant                      string  `json:"tenant"`
	Mode                        string  `json:"mode"`
	SubmitToUpstreamDispatchNS  uint64  `json:"submit_to_upstream_dispatch_ns"`
	UpstreamReleaseToSSEEventNS *uint64 `json:"upstream_release_to_first_sse_event_ns"`
}

type saturationVisualTiming struct {
	Clock             string                         `json:"clock"`
	Thresholds        bool                           `json:"thresholds"`
	OneRunDiagnostics bool                           `json:"one_run_diagnostics"`
	ByteReproducible  bool                           `json:"byte_reproducible"`
	Boundary          string                         `json:"boundary"`
	Samples           []saturationVisualTimingSample `json:"samples"`
}

type saturationVisualDigest struct {
	Projection       string `json:"projection"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	Digest           string `json:"digest"`
	ExcludesTiming   bool   `json:"excludes_timing"`
	ByteReproducible bool   `json:"byte_reproducible"`
}

type saturationVisualBoundedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (buffer *saturationVisualBoundedBuffer) Write(payload []byte) (int, error) {
	if buffer == nil || buffer.limit < 0 {
		return 0, errSaturationVisual
	}
	remaining := buffer.limit - buffer.buffer.Len()
	if remaining < len(payload) {
		buffer.exceeded = true
		if remaining > 0 {
			_, _ = buffer.buffer.Write(payload[:remaining])
		}
		return len(payload), nil
	}
	_, _ = buffer.buffer.Write(payload)
	return len(payload), nil
}

func (buffer *saturationVisualBoundedBuffer) Bytes() []byte {
	if buffer == nil {
		return nil
	}
	return buffer.buffer.Bytes()
}

func runSaturationVisual(
	parent context.Context,
	root string,
) (result saturationVisualEvidence, resultErr error) {
	if parent == nil || parent.Err() != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	privateDirectory, err := os.MkdirTemp(absoluteRoot, ".saturation-visual-")
	if err != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	defer func() {
		if cleanupErr := os.RemoveAll(privateDirectory); cleanupErr != nil {
			result = saturationVisualEvidence{}
			resultErr = errSaturationVisual
		}
	}()
	if err := os.Chmod(privateDirectory, 0o700); err != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}

	binaryPath := filepath.Join(privateDirectory, "run-saturation")
	buildContext, cancelBuild := context.WithTimeout(parent, visualSaturationBuildTimeout)
	build := exec.CommandContext(
		buildContext,
		filepath.Join(runtime.GOROOT(), "bin", "go"),
		"build",
		"-trimpath",
		"-o",
		binaryPath,
		"./tools/run_saturation",
	)
	build.Dir = absoluteRoot
	build.Env = []string{
		"CGO_ENABLED=0",
		"GOCACHE=" + filepath.Join(privateDirectory, "go-cache"),
		"GOENV=off",
		"GOFLAGS=",
		"GOMODCACHE=" + filepath.Join(privateDirectory, "module-cache"),
		"GOPATH=" + filepath.Join(privateDirectory, "gopath"),
		"GOTOOLCHAIN=local",
		"GOWORK=off",
		"HOME=" + privateDirectory,
		"LANG=C",
		"LC_ALL=C",
	}
	buildStdout := &saturationVisualBoundedBuffer{limit: visualSaturationMaxStderr}
	buildStderr := &saturationVisualBoundedBuffer{limit: visualSaturationMaxStderr}
	build.Stdout = buildStdout
	build.Stderr = buildStderr
	buildErr := build.Run()
	cancelBuild()
	if buildErr != nil || buildStdout.exceeded || buildStderr.exceeded ||
		len(buildStdout.Bytes()) != 0 || len(buildStderr.Bytes()) != 0 ||
		build.ProcessState == nil || build.ProcessState.ExitCode() != 0 {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	info, err := os.Stat(binaryPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o100 == 0 {
		return saturationVisualEvidence{}, errSaturationVisual
	}

	runContext, cancelRun := context.WithTimeout(parent, visualSaturationProcessTimeout)
	command := exec.CommandContext(
		runContext,
		binaryPath,
		"--profile="+visualSaturationProfile,
		"--seed=20260725",
	)
	command.Dir = absoluteRoot
	command.Env = []string{"HOME=/nonexistent", "LANG=C", "LC_ALL=C"}
	stdout := &saturationVisualBoundedBuffer{limit: visualSaturationMaxStdout}
	stderr := &saturationVisualBoundedBuffer{limit: visualSaturationMaxStderr}
	command.Stdout = stdout
	command.Stderr = stderr
	runErr := command.Run()
	cancelRun()
	if runErr != nil || stdout.exceeded || stderr.exceeded ||
		len(stderr.Bytes()) != 0 || command.ProcessState == nil ||
		command.ProcessState.ExitCode() != 0 {
		return saturationVisualEvidence{}, errSaturationVisual
	}

	report, err := decodeSaturationVisualReport(stdout.Bytes())
	if err != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	evidence, err := saturationVisualEvidenceFromReport(report)
	if err != nil {
		return saturationVisualEvidence{}, errSaturationVisual
	}
	return evidence, nil
}

func decodeSaturationVisualReport(payload []byte) (saturationVisualReport, error) {
	if len(payload) == 0 || len(payload) > visualSaturationMaxStdout ||
		payload[len(payload)-1] != '\n' || bytes.Count(payload, []byte{'\n'}) != 1 {
		return saturationVisualReport{}, errSaturationVisual
	}
	if err := rejectSaturationVisualDuplicateKeys(payload); err != nil {
		return saturationVisualReport{}, errSaturationVisual
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var report saturationVisualReport
	if err := decoder.Decode(&report); err != nil {
		return saturationVisualReport{}, errSaturationVisual
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return saturationVisualReport{}, errSaturationVisual
	}
	if err := validateSaturationVisualReport(report); err != nil {
		return saturationVisualReport{}, err
	}
	return report, nil
}

func rejectSaturationVisualDuplicateKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return errSaturationVisual
	}
	if err := consumeSaturationVisualJSONValue(decoder, first); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errSaturationVisual
	}
	return nil
}

func consumeSaturationVisualJSONValue(
	decoder *json.Decoder,
	first json.Token,
) error {
	delimiter, composite := first.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return errSaturationVisual
			}
			key, ok := keyToken.(string)
			if !ok {
				return errSaturationVisual
			}
			if _, exists := keys[key]; exists {
				return errSaturationVisual
			}
			keys[key] = struct{}{}
			value, err := decoder.Token()
			if err != nil {
				return errSaturationVisual
			}
			if err := consumeSaturationVisualJSONValue(decoder, value); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errSaturationVisual
		}
	case '[':
		for decoder.More() {
			value, err := decoder.Token()
			if err != nil {
				return errSaturationVisual
			}
			if err := consumeSaturationVisualJSONValue(decoder, value); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errSaturationVisual
		}
	default:
		return errSaturationVisual
	}
	return nil
}

func validateSaturationVisualReport(report saturationVisualReport) error {
	if report.SchemaVersion != 3 || report.Profile != visualSaturationProfile ||
		report.Seed != visualSaturationSeed ||
		report.GoVersion != visualExpectedGo ||
		report.OperatingSystem != visualExpectedOS ||
		report.Architecture != "amd64" ||
		report.Claims.Performance ||
		report.Claims.ReportByteReproducible ||
		report.Claims.Scope != visualSaturationScope ||
		report.Claims.Timings !=
			"diagnostic intervals without thresholds or service-level claims" ||
		report.Claims.Streaming !=
			"SSE wire-order observation only; no timeout or backpressure claim" ||
		report.Claims.Encoding != "compact JSON with stable struct field order" {
		return errSaturationVisual
	}
	if report.Topology != (saturationVisualTopology{
		Gateway:                   "production parser, scheduler, HTTP relay, and server",
		Transport:                 "HTTP/1 over numeric loopback",
		ControlledUpstream:        true,
		SeededArrivalOrder:        true,
		SchedulerSnapshotBarriers: true,
		GlobalInflightRequests:    1,
		GlobalQueuedRequests:      20,
		ServiceRequestsPerTenant:  10,
	}) {
		return errSaturationVisual
	}
	if report.ServiceTotals != (saturationVisualOutcomes{
		Submitted:        26,
		Admitted:         24,
		Rejected:         2,
		Completed:        20,
		Canceled:         2,
		DeadlineExceeded: 2,
	}) || report.Control != (saturationVisualControl{
		Submitted:        1,
		Admitted:         1,
		Completed:        1,
		UpstreamRequests: 1,
	}) {
		return errSaturationVisual
	}
	if report.CapacityProbes.Tenant.Submitted != 2 ||
		report.CapacityProbes.Tenant.Rejected != 2 ||
		report.CapacityProbes.Tenant.StatusCode != 429 ||
		report.CapacityProbes.Tenant.ErrorCode != "tenant_capacity_exhausted" ||
		report.CapacityProbes.Tenant.UpstreamRequests != 0 ||
		report.CapacityProbes.Global.Submitted != 1 ||
		report.CapacityProbes.Global.Rejected != 1 ||
		report.CapacityProbes.Global.StatusCode != 503 ||
		report.CapacityProbes.Global.ErrorCode != "overloaded" ||
		report.CapacityProbes.Global.UpstreamRequests != 0 {
		return errSaturationVisual
	}
	if report.Accounting.TotalJobs != visualSaturationMaximumJobs ||
		report.Accounting.ServiceSubmissions != 26 ||
		report.Accounting.ControlSubmissions != 1 ||
		report.Accounting.GlobalProbeSubmissions != 1 ||
		report.Accounting.TotalUpstreamRequests != 21 ||
		!report.Accounting.Reconciled ||
		report.Accounting.Boundary !=
			"service_totals include two tenant-capacity probes; control and the global-capacity probe are separate" {
		return errSaturationVisual
	}
	if report.Timeouts != (saturationVisualTimeouts{
		ExecutionContextMS:      30_000,
		GracefulAttemptMS:       9_000,
		AbortReserveMS:          10_000,
		MaximumCleanupMS:        19_000,
		SeparateCleanupEnvelope: true,
		Boundary: "the execution context and subsequent cleanup waits use separate bounded envelopes, " +
			"not one wall-clock deadline",
	}) {
		return errSaturationVisual
	}
	if len(report.Tenants) != 2 ||
		report.Tenants[0].Tenant != "tenant-a" ||
		report.Tenants[0].Weight != 1 ||
		report.Tenants[1].Tenant != "tenant-b" ||
		report.Tenants[1].Weight != 3 {
		return errSaturationVisual
	}
	for _, tenant := range report.Tenants {
		if tenant.Submitted != 13 || tenant.Admitted != 12 ||
			tenant.Rejected != 1 || tenant.Completed != 10 ||
			tenant.Canceled != 1 || tenant.DeadlineExceeded != 1 ||
			tenant.DispatchedRequests != 10 ||
			tenant.DispatchedWorkUnits == 0 ||
			tenant.Submitted != tenant.Admitted+tenant.Rejected ||
			tenant.Admitted !=
				tenant.Completed+tenant.Canceled+tenant.DeadlineExceeded {
			return errSaturationVisual
		}
	}
	if report.Service.Oracle !=
		"independent bounded weighted deficit round-robin state machine" ||
		!report.Service.OracleMatch ||
		len(report.Service.Expected) != 20 ||
		!reflect.DeepEqual(report.Service.Expected, report.Service.Observed) ||
		report.Service.UpstreamRequests != 20 ||
		len(report.Timing.Samples) != 20 ||
		report.Timing.Clock != "process monotonic intervals" ||
		report.Timing.Thresholds ||
		!report.Timing.OneRunDiagnostics ||
		report.Timing.ByteReproducible ||
		report.Timing.Boundary !=
			"submit-to-dispatch includes the deliberate control hold plus ingress, queue, and scheduler time" {
		return errSaturationVisual
	}
	for index, dispatch := range report.Service.Observed {
		if dispatch.Position != uint64(index+1) ||
			dispatch.WorkUnits == 0 || dispatch.WorkUnits > 320 ||
			dispatch.Tenant != "tenant-a" && dispatch.Tenant != "tenant-b" ||
			dispatch.Weight != 1 && dispatch.Weight != 3 ||
			dispatch.Mode != "buffered" && dispatch.Mode != "sse" {
			return errSaturationVisual
		}
		sample := report.Timing.Samples[index]
		if sample.Position != dispatch.Position ||
			sample.Tenant != dispatch.Tenant ||
			sample.Mode != dispatch.Mode ||
			dispatch.Mode == "sse" && sample.UpstreamReleaseToSSEEventNS == nil ||
			dispatch.Mode != "sse" && sample.UpstreamReleaseToSSEEventNS != nil {
			return errSaturationVisual
		}
	}
	projection := saturationVisualProjectionFromReport(report)
	digest, err := saturationVisualProjectionDigest(projection)
	if err != nil ||
		report.Categorical != (saturationVisualDigest{
			Projection:       "seeded accounting and dispatch trace",
			DigestAlgorithm:  "SHA-256",
			Digest:           visualSaturationDigest,
			ExcludesTiming:   true,
			ByteReproducible: true,
		}) ||
		digest != report.Categorical.Digest {
		return errSaturationVisual
	}
	return nil
}

func saturationVisualProjectionFromReport(
	report saturationVisualReport,
) saturationVisualProjection {
	return saturationVisualProjection{
		SchemaVersion:  report.SchemaVersion,
		Profile:        report.Profile,
		Seed:           report.Seed,
		Topology:       report.Topology,
		ServiceTotals:  report.ServiceTotals,
		Control:        report.Control,
		CapacityProbes: report.CapacityProbes,
		Accounting:     report.Accounting,
		Timeouts:       report.Timeouts,
		Tenants:        append([]saturationVisualTenant(nil), report.Tenants...),
		Service: saturationVisualService{
			Oracle:           report.Service.Oracle,
			OracleMatch:      report.Service.OracleMatch,
			Expected:         append([]saturationVisualDispatch(nil), report.Service.Expected...),
			Observed:         append([]saturationVisualDispatch(nil), report.Service.Observed...),
			UpstreamRequests: report.Service.UpstreamRequests,
		},
	}
}

func saturationVisualProjectionDigest(
	projection saturationVisualProjection,
) (string, error) {
	payload, err := json.Marshal(projection)
	if err != nil {
		return "", errSaturationVisual
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func saturationVisualEvidenceFromReport(
	report saturationVisualReport,
) (saturationVisualEvidence, error) {
	if err := validateSaturationVisualReport(report); err != nil {
		return saturationVisualEvidence{}, err
	}
	evidence := saturationVisualEvidence{
		Schema:                    visualSaturationSchema,
		Scope:                     report.Claims.Scope,
		Toolchain:                 report.GoVersion,
		Platform:                  report.OperatingSystem,
		Architecture:              report.Architecture,
		PerformanceClaim:          false,
		DiagnosticTimingsIncluded: false,
		Projection:                saturationVisualProjectionFromReport(report),
		Categorical:               report.Categorical,
	}
	if err := validateSaturationVisualEvidence(evidence); err != nil {
		return saturationVisualEvidence{}, err
	}
	return evidence, nil
}

func validateSaturationVisualEvidence(evidence saturationVisualEvidence) error {
	if evidence.Schema != visualSaturationSchema ||
		evidence.Scope != visualSaturationScope ||
		evidence.Toolchain != visualExpectedGo ||
		evidence.Platform != visualExpectedOS ||
		evidence.Architecture != "amd64" ||
		evidence.PerformanceClaim ||
		evidence.DiagnosticTimingsIncluded ||
		evidence.Projection.SchemaVersion != 3 ||
		evidence.Projection.Profile != visualSaturationProfile ||
		evidence.Projection.Seed != visualSaturationSeed ||
		!evidence.Projection.Accounting.Reconciled ||
		!evidence.Projection.Service.OracleMatch ||
		!reflect.DeepEqual(
			evidence.Projection.Service.Expected,
			evidence.Projection.Service.Observed,
		) ||
		evidence.Categorical != (saturationVisualDigest{
			Projection:       "seeded accounting and dispatch trace",
			DigestAlgorithm:  "SHA-256",
			Digest:           visualSaturationDigest,
			ExcludesTiming:   true,
			ByteReproducible: true,
		}) {
		return errSaturationVisual
	}
	digest, err := saturationVisualProjectionDigest(evidence.Projection)
	if err != nil || digest != evidence.Categorical.Digest {
		return errSaturationVisual
	}
	return nil
}
