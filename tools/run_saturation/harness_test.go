package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

func TestCIHarnessExercisesTheProductionGateway(t *testing.T) {
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), profile.executionTimeout)
	defer cancel()

	report, err := runSaturationHarness(ctx, profile, 20260725)
	if err != nil {
		t.Fatalf("runSaturationHarness() error = %v", err)
	}
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() error = %v", err)
	}
	if report.Claims.Performance || report.Claims.ReportByteReproducible ||
		report.Timing.Thresholds || report.Timing.ByteReproducible ||
		!report.Timing.OneRunDiagnostics ||
		report.Claims.Streaming != reportStreamingClaim {
		t.Fatal("loopback harness misstated its performance or reproducibility boundary")
	}
	if report.ServiceTotals != (outcomeCounts{
		Submitted:        26,
		Admitted:         24,
		Rejected:         2,
		Completed:        20,
		Canceled:         2,
		DeadlineExceeded: 2,
	}) {
		t.Fatalf("service totals = %+v", report.ServiceTotals)
	}
	if !report.Accounting.Reconciled ||
		report.Accounting.TotalJobs != ciTotalRequestCount ||
		report.Accounting.ServiceSubmissions != 26 ||
		report.Accounting.ControlSubmissions != 1 ||
		report.Accounting.GlobalProbeSubmissions != 1 ||
		report.Accounting.TotalUpstreamRequests != 21 ||
		report.Control != (controlReport{
			Submitted:        1,
			Admitted:         1,
			Completed:        1,
			UpstreamRequests: 1,
		}) ||
		report.CapacityProbes.Global != (publicErrorProbeReport{
			Scope:            globalProbeScope,
			Submitted:        1,
			Rejected:         1,
			StatusCode:       http.StatusServiceUnavailable,
			ErrorCode:        "overloaded",
			UpstreamRequests: 0,
		}) ||
		report.Timeouts.MaximumCleanupMS != 19_000 ||
		!report.Timeouts.SeparateCleanupEnvelope {
		t.Fatalf(
			"accounting boundary = (%+v, %+v, %+v), want exact 28-job reconciliation",
			report.Accounting,
			report.Control,
			report.CapacityProbes,
		)
	}
	if !reflect.DeepEqual(report.Service.Expected, report.Service.Observed) {
		t.Fatal("observed service trace differs from the independent oracle")
	}

	encoded, err := marshalReport(report)
	if err != nil {
		t.Fatalf("marshalReport() error = %v", err)
	}
	encodedAgain, err := marshalReport(report)
	if err != nil || !bytes.Equal(encoded, encodedAgain) ||
		len(encoded) == 0 || encoded[len(encoded)-1] != '\n' ||
		bytes.Count(encoded, []byte{'\n'}) != 1 {
		t.Fatal("one report did not use its exact compact stable encoding")
	}
	var decoded saturationReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(report, decoded) {
		t.Fatal("report did not round-trip exactly")
	}

	altered := report
	altered.Timing.Samples = append([]timingSample(nil), report.Timing.Samples...)
	altered.Timing.Samples[0].SubmitToUpstreamDispatchNS++
	alteredEncoded, err := marshalReport(altered)
	if err != nil {
		t.Fatalf("marshalReport(altered timing) error = %v", err)
	}
	if bytes.Equal(encoded, alteredEncoded) {
		t.Fatal("whole report bytes ignored a changed one-run timing")
	}
	alteredDigest, err := categoricalDigest(altered)
	if err != nil {
		t.Fatalf("categoricalDigest(altered timing) error = %v", err)
	}
	if alteredDigest != report.Categorical.Digest ||
		!report.Categorical.ExcludesTiming ||
		!report.Categorical.ByteReproducible {
		t.Fatal("categorical evidence changed with an excluded diagnostic timing")
	}
	corrupt := report
	corrupt.Tenants = append([]tenantReport(nil), report.Tenants...)
	corrupt.Tenants[0].Completed++
	if _, corruptErr := marshalReport(corrupt); !errors.Is(corruptErr, errHarnessReport) {
		t.Fatalf("marshalReport(corrupt accounting) error = %v", corruptErr)
	}
	for _, forbidden := range []string{
		syntheticModel,
		controlTenant.token,
		globalProbeTenant.token,
		serviceTenants[0].token,
		serviceTenants[1].token,
		upstreamCredential,
		"Authorization",
		chatCompletionsPath,
		"127.0.0.1",
		"http://",
		"https://",
		`"messages"`,
		`"content"`,
		`"benchmark"`,
		`"requests_per_second"`,
		`"rps"`,
		`"percentile"`,
		`"rss_bytes"`,
		`"throughput"`,
	} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("report contains forbidden runtime data")
		}
	}
}

func TestCIHarnessRunsAnAlternateSeedThroughProduction(t *testing.T) {
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), profile.executionTimeout)
	defer cancel()

	report, err := runSaturationHarness(ctx, profile, 0)
	if err != nil {
		t.Fatalf("runSaturationHarness(alternate seed) error = %v", err)
	}
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport(alternate seed) error = %v", err)
	}
	if report.Seed != 0 || !report.Service.OracleMatch ||
		!report.Accounting.Reconciled ||
		report.CapacityProbes.Tenant.StatusCode != http.StatusTooManyRequests ||
		report.CapacityProbes.Global.StatusCode != http.StatusServiceUnavailable ||
		report.Categorical.Digest == "" {
		t.Fatal("alternate-seed production run did not preserve categorical invariants")
	}
}

func TestHarnessRejectsAContextThatCannotOwnTheRun(t *testing.T) {
	t.Parallel()
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{nil, canceled} {
		if _, runErr := runSaturationHarness(ctx, profile, 1); !errors.Is(runErr, errHarnessContext) {
			t.Fatalf("runSaturationHarness() error = %v, want context rejection", runErr)
		}
	}
}

func TestCommandParsingIsClosedAndSeedExplicit(t *testing.T) {
	t.Parallel()
	valid := [][]string{
		{"--profile=ci", "--seed=0"},
		{"--seed=18446744073709551615", "--profile=ci"},
	}
	for _, args := range valid {
		_, help, ok := parseArguments(args)
		if help || !ok {
			t.Fatalf("parseArguments(%v) = (_, %t, %t)", args, help, ok)
		}
	}
	invalid := [][]string{
		nil,
		{"--profile=ci"},
		{"--seed=1"},
		{"--profile=local", "--seed=1"},
		{"--profile=ci", "--seed=-1"},
		{"--profile=ci", "--seed=+1"},
		{"--profile=ci", "--seed=1", "--endpoint=http://127.0.0.1"},
		{"--profile=ci", "--profile=ci"},
		{"--seed=1", "--seed=2"},
	}
	for _, args := range invalid {
		if _, help, ok := parseArguments(args); help || ok {
			t.Fatalf("parseArguments(%v) unexpectedly succeeded", args)
		}
	}
	if _, help, ok := parseArguments([]string{"--help"}); !help || !ok {
		t.Fatal("parseArguments(--help) did not select static help")
	}
}

func TestCommandRejectsInvalidInputWithoutEchoingIt(t *testing.T) {
	t.Parallel()
	const canary = "PRIVATE_COMMAND_CANARY"
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := runCommand(
		[]string{"--profile=ci", "--seed=" + canary},
		&stdout,
		&stderr,
	)
	if exitCode != 2 || stdout.Len() != 0 ||
		strings.Contains(stderr.String(), canary) ||
		stderr.String() != saturationUsage {
		t.Fatalf(
			"invalid command = (exit %d, stdout %q, stderr %q)",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
}

func TestProductionSchedulerReturnsTheRealQueueDeadlineDecision(t *testing.T) {
	scheduler, err := admission.New(saturationAdmissionConfig())
	if err != nil {
		t.Fatalf("admission.New() error = %v", err)
	}
	t.Cleanup(func() {
		closeScheduler(scheduler)
	})

	blocker, decision := scheduler.Acquire(context.Background(), admission.Admission{
		Tenant:       controlTenant.id,
		BodyBytes:    1,
		WorkUnits:    1,
		QueueTimeout: time.Second,
	})
	if blocker == nil || decision.Kind != admission.DecisionDispatched {
		t.Fatalf("blocking Acquire() = (%v, %+v)", blocker, decision)
	}
	defer blocker.Finish(admission.ServingCompleted)

	type result struct {
		permit   *admission.Permit
		decision admission.Decision
	}
	results := make(chan result, 1)
	go func() {
		permit, got := scheduler.Acquire(
			context.Background(),
			admission.Admission{
				Tenant:       tenantAID,
				BodyBytes:    1,
				WorkUnits:    1,
				QueueTimeout: 50 * time.Millisecond,
			},
		)
		results <- result{permit: permit, decision: got}
	}()

	select {
	case got := <-results:
		if got.permit != nil ||
			got.decision.Kind != admission.DecisionQueueExpired {
			t.Fatalf("deadline Acquire() = (%v, %+v)", got.permit, got.decision)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline Acquire() did not return")
	}
}

type deadlineNeverUpstream struct {
	calls atomic.Uint64
}

func (u *deadlineNeverUpstream) Complete(
	context.Context,
	contract.Request,
) (httpapi.UpstreamResponse, error) {
	u.calls.Add(1)
	return httpapi.UpstreamResponse{}, errors.New("unexpected upstream call")
}

func TestProductionHandlerMapsARealQueueDeadline(t *testing.T) {
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	parser, err := saturationParser()
	if err != nil {
		t.Fatalf("saturationParser() error = %v", err)
	}
	scheduler, err := admission.New(saturationAdmissionConfig())
	if err != nil {
		t.Fatalf("admission.New() error = %v", err)
	}
	t.Cleanup(func() {
		closeScheduler(scheduler)
	})
	upstream := &deadlineNeverUpstream{}
	handler, err := httpapi.NewHandler(
		saturationHTTPConfig(profile),
		parser,
		scheduler,
		upstream,
	)
	if err != nil {
		t.Fatalf("httpapi.NewHandler() error = %v", err)
	}

	blocker, decision := scheduler.Acquire(context.Background(), admission.Admission{
		Tenant:       controlTenant.id,
		BodyBytes:    1,
		WorkUnits:    1,
		QueueTimeout: time.Second,
	})
	if blocker == nil || decision.Kind != admission.DecisionDispatched {
		t.Fatalf("blocking Acquire() = (%v, %+v)", blocker, decision)
	}
	defer blocker.Finish(admission.ServingCompleted)

	workload, err := buildSeededWorkload(1, profile)
	if err != nil {
		t.Fatalf("buildSeededWorkload() error = %v", err)
	}
	job := workload.jobs[workload.deadlines[0]]
	request := httptest.NewRequest(
		http.MethodPost,
		chatCompletionsPath,
		bytes.NewReader(job.body),
	)
	request.Header.Set("Authorization", "Bearer "+job.tenant.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(queueTimeoutHeader, "50")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable ||
		recorder.Body.String() != queueDeadlineBody {
		t.Fatalf(
			"deadline response = (%d, %q), want exact 503 queue deadline",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	if upstream.calls.Load() != 0 {
		t.Fatalf("deadline request reached upstream %d times", upstream.calls.Load())
	}
}
