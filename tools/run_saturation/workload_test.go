package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSeededWorkloadIsDeterministicBoundedAndContentFree(t *testing.T) {
	t.Parallel()
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	first, err := buildSeededWorkload(20260725, profile)
	if err != nil {
		t.Fatalf("buildSeededWorkload(first) error = %v", err)
	}
	second, err := buildSeededWorkload(20260725, profile)
	if err != nil {
		t.Fatalf("buildSeededWorkload(second) error = %v", err)
	}
	other, err := buildSeededWorkload(20260726, profile)
	if err != nil {
		t.Fatalf("buildSeededWorkload(other) error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("the same seed produced different workloads")
	}
	if reflect.DeepEqual(first, other) {
		t.Fatal("different seeds produced the same workload")
	}

	seen := make(map[uint64]struct{}, ciTotalRequestCount)
	for identifier, job := range first.jobs {
		if _, duplicate := seen[identifier]; duplicate {
			t.Fatalf("duplicate job identifier %d", identifier)
		}
		seen[identifier] = struct{}{}
		if identifier == 0 || identifier > ciMaximumCompletionTokens {
			t.Fatalf("job identifier %d exceeds the bounded completion range", identifier)
		}
		if len(job.body) > ciMaximumBodyBytes ||
			job.workUnits > ciMaximumRequestUnits {
			t.Fatalf("job %d exceeds its bounded envelope", identifier)
		}
		if !bytes.Contains(job.body, []byte(`"content":""`)) {
			t.Fatalf("job %d contains nonempty message content", identifier)
		}
		for _, forbidden := range []string{
			"Bearer ",
			"Authorization",
			"api.",
			"/v1/",
			"sk-",
			"github_",
		} {
			if strings.Contains(string(job.body), forbidden) {
				t.Fatalf("job %d contains forbidden fixture text", identifier)
			}
		}
	}
	if len(seen) != ciTotalRequestCount {
		t.Fatalf("jobs = %d, want %d", len(seen), ciTotalRequestCount)
	}
}

func TestIndependentWDRROraclePreservesAnOpenVisit(t *testing.T) {
	t.Parallel()
	workload := seededWorkload{
		jobs: make(map[uint64]saturationJob, 8),
		serviceQueues: [len(serviceTenants)][]uint64{
			{1, 2, 3, 4},
			{5, 6, 7, 8},
		},
	}
	for identifier := uint64(1); identifier <= 8; identifier++ {
		tenant := serviceTenants[0]
		if identifier >= 5 {
			tenant = serviceTenants[1]
		}
		workload.jobs[identifier] = saturationJob{
			id:        identifier,
			tenant:    tenant,
			kind:      jobService,
			workUnits: 100,
		}
	}
	trace, err := expectedDispatchTrace(workload)
	if err != nil {
		t.Fatalf("expectedDispatchTrace() error = %v", err)
	}
	got := make([]string, len(trace))
	for index, record := range trace {
		got[index] = record.Tenant
	}
	want := []string{
		"tenant-a",
		"tenant-b",
		"tenant-b",
		"tenant-b",
		"tenant-a",
		"tenant-b",
		"tenant-a",
		"tenant-a",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("oracle trace = %v, want %v", got, want)
	}
}

func TestSeededWorkloadOracleAcceptsBoundarySeeds(t *testing.T) {
	t.Parallel()
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	for _, seed := range []uint64{0, 1, ^uint64(0)} {
		workload, buildErr := buildSeededWorkload(seed, profile)
		if buildErr != nil {
			t.Fatalf("buildSeededWorkload(%d) error = %v", seed, buildErr)
		}
		trace, traceErr := expectedDispatchTrace(workload)
		if traceErr != nil {
			t.Fatalf("expectedDispatchTrace(seed %d) error = %v", seed, traceErr)
		}
		if len(trace) != ciServiceRequestsPerTenant*len(serviceTenants) {
			t.Fatalf("seed %d trace length = %d", seed, len(trace))
		}
	}
}

func TestSeededWorkloadValidationRejectsBrokenAccounting(t *testing.T) {
	t.Parallel()
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*seededWorkload)
	}{
		{
			name: "phase duplicate",
			mutate: func(workload *seededWorkload) {
				workload.cancellations[0] = workload.initialService[0]
			},
		},
		{
			name: "service queue order",
			mutate: func(workload *seededWorkload) {
				queue := workload.serviceQueues[0]
				queue[0], queue[1] = queue[1], queue[0]
			},
		},
		{
			name: "kind mismatch",
			mutate: func(workload *seededWorkload) {
				identifier := workload.deadlines[0]
				job := workload.jobs[identifier]
				job.kind = jobCancel
				workload.jobs[identifier] = job
			},
		},
		{
			name: "global probe mismatch",
			mutate: func(workload *seededWorkload) {
				job := workload.jobs[workload.globalRejection]
				job.kind = jobReject
				workload.jobs[workload.globalRejection] = job
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workload, buildErr := buildSeededWorkload(20260725, profile)
			if buildErr != nil {
				t.Fatalf("buildSeededWorkload() error = %v", buildErr)
			}
			test.mutate(&workload)
			if validationErr := validateSeededWorkload(workload); validationErr == nil {
				t.Fatal("validateSeededWorkload() accepted corrupt accounting")
			}
		})
	}
}

func TestProfileRejectsUnboundedVariants(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "local", "benchmark", "CI"} {
		if _, err := profileByName(name); err == nil {
			t.Fatalf("profileByName(%q) unexpectedly succeeded", name)
		}
	}
}

func TestCIProfileHasSeparateFiniteExecutionAndCleanupEnvelopes(t *testing.T) {
	t.Parallel()
	profile, err := profileByName(ciProfileName)
	if err != nil {
		t.Fatalf("profileByName() error = %v", err)
	}
	timeouts := []time.Duration{
		profile.executionTimeout,
		profile.gracefulCleanupTimeout,
		profile.abortCleanupTimeout,
		profile.upstreamAbortTimeout,
		profile.runtimeAbortTimeout,
		profile.queueDeadline,
		profile.defaultQueueTimeout,
		profile.bodyReadTimeout,
		profile.upstreamTimeout,
		profile.streamReadTimeout,
		profile.streamEventTimeout,
		profile.responseWriteTimeout,
		profile.graceTimeout,
		profile.forceTimeout,
	}
	for index, timeout := range timeouts {
		if timeout <= 0 {
			t.Fatalf("timeout %d = %s, want a finite positive bound", index, timeout)
		}
	}
	if profile.executionTimeout != 30*time.Second ||
		profile.gracefulCleanupTimeout != 9*time.Second ||
		profile.abortCleanupTimeout != 10*time.Second ||
		profile.gracefulCleanupTimeout !=
			profile.graceTimeout+2*profile.forceTimeout ||
		profile.abortCleanupTimeout !=
			profile.upstreamAbortTimeout+profile.runtimeAbortTimeout ||
		profile.queueDeadline >= profile.defaultQueueTimeout ||
		profile.streamReadTimeout > profile.streamEventTimeout ||
		profile.streamEventTimeout > profile.upstreamTimeout {
		t.Fatal("CI profile timeout envelopes are unsafe or no longer explicit")
	}
}
