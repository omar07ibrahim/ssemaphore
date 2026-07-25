package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

const (
	queueTimeoutHeader = "X-Ssemaphore-Queue-Timeout-Ms"

	queueDeadlineBody  = `{"error":{"code":"queue_deadline_exceeded","message":"The request could not be admitted before its queue deadline."}}` + "\n"
	tenantRejectedBody = `{"error":{"code":"tenant_capacity_exhausted","message":"The tenant has no request capacity available."}}` + "\n"
	globalRejectedBody = `{"error":{"code":"overloaded","message":"The service has no global request capacity available."}}` + "\n"

	reportScope                  = "one bounded synthetic numeric-loopback request-count saturation run"
	reportTimingClaim            = "diagnostic intervals without thresholds or service-level claims"
	reportStreamingClaim         = "SSE wire-order observation only; no timeout or backpressure claim"
	reportEncoding               = "compact JSON with stable struct field order"
	accountingBoundary           = "service_totals include two tenant-capacity probes; control and the global-capacity probe are separate"
	timeoutBoundary              = "the execution context and subsequent cleanup waits use separate bounded envelopes, not one wall-clock deadline"
	tenantProbeScope             = "two saturated service tenants"
	globalProbeScope             = "dedicated tenant with available tenant capacity"
	serviceOracle                = "independent bounded weighted deficit round-robin state machine"
	timingClock                  = "process monotonic intervals"
	timingBoundary               = "submit-to-dispatch includes the deliberate control hold plus ingress, queue, and scheduler time"
	categoricalProjectionName    = "seeded accounting and dispatch trace"
	productionGatewayDescription = "production parser, scheduler, HTTP relay, and server"
	loopbackTransportDescription = "HTTP/1 over numeric loopback"
)

var (
	errHarnessContext            = errors.New("saturation harness requires a live context")
	errHarnessConstruction       = errors.New("saturation harness topology could not be constructed")
	errHarnessState              = errors.New("saturation harness observed an invalid state")
	errHarnessClient             = errors.New("saturation harness client exchange failed")
	errHarnessClientBuild        = errors.New("saturation harness could not build a client request")
	errHarnessClientIO           = errors.New("saturation harness client transport failed")
	errHarnessClientWire         = errors.New("saturation harness client observed an invalid response")
	errHarnessClientHeader       = errors.New("saturation harness client observed invalid response headers")
	errHarnessControlWire        = errors.New("saturation harness control response was invalid")
	errHarnessServiceWire        = errors.New("saturation harness service response was invalid")
	errHarnessDeadlineWire       = errors.New("saturation harness deadline response was invalid")
	errHarnessDeadlineCode       = errors.New("saturation harness deadline status was invalid")
	errHarnessDeadlineBody       = errors.New("saturation harness deadline envelope was invalid")
	errHarnessRejectedWire       = errors.New("saturation harness tenant rejection response was invalid")
	errHarnessGlobalRejectedWire = errors.New("saturation harness global rejection response was invalid")
	errHarnessShutdown           = errors.New("saturation harness did not shut down cleanly")
	errHarnessReport             = errors.New("saturation harness report is inconsistent")
)

type outcomeCounts struct {
	Submitted        uint64 `json:"submitted"`
	Admitted         uint64 `json:"admitted"`
	Rejected         uint64 `json:"rejected"`
	Completed        uint64 `json:"completed"`
	Canceled         uint64 `json:"canceled"`
	DeadlineExceeded uint64 `json:"deadline_exceeded"`
}

type tenantReport struct {
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

type timingSample struct {
	Position                    uint64  `json:"position"`
	Tenant                      string  `json:"tenant"`
	Mode                        string  `json:"mode"`
	SubmitToUpstreamDispatchNS  uint64  `json:"submit_to_upstream_dispatch_ns"`
	UpstreamReleaseToSSEEventNS *uint64 `json:"upstream_release_to_first_sse_event_ns"`
}

type reportClaims struct {
	Performance            bool   `json:"performance"`
	ReportByteReproducible bool   `json:"report_byte_reproducible"`
	Scope                  string `json:"scope"`
	Timings                string `json:"timings"`
	Streaming              string `json:"streaming"`
	Encoding               string `json:"encoding"`
}

type topologyReport struct {
	Gateway                   string `json:"gateway"`
	Transport                 string `json:"transport"`
	ControlledUpstream        bool   `json:"controlled_upstream"`
	SeededArrivalOrder        bool   `json:"seeded_arrival_order"`
	SchedulerSnapshotBarriers bool   `json:"scheduler_snapshot_barriers"`
	GlobalInflightRequests    uint64 `json:"global_inflight_requests"`
	GlobalQueuedRequests      uint64 `json:"global_queued_requests"`
	ServiceRequestsPerTenant  uint64 `json:"service_requests_per_tenant"`
}

type controlReport struct {
	Submitted        uint64 `json:"submitted"`
	Admitted         uint64 `json:"admitted"`
	Completed        uint64 `json:"completed"`
	UpstreamRequests uint64 `json:"upstream_requests"`
}

type accountingReport struct {
	TotalJobs              uint64 `json:"total_jobs"`
	ServiceSubmissions     uint64 `json:"service_submissions"`
	ControlSubmissions     uint64 `json:"control_submissions"`
	GlobalProbeSubmissions uint64 `json:"global_probe_submissions"`
	TotalUpstreamRequests  uint64 `json:"total_upstream_requests"`
	Reconciled             bool   `json:"reconciled"`
	Boundary               string `json:"boundary"`
}

type publicErrorProbeReport struct {
	Scope            string `json:"scope"`
	Submitted        uint64 `json:"submitted"`
	Rejected         uint64 `json:"rejected"`
	StatusCode       uint64 `json:"status_code"`
	ErrorCode        string `json:"error_code"`
	UpstreamRequests uint64 `json:"upstream_requests"`
}

type capacityProbeReport struct {
	Tenant publicErrorProbeReport `json:"tenant_capacity"`
	Global publicErrorProbeReport `json:"global_capacity"`
}

type timeoutEnvelopeReport struct {
	ExecutionContextMS      uint64 `json:"execution_context_ms"`
	GracefulAttemptMS       uint64 `json:"graceful_cleanup_attempt_ms"`
	AbortReserveMS          uint64 `json:"abort_cleanup_reserve_ms"`
	MaximumCleanupMS        uint64 `json:"maximum_cleanup_ms"`
	SeparateCleanupEnvelope bool   `json:"separate_cleanup_envelope"`
	Boundary                string `json:"boundary"`
}

type serviceReport struct {
	Oracle           string           `json:"oracle"`
	OracleMatch      bool             `json:"oracle_match"`
	Expected         []dispatchRecord `json:"expected_dispatch"`
	Observed         []dispatchRecord `json:"observed_dispatch"`
	UpstreamRequests uint64           `json:"upstream_requests"`
}

type timingReport struct {
	Clock             string         `json:"clock"`
	Thresholds        bool           `json:"thresholds"`
	OneRunDiagnostics bool           `json:"one_run_diagnostics"`
	ByteReproducible  bool           `json:"byte_reproducible"`
	Boundary          string         `json:"boundary"`
	Samples           []timingSample `json:"samples"`
}

type categoricalEvidence struct {
	Projection       string `json:"projection"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	Digest           string `json:"digest"`
	ExcludesTiming   bool   `json:"excludes_timing"`
	ByteReproducible bool   `json:"byte_reproducible"`
}

type saturationReport struct {
	SchemaVersion   uint64                `json:"schema_version"`
	Profile         string                `json:"profile"`
	Seed            uint64                `json:"seed"`
	GoVersion       string                `json:"go_version"`
	OperatingSystem string                `json:"operating_system"`
	Architecture    string                `json:"architecture"`
	Claims          reportClaims          `json:"claims"`
	Topology        topologyReport        `json:"topology"`
	ServiceTotals   outcomeCounts         `json:"service_totals"`
	Control         controlReport         `json:"control"`
	CapacityProbes  capacityProbeReport   `json:"capacity_probes"`
	Accounting      accountingReport      `json:"accounting"`
	Timeouts        timeoutEnvelopeReport `json:"timeout_envelopes"`
	Tenants         []tenantReport        `json:"tenants"`
	Service         serviceReport         `json:"service"`
	Timing          timingReport          `json:"timing_hooks"`
	Categorical     categoricalEvidence   `json:"categorical_evidence"`
}

type categoricalProjection struct {
	SchemaVersion  uint64                `json:"schema_version"`
	Profile        string                `json:"profile"`
	Seed           uint64                `json:"seed"`
	Topology       topologyReport        `json:"topology"`
	ServiceTotals  outcomeCounts         `json:"service_totals"`
	Control        controlReport         `json:"control"`
	CapacityProbes capacityProbeReport   `json:"capacity_probes"`
	Accounting     accountingReport      `json:"accounting"`
	Timeouts       timeoutEnvelopeReport `json:"timeout_envelopes"`
	Tenants        []tenantReport        `json:"tenants"`
	Service        serviceReport         `json:"service"`
}

type tenantAccumulator struct {
	definition          tenantDefinition
	counts              outcomeCounts
	dispatchedRequests  uint64
	dispatchedWorkUnits uint64
}

func runSaturationHarness(
	parent context.Context,
	profile saturationProfile,
	seed uint64,
) (saturationReport, error) {
	if parent == nil || parent.Err() != nil {
		return saturationReport{}, errHarnessContext
	}
	runContext, cancelRun := context.WithTimeout(
		parent,
		profile.executionTimeout,
	)
	workload, err := buildSeededWorkload(seed, profile)
	if err != nil {
		cancelRun()
		return saturationReport{}, err
	}
	upstream, err := startControlledUpstream(workload)
	if err != nil {
		cancelRun()
		return saturationReport{}, err
	}

	runtimeState, err := startGatewayRuntime(
		runContext,
		profile,
		upstream,
	)
	if err != nil {
		cancelRun()
		cleanupContext, cancelCleanup := context.WithTimeout(
			context.Background(),
			profile.abortCleanupTimeout,
		)
		abortSaturationRuntime(
			cleanupContext,
			profile,
			upstream,
			nil,
		)
		cancelCleanup()
		return saturationReport{}, err
	}

	cleanupComplete := false
	defer func() {
		cancelRun()
		if cleanupComplete {
			return
		}
		cleanupContext, cancelCleanup := context.WithTimeout(
			context.Background(),
			profile.abortCleanupTimeout,
		)
		abortSaturationRuntime(
			cleanupContext,
			profile,
			upstream,
			runtimeState,
		)
		cancelCleanup()
	}()

	report, err := exerciseSaturation(
		runContext,
		profile,
		workload,
		runtimeState,
		upstream,
	)
	if err != nil {
		return saturationReport{}, err
	}

	cleanupContext, cancelCleanup := context.WithTimeout(
		context.Background(),
		profile.gracefulCleanupTimeout+profile.abortCleanupTimeout,
	)
	gracefulContext, cancelGraceful := context.WithTimeout(
		cleanupContext,
		profile.gracefulCleanupTimeout,
	)
	runtimeErr := runtimeState.shutdown(gracefulContext)
	upstreamErr := error(nil)
	if runtimeErr == nil {
		upstreamErr = upstream.shutdown(gracefulContext)
	}
	cancelGraceful()
	if runtimeErr != nil || upstreamErr != nil {
		cancelRun()
		abortContext, cancelAbort := context.WithTimeout(
			cleanupContext,
			profile.abortCleanupTimeout,
		)
		failedRuntime := runtimeState
		if runtimeErr == nil {
			failedRuntime = nil
		}
		abortSaturationRuntime(
			abortContext,
			profile,
			upstream,
			failedRuntime,
		)
		cancelAbort()
		cancelCleanup()
		cleanupComplete = true
		if runtimeErr != nil {
			return saturationReport{}, runtimeErr
		}
		return saturationReport{}, upstreamErr
	}
	cancelCleanup()
	cleanupComplete = true
	return report, nil
}

func abortSaturationRuntime(
	ctx context.Context,
	profile saturationProfile,
	upstream *controlledUpstream,
	runtimeState *gatewayRuntime,
) {
	upstreamContext, cancelUpstream := context.WithTimeout(
		ctx,
		profile.upstreamAbortTimeout,
	)
	upstream.abort(upstreamContext)
	cancelUpstream()
	if runtimeState == nil {
		return
	}
	runtimeContext, cancelRuntime := context.WithTimeout(
		ctx,
		profile.runtimeAbortTimeout,
	)
	runtimeState.abort(runtimeContext)
	cancelRuntime()
}

func exerciseSaturation(
	ctx context.Context,
	profile saturationProfile,
	workload seededWorkload,
	runtimeState *gatewayRuntime,
	upstream *controlledUpstream,
) (saturationReport, error) {
	accumulators := [len(serviceTenants)]tenantAccumulator{}
	for index, tenant := range serviceTenants {
		accumulators[index].definition = tenant
	}

	controlJob := workload.jobs[workload.control]
	controlCall, err := runtimeState.launch(controlJob)
	if err != nil {
		return saturationReport{}, err
	}
	controlDispatch, err := awaitUpstreamDispatch(ctx, upstream)
	if err != nil {
		return saturationReport{}, err
	}
	if controlDispatch.job.id != controlJob.id ||
		controlDispatch.job.kind != jobControl {
		return saturationReport{}, errHarnessState
	}

	expectation := newQueueExpectation(controlJob)
	if err := waitForSnapshot(ctx, runtimeState.scheduler, expectation); err != nil {
		return saturationReport{}, err
	}

	for _, identifier := range workload.initialService {
		if err := enqueueObserved(
			ctx,
			runtimeState,
			workload.jobs[identifier],
			&expectation,
			&accumulators,
		); err != nil {
			return saturationReport{}, err
		}
	}
	for _, identifier := range workload.cancellations {
		if err := enqueueObserved(
			ctx,
			runtimeState,
			workload.jobs[identifier],
			&expectation,
			&accumulators,
		); err != nil {
			return saturationReport{}, err
		}
	}
	for _, identifier := range workload.deadlines {
		if err := enqueueObserved(
			ctx,
			runtimeState,
			workload.jobs[identifier],
			&expectation,
			&accumulators,
		); err != nil {
			return saturationReport{}, err
		}
	}

	for _, identifier := range workload.deadlines {
		call := runtimeState.calls[identifier]
		result, err := awaitClientResult(ctx, call)
		if err != nil {
			return saturationReport{}, err
		}
		if result.failure != nil {
			return saturationReport{}, result.failure
		}
		if !result.deadline {
			return saturationReport{}, errHarnessClient
		}
		job := workload.jobs[identifier]
		expectation.removeQueued(job)
		accumulator, ok := accumulatorFor(&accumulators, job.tenant.id)
		if !ok {
			return saturationReport{}, errHarnessState
		}
		accumulator.counts.DeadlineExceeded++
	}
	if err := waitForSnapshot(ctx, runtimeState.scheduler, expectation); err != nil {
		return saturationReport{}, err
	}

	for _, identifier := range workload.cancellations {
		call := runtimeState.calls[identifier]
		if call == nil || call.cancel == nil {
			return saturationReport{}, errHarnessState
		}
		call.cancel()
		result, err := awaitClientResult(ctx, call)
		if err != nil {
			return saturationReport{}, err
		}
		if result.failure != nil {
			return saturationReport{}, result.failure
		}
		if !result.canceled {
			return saturationReport{}, errHarnessClient
		}
		job := workload.jobs[identifier]
		expectation.removeQueued(job)
		if err := waitForSnapshot(ctx, runtimeState.scheduler, expectation); err != nil {
			return saturationReport{}, err
		}
		accumulator, ok := accumulatorFor(&accumulators, job.tenant.id)
		if !ok {
			return saturationReport{}, errHarnessState
		}
		accumulator.counts.Canceled++
	}

	for _, identifier := range workload.lateService {
		if err := enqueueObserved(
			ctx,
			runtimeState,
			workload.jobs[identifier],
			&expectation,
			&accumulators,
		); err != nil {
			return saturationReport{}, err
		}
	}

	for _, identifier := range workload.rejections {
		job := workload.jobs[identifier]
		accumulator, ok := accumulatorFor(&accumulators, job.tenant.id)
		if !ok {
			return saturationReport{}, errHarnessState
		}
		accumulator.counts.Submitted++
		call, err := runtimeState.launch(job)
		if err != nil {
			return saturationReport{}, err
		}
		result, err := awaitClientResult(ctx, call)
		if err != nil {
			return saturationReport{}, err
		}
		if result.failure != nil {
			return saturationReport{}, result.failure
		}
		if !result.rejected {
			return saturationReport{}, errHarnessClient
		}
		accumulator.counts.Rejected++
		if err := waitForSnapshot(ctx, runtimeState.scheduler, expectation); err != nil {
			return saturationReport{}, err
		}
	}

	globalRejection := workload.jobs[workload.globalRejection]
	if globalRejection.kind != jobGlobalReject ||
		globalRejection.tenant != globalProbeTenant {
		return saturationReport{}, errHarnessState
	}
	globalCall, err := runtimeState.launch(globalRejection)
	if err != nil {
		return saturationReport{}, err
	}
	globalResult, err := awaitClientResult(ctx, globalCall)
	if err != nil {
		return saturationReport{}, err
	}
	if globalResult.failure != nil {
		return saturationReport{}, globalResult.failure
	}
	if !globalResult.globalRejected {
		return saturationReport{}, errHarnessClient
	}
	if err := waitForSnapshot(ctx, runtimeState.scheduler, expectation); err != nil {
		return saturationReport{}, err
	}

	close(controlDispatch.releaseFirst)
	controlResult, err := awaitClientResult(ctx, controlCall)
	if err != nil {
		return saturationReport{}, err
	}
	if controlResult.failure != nil {
		return saturationReport{}, controlResult.failure
	}
	if !controlResult.completed {
		return saturationReport{}, errHarnessClient
	}

	plan, err := expectedDispatchPlan(workload)
	if err != nil {
		return saturationReport{}, err
	}
	expected := make([]dispatchRecord, len(plan))
	observed := make([]dispatchRecord, 0, len(plan))
	timings := make([]timingSample, 0, len(plan))
	for index, step := range plan {
		expected[index] = step.record
		dispatch, dispatchErr := awaitUpstreamDispatch(ctx, upstream)
		if dispatchErr != nil {
			return saturationReport{}, dispatchErr
		}
		if dispatch.job.id != step.identifier ||
			dispatch.job.kind != jobService {
			return saturationReport{}, errHarnessState
		}
		call := runtimeState.calls[dispatch.job.id]
		if call == nil || dispatch.arrived.Before(call.submitted) {
			return saturationReport{}, errHarnessState
		}

		record := dispatchRecord{
			Position:  uint64(index + 1),
			Tenant:    dispatch.job.tenant.label,
			Weight:    dispatch.job.tenant.weight,
			WorkUnits: dispatch.job.workUnits,
			Mode:      dispatch.job.mode(),
		}
		observed = append(observed, record)
		sample := timingSample{
			Position:                   uint64(index + 1),
			Tenant:                     dispatch.job.tenant.label,
			Mode:                       dispatch.job.mode(),
			SubmitToUpstreamDispatchNS: uint64(dispatch.arrived.Sub(call.submitted)),
		}

		released := time.Now()
		close(dispatch.releaseFirst)
		if dispatch.job.stream {
			firstEvent, eventErr := awaitFirstEvent(ctx, call)
			if eventErr != nil || firstEvent.Before(released) {
				return saturationReport{}, errHarnessClient
			}
			firstEventNS := uint64(firstEvent.Sub(released))
			sample.UpstreamReleaseToSSEEventNS = &firstEventNS
			close(dispatch.releaseDone)
		}

		result, resultErr := awaitClientResult(ctx, call)
		if resultErr != nil {
			return saturationReport{}, resultErr
		}
		if result.failure != nil {
			return saturationReport{}, result.failure
		}
		if !result.completed {
			return saturationReport{}, errHarnessClient
		}
		accumulator, ok := accumulatorFor(
			&accumulators,
			dispatch.job.tenant.id,
		)
		if !ok {
			return saturationReport{}, errHarnessState
		}
		accumulator.counts.Completed++
		accumulator.dispatchedRequests++
		accumulator.dispatchedWorkUnits += dispatch.job.workUnits
		timings = append(timings, sample)
	}

	if !reflect.DeepEqual(expected, observed) ||
		upstream.dispatchedCount() != len(plan)+1 {
		return saturationReport{}, errHarnessState
	}
	select {
	case <-upstream.issues:
		return saturationReport{}, errHarnessState
	default:
	}
	if err := runtimeState.waitClients(ctx); err != nil {
		return saturationReport{}, err
	}
	if err := waitForEmptySnapshot(ctx, runtimeState.scheduler); err != nil {
		return saturationReport{}, err
	}

	report, err := buildReport(
		profile,
		workload,
		accumulators,
		expected,
		observed,
		timings,
		uint64(upstream.dispatchedCount()),
	)
	if err != nil {
		return saturationReport{}, err
	}
	if err := validateReport(report); err != nil {
		return saturationReport{}, err
	}
	return report, nil
}

func enqueueObserved(
	ctx context.Context,
	runtimeState *gatewayRuntime,
	job saturationJob,
	expectation *queueExpectation,
	accumulators *[len(serviceTenants)]tenantAccumulator,
) error {
	accumulator, ok := accumulatorFor(accumulators, job.tenant.id)
	if !ok || job.kind == jobReject || job.kind == jobControl {
		return errHarnessState
	}
	accumulator.counts.Submitted++
	if _, err := runtimeState.launch(job); err != nil {
		return err
	}
	expectation.addQueued(job)
	if err := waitForSnapshot(ctx, runtimeState.scheduler, *expectation); err != nil {
		return err
	}
	accumulator.counts.Admitted++
	return nil
}

type queueExpectation struct {
	global  admission.Counters
	tenants map[admission.TenantID]admission.Counters
}

func newQueueExpectation(control saturationJob) queueExpectation {
	global := admission.Counters{
		InflightCount: 1,
		InflightWork:  control.workUnits,
	}
	return queueExpectation{
		global: global,
		tenants: map[admission.TenantID]admission.Counters{
			control.tenant.id:    global,
			tenantAID:            {},
			tenantBID:            {},
			globalProbeTenant.id: {},
		},
	}
}

func (e *queueExpectation) addQueued(job saturationJob) {
	e.global.QueuedCount++
	e.global.QueuedBytes += uint64(len(job.body))
	e.global.QueuedWork += job.workUnits
	counters := e.tenants[job.tenant.id]
	counters.QueuedCount++
	counters.QueuedBytes += uint64(len(job.body))
	counters.QueuedWork += job.workUnits
	e.tenants[job.tenant.id] = counters
}

func (e *queueExpectation) removeQueued(job saturationJob) {
	e.global.QueuedCount--
	e.global.QueuedBytes -= uint64(len(job.body))
	e.global.QueuedWork -= job.workUnits
	counters := e.tenants[job.tenant.id]
	counters.QueuedCount--
	counters.QueuedBytes -= uint64(len(job.body))
	counters.QueuedWork -= job.workUnits
	e.tenants[job.tenant.id] = counters
}

func waitForSnapshot(
	ctx context.Context,
	scheduler *admission.Scheduler,
	expected queueExpectation,
) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := scheduler.Snapshot(ctx)
		if err != nil {
			return errHarnessState
		}
		if snapshot.Accepting && snapshot.Global == expected.global &&
			len(snapshot.Tenants) == len(expected.tenants) {
			matched := true
			for _, tenant := range snapshot.Tenants {
				want, exists := expected.tenants[tenant.ID]
				if !exists || tenant.Counters != want || tenant.Deficit != 0 {
					matched = false
					break
				}
			}
			if matched {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errHarnessState
		case <-ticker.C:
		}
	}
}

func waitForEmptySnapshot(
	ctx context.Context,
	scheduler *admission.Scheduler,
) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := scheduler.Snapshot(ctx)
		if err != nil {
			return errHarnessState
		}
		empty := snapshot.Accepting && snapshot.Global == (admission.Counters{})
		for _, tenant := range snapshot.Tenants {
			empty = empty && tenant.Counters == (admission.Counters{}) &&
				tenant.Deficit == 0
		}
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
			return errHarnessState
		case <-ticker.C:
		}
	}
}

type gatewayRuntime struct {
	context context.Context

	scheduler        *admission.Scheduler
	gateway          *server.Server
	gatewayServeDone chan error
	upstreamClient   *httpapi.HTTPUpstream
	clientTransport  *http.Transport
	client           *http.Client
	endpoint         string

	calls map[uint64]*clientCall
	wg    sync.WaitGroup
}

func startGatewayRuntime(
	ctx context.Context,
	profile saturationProfile,
	upstream *controlledUpstream,
) (*gatewayRuntime, error) {
	parser, err := saturationParser()
	if err != nil {
		return nil, errHarnessConstruction
	}
	admissionConfig := saturationAdmissionConfig()
	scheduler, err := admission.New(admissionConfig)
	if err != nil {
		return nil, errHarnessConstruction
	}

	upstreamClient, err := httpapi.NewHTTPUpstream(httpapi.HTTPUpstreamConfig{
		Endpoint:               upstream.endpoint(),
		ConnectTimeout:         time.Second,
		TLSHandshakeTimeout:    time.Second,
		ResponseHeaderTimeout:  profile.upstreamTimeout,
		IdleConnectionTimeout:  time.Second,
		MaxResponseHeaderBytes: 8 << 10,
		MaxConnections:         1,
	}, upstreamCredential)
	if err != nil {
		closeScheduler(scheduler)
		return nil, errHarnessConstruction
	}

	handler, err := httpapi.NewHandler(
		saturationHTTPConfig(profile),
		parser,
		scheduler,
		upstreamClient,
	)
	if err != nil {
		upstreamClient.CloseIdleConnections()
		closeScheduler(scheduler)
		return nil, errHarnessConstruction
	}

	listener, err := net.ListenTCP(
		"tcp4",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)},
	)
	if err != nil {
		upstreamClient.CloseIdleConnections()
		closeScheduler(scheduler)
		return nil, errHarnessConstruction
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		address.Port <= 0 || address.Port > 65_535 {
		_ = listener.Close()
		upstreamClient.CloseIdleConnections()
		closeScheduler(scheduler)
		return nil, errHarnessConstruction
	}

	gateway, err := server.New(server.Config{
		HeaderReadTimeout:       2 * time.Second,
		ResponseWriteTimeout:    profile.responseWriteTimeout,
		IdleTimeout:             2 * time.Second,
		GraceTimeout:            profile.graceTimeout,
		ForceTimeout:            profile.forceTimeout,
		HeaderReadEnvelopeBytes: 8 << 10,
		MaxConnections:          32,
	}, listener, handler, scheduler)
	if err != nil {
		_ = listener.Close()
		upstreamClient.CloseIdleConnections()
		closeScheduler(scheduler)
		return nil, errHarnessConstruction
	}

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	clientTransport := &http.Transport{
		Proxy:                 nil,
		DisableCompression:    true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       32,
		IdleConnTimeout:       time.Second,
		ResponseHeaderTimeout: profile.defaultQueueTimeout,
		Protocols:             protocols,
	}
	state := &gatewayRuntime{
		context:          ctx,
		scheduler:        scheduler,
		gateway:          gateway,
		gatewayServeDone: make(chan error, 1),
		upstreamClient:   upstreamClient,
		clientTransport:  clientTransport,
		client:           &http.Client{Transport: clientTransport},
		endpoint: "http://" +
			net.JoinHostPort("127.0.0.1", strconv.Itoa(address.Port)) +
			chatCompletionsPath,
		calls: make(map[uint64]*clientCall, ciTotalRequestCount),
	}
	go func() {
		state.gatewayServeDone <- gateway.Serve()
	}()
	return state, nil
}

func saturationParser() (*contract.Parser, error) {
	return contract.NewParser(syntheticModel, contract.Limits{
		MaxBodyBytes:        ciMaximumBodyBytes,
		MaxMessageCount:     1,
		MaxMessageTextBytes: 1,
		MaxCompletionTokens: ciMaximumCompletionTokens,
		CompletionWeight:    1,
		MaxRequestUnits:     ciMaximumRequestUnits,
	})
}

func saturationHTTPConfig(profile saturationProfile) httpapi.Config {
	return httpapi.Config{
		DefaultQueueTimeout:    profile.defaultQueueTimeout,
		BodyReadTimeout:        profile.bodyReadTimeout,
		UpstreamTimeout:        profile.upstreamTimeout,
		StreamReadTimeout:      profile.streamReadTimeout,
		StreamEventTimeout:     profile.streamEventTimeout,
		MaxResponseBodyBytes:   512,
		MaxStreamEventBytes:    256,
		MaxStreamEvents:        4,
		GlobalPreDispatchLimit: ciGlobalQueueCapacity,
		TenantPreDispatch: []httpapi.TenantPreDispatchLimit{
			{Tenant: controlTenant.id, Count: 1},
			{Tenant: serviceTenants[0].id, Count: ciQueueCapacityPerTenant},
			{Tenant: serviceTenants[1].id, Count: ciQueueCapacityPerTenant},
			{Tenant: globalProbeTenant.id, Count: 1},
		},
		Credentials: []httpapi.Credential{
			{Tenant: controlTenant.id, Token: controlTenant.token},
			{Tenant: serviceTenants[0].id, Token: serviceTenants[0].token},
			{Tenant: serviceTenants[1].id, Token: serviceTenants[1].token},
			{Tenant: globalProbeTenant.id, Token: globalProbeTenant.token},
		},
	}
}

func saturationAdmissionConfig() admission.Config {
	return admission.Config{
		MaxBodyBytes:    ciMaximumBodyBytes,
		MaxRequestUnits: ciMaximumRequestUnits,
		BaseQuantum:     ciBaseQuantum,
		DeficitCap:      ciDeficitCap,
		GlobalQueue: admission.QueueLimits{
			Count: ciGlobalQueueCapacity,
			Bytes: ciGlobalQueueCapacity * ciMaximumBodyBytes,
			Work:  ciGlobalQueueCapacity * ciMaximumRequestUnits,
		},
		GlobalInflight: admission.InflightLimits{
			Count: 1,
			Work:  ciMaximumRequestUnits,
		},
		Tenants: []admission.TenantConfig{
			{
				ID:     controlTenant.id,
				Weight: controlTenant.weight,
				Queue: admission.QueueLimits{
					Count: 1,
					Bytes: ciMaximumBodyBytes,
					Work:  ciMaximumRequestUnits,
				},
				Inflight: admission.InflightLimits{
					Count: 1,
					Work:  ciMaximumRequestUnits,
				},
			},
			{
				ID:     serviceTenants[0].id,
				Weight: serviceTenants[0].weight,
				Queue: admission.QueueLimits{
					Count: ciQueueCapacityPerTenant,
					Bytes: ciQueueCapacityPerTenant * ciMaximumBodyBytes,
					Work:  ciQueueCapacityPerTenant * ciMaximumRequestUnits,
				},
				Inflight: admission.InflightLimits{
					Count: 1,
					Work:  ciMaximumRequestUnits,
				},
			},
			{
				ID:     serviceTenants[1].id,
				Weight: serviceTenants[1].weight,
				Queue: admission.QueueLimits{
					Count: ciQueueCapacityPerTenant,
					Bytes: ciQueueCapacityPerTenant * ciMaximumBodyBytes,
					Work:  ciQueueCapacityPerTenant * ciMaximumRequestUnits,
				},
				Inflight: admission.InflightLimits{
					Count: 1,
					Work:  ciMaximumRequestUnits,
				},
			},
			{
				ID:     globalProbeTenant.id,
				Weight: globalProbeTenant.weight,
				Queue: admission.QueueLimits{
					Count: 1,
					Bytes: ciMaximumBodyBytes,
					Work:  ciMaximumRequestUnits,
				},
				Inflight: admission.InflightLimits{
					Count: 1,
					Work:  ciMaximumRequestUnits,
				},
			},
		},
	}
}

func closeScheduler(scheduler *admission.Scheduler) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = scheduler.Close(ctx)
	cancel()
}

func (r *gatewayRuntime) shutdown(ctx context.Context) error {
	if err := r.waitClients(ctx); err != nil {
		return errHarnessShutdown
	}
	result, err := r.gateway.Shutdown(ctx)
	if err != nil || result.Forced ||
		result.Drain != (admission.DrainResult{}) ||
		result.Force != (admission.ForceCancelResult{}) {
		return errHarnessShutdown
	}
	select {
	case serveErr := <-r.gatewayServeDone:
		if serveErr != nil {
			return errHarnessShutdown
		}
	case <-ctx.Done():
		return errHarnessShutdown
	}
	r.clientTransport.CloseIdleConnections()
	r.upstreamClient.CloseIdleConnections()
	return nil
}

func (r *gatewayRuntime) abort(ctx context.Context) {
	r.clientTransport.CloseIdleConnections()
	_, _ = r.gateway.Shutdown(ctx)
	select {
	case <-r.gatewayServeDone:
	case <-ctx.Done():
	}
	r.upstreamClient.CloseIdleConnections()
	wait := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(wait)
	}()
	select {
	case <-wait:
	case <-ctx.Done():
	}
}

type clientCall struct {
	job        saturationJob
	submitted  time.Time
	cancel     context.CancelFunc
	result     chan clientResult
	firstEvent chan time.Time
}

type clientResult struct {
	completed      bool
	canceled       bool
	deadline       bool
	rejected       bool
	globalRejected bool
	failure        error
}

func (r *gatewayRuntime) launch(job saturationJob) (*clientCall, error) {
	if _, duplicate := r.calls[job.id]; duplicate {
		return nil, errHarnessState
	}
	requestContext := r.context
	cancel := context.CancelFunc(nil)
	if job.kind == jobCancel {
		requestContext, cancel = context.WithCancel(r.context)
	}
	call := &clientCall{
		job:        job,
		submitted:  time.Now(),
		cancel:     cancel,
		result:     make(chan clientResult, 1),
		firstEvent: make(chan time.Time, 1),
	}
	r.calls[job.id] = call
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		call.result <- executeClientRequest(
			requestContext,
			r.client,
			r.endpoint,
			call,
		)
	}()
	return call, nil
}

func executeClientRequest(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	call *clientCall,
) clientResult {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(call.job.body),
	)
	if err != nil {
		return clientResult{failure: errHarnessClientBuild}
	}
	request.Header.Set("Authorization", "Bearer "+call.job.tenant.token)
	request.Header.Set("Content-Type", "application/json")
	if call.job.queueTimeoutMS != 0 {
		request.Header.Set(
			queueTimeoutHeader,
			strconv.FormatUint(call.job.queueTimeoutMS, 10),
		)
	}

	response, requestErr := client.Do(request)
	if requestErr != nil {
		if call.job.kind == jobCancel &&
			errors.Is(ctx.Err(), context.Canceled) {
			return clientResult{canceled: true}
		}
		return clientResult{failure: errHarnessClientIO}
	}
	if response.ProtoMajor != 1 || !validGatewayHeaders(response.Header) {
		_ = response.Body.Close()
		return clientResult{failure: errHarnessClientHeader}
	}
	if call.job.stream && response.StatusCode == http.StatusOK {
		valid := validStreamHeaders(response.Header) &&
			readClientStream(response.Body, call.firstEvent)
		if response.Body.Close() != nil || !valid {
			return clientResult{failure: invalidClientWireError(call.job.kind)}
		}
		return clientResult{completed: true}
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1025))
	if readErr != nil {
		_ = response.Body.Close()
		return clientResult{failure: errHarnessClientIO}
	}
	if !validBufferedHeaders(response.Header, len(body)) {
		_ = response.Body.Close()
		return clientResult{failure: errHarnessClientHeader}
	}
	valid := false
	switch call.job.kind {
	case jobControl, jobService:
		valid = response.StatusCode == http.StatusOK &&
			string(body) == bufferedCompletionBody
	case jobDeadline:
		if response.StatusCode != http.StatusServiceUnavailable {
			_ = response.Body.Close()
			return clientResult{failure: errHarnessDeadlineCode}
		}
		if string(body) != queueDeadlineBody {
			_ = response.Body.Close()
			return clientResult{failure: errHarnessDeadlineBody}
		}
		valid = true
	case jobReject:
		valid = response.StatusCode == http.StatusTooManyRequests &&
			string(body) == tenantRejectedBody
	case jobGlobalReject:
		valid = response.StatusCode == http.StatusServiceUnavailable &&
			string(body) == globalRejectedBody
	}
	if response.Body.Close() != nil || !valid {
		return clientResult{failure: invalidClientWireError(call.job.kind)}
	}
	switch call.job.kind {
	case jobControl, jobService:
		return clientResult{completed: true}
	case jobDeadline:
		return clientResult{deadline: true}
	case jobReject:
		return clientResult{rejected: true}
	case jobGlobalReject:
		return clientResult{globalRejected: true}
	default:
		return clientResult{failure: errHarnessClientWire}
	}
}

func invalidClientWireError(kind jobKind) error {
	switch kind {
	case jobControl:
		return errHarnessControlWire
	case jobService:
		return errHarnessServiceWire
	case jobDeadline:
		return errHarnessDeadlineWire
	case jobReject:
		return errHarnessRejectedWire
	case jobGlobalReject:
		return errHarnessGlobalRejectedWire
	default:
		return errHarnessClientWire
	}
}

func validGatewayHeaders(header http.Header) bool {
	return header.Get("Cache-Control") == "no-store" &&
		header.Get("X-Content-Type-Options") == "nosniff" &&
		validPublicRequestID(header.Get("X-Request-Id")) &&
		len(header.Values("Authorization")) == 0
}

func validBufferedHeaders(header http.Header, bodyBytes int) bool {
	mediaType, parameters, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || len(parameters) != 0 {
		return false
	}
	return header.Get("Content-Length") == strconv.Itoa(bodyBytes)
}

func validStreamHeaders(header http.Header) bool {
	mediaType, parameters, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" || len(parameters) != 0 {
		return false
	}
	return len(header.Values("Content-Length")) == 0
}

func validPublicRequestID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for index := range len(value) {
		if value[index] < '0' ||
			value[index] > '9' && (value[index] < 'a' || value[index] > 'f') {
			return false
		}
	}
	return true
}

func readClientStream(body io.Reader, firstEvent chan<- time.Time) bool {
	reader := bufio.NewReaderSize(body, 256)
	firstLine, err := reader.ReadString('\n')
	if err != nil || firstLine != streamChunkEvent[:len(streamChunkEvent)-1] {
		return false
	}
	// ReadString includes the line ending. The chunk constant contains a second
	// blank line, so the first slice intentionally drops only that final LF.
	if firstLine != "data: {\"object\":\"chat.completion.chunk\"}\n" {
		return false
	}
	delimiter, err := reader.ReadString('\n')
	if err != nil || delimiter != "\n" {
		return false
	}
	firstEvent <- time.Now()
	remainder, err := io.ReadAll(io.LimitReader(reader, int64(len(streamDoneEvent)+1)))
	return err == nil && string(remainder) == streamDoneEvent
}

func awaitClientResult(
	ctx context.Context,
	call *clientCall,
) (clientResult, error) {
	if call == nil {
		return clientResult{}, errHarnessState
	}
	select {
	case result := <-call.result:
		return result, nil
	case <-ctx.Done():
		return clientResult{}, errHarnessClient
	}
}

func awaitFirstEvent(ctx context.Context, call *clientCall) (time.Time, error) {
	select {
	case observed := <-call.firstEvent:
		return observed, nil
	case <-ctx.Done():
		return time.Time{}, errHarnessClient
	}
}

func awaitUpstreamDispatch(
	ctx context.Context,
	upstream *controlledUpstream,
) (upstreamDispatch, error) {
	select {
	case dispatch := <-upstream.dispatches:
		return dispatch, nil
	case err := <-upstream.issues:
		return upstreamDispatch{}, err
	case <-ctx.Done():
		return upstreamDispatch{}, errHarnessState
	}
}

func (r *gatewayRuntime) waitClients(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errHarnessClient
	}
}

func accumulatorFor(
	accumulators *[len(serviceTenants)]tenantAccumulator,
	identifier admission.TenantID,
) (*tenantAccumulator, bool) {
	index, ok := serviceTenantIndex(identifier)
	if !ok {
		return nil, false
	}
	return &accumulators[index], true
}

func buildReport(
	profile saturationProfile,
	workload seededWorkload,
	accumulators [len(serviceTenants)]tenantAccumulator,
	expected []dispatchRecord,
	observed []dispatchRecord,
	timings []timingSample,
	upstreamRuns uint64,
) (saturationReport, error) {
	report := saturationReport{
		SchemaVersion:   3,
		Profile:         profile.name,
		Seed:            workload.seed,
		GoVersion:       runtime.Version(),
		OperatingSystem: runtime.GOOS,
		Architecture:    runtime.GOARCH,
		Tenants:         make([]tenantReport, 0, len(accumulators)),
	}
	report.Claims.Performance = false
	report.Claims.ReportByteReproducible = false
	report.Claims.Scope = reportScope
	report.Claims.Timings = reportTimingClaim
	report.Claims.Streaming = reportStreamingClaim
	report.Claims.Encoding = reportEncoding
	report.Topology.Gateway = productionGatewayDescription
	report.Topology.Transport = loopbackTransportDescription
	report.Topology.ControlledUpstream = true
	report.Topology.SeededArrivalOrder = true
	report.Topology.SchedulerSnapshotBarriers = true
	report.Topology.GlobalInflightRequests = 1
	report.Topology.GlobalQueuedRequests = ciGlobalQueueCapacity
	report.Topology.ServiceRequestsPerTenant = ciServiceRequestsPerTenant
	for _, accumulator := range accumulators {
		report.Tenants = append(report.Tenants, tenantReport{
			Tenant:              accumulator.definition.label,
			Weight:              accumulator.definition.weight,
			Submitted:           accumulator.counts.Submitted,
			Admitted:            accumulator.counts.Admitted,
			Rejected:            accumulator.counts.Rejected,
			Completed:           accumulator.counts.Completed,
			Canceled:            accumulator.counts.Canceled,
			DeadlineExceeded:    accumulator.counts.DeadlineExceeded,
			DispatchedRequests:  accumulator.dispatchedRequests,
			DispatchedWorkUnits: accumulator.dispatchedWorkUnits,
		})
		report.ServiceTotals.Submitted += accumulator.counts.Submitted
		report.ServiceTotals.Admitted += accumulator.counts.Admitted
		report.ServiceTotals.Rejected += accumulator.counts.Rejected
		report.ServiceTotals.Completed += accumulator.counts.Completed
		report.ServiceTotals.Canceled += accumulator.counts.Canceled
		report.ServiceTotals.DeadlineExceeded += accumulator.counts.DeadlineExceeded
	}
	report.Control = controlReport{
		Submitted:        1,
		Admitted:         1,
		Completed:        1,
		UpstreamRequests: 1,
	}
	report.CapacityProbes = capacityProbeReport{
		Tenant: publicErrorProbeReport{
			Scope:            tenantProbeScope,
			Submitted:        uint64(len(serviceTenants)),
			Rejected:         uint64(len(serviceTenants)),
			StatusCode:       http.StatusTooManyRequests,
			ErrorCode:        "tenant_capacity_exhausted",
			UpstreamRequests: 0,
		},
		Global: publicErrorProbeReport{
			Scope:            globalProbeScope,
			Submitted:        1,
			Rejected:         1,
			StatusCode:       http.StatusServiceUnavailable,
			ErrorCode:        "overloaded",
			UpstreamRequests: 0,
		},
	}
	report.Accounting.TotalJobs = uint64(len(workload.jobs))
	report.Accounting.ServiceSubmissions = report.ServiceTotals.Submitted
	report.Accounting.ControlSubmissions = report.Control.Submitted
	report.Accounting.GlobalProbeSubmissions = report.CapacityProbes.Global.Submitted
	report.Accounting.TotalUpstreamRequests = upstreamRuns
	report.Accounting.Reconciled =
		report.Accounting.TotalJobs ==
			report.Accounting.ServiceSubmissions+
				report.Accounting.ControlSubmissions+
				report.Accounting.GlobalProbeSubmissions &&
			upstreamRuns ==
				report.ServiceTotals.Completed+report.Control.UpstreamRequests
	report.Accounting.Boundary = accountingBoundary
	report.Timeouts = timeoutEnvelopeReport{
		ExecutionContextMS:      uint64(profile.executionTimeout / time.Millisecond),
		GracefulAttemptMS:       uint64(profile.gracefulCleanupTimeout / time.Millisecond),
		AbortReserveMS:          uint64(profile.abortCleanupTimeout / time.Millisecond),
		MaximumCleanupMS:        uint64((profile.gracefulCleanupTimeout + profile.abortCleanupTimeout) / time.Millisecond),
		SeparateCleanupEnvelope: true,
		Boundary:                timeoutBoundary,
	}
	report.Service.Oracle = serviceOracle
	report.Service.OracleMatch = reflect.DeepEqual(expected, observed)
	report.Service.Expected = expected
	report.Service.Observed = observed
	report.Service.UpstreamRequests = uint64(len(observed))
	report.Timing.Clock = timingClock
	report.Timing.Thresholds = false
	report.Timing.OneRunDiagnostics = true
	report.Timing.ByteReproducible = false
	report.Timing.Boundary = timingBoundary
	report.Timing.Samples = timings
	report.Categorical.Projection = categoricalProjectionName
	report.Categorical.DigestAlgorithm = "SHA-256"
	report.Categorical.ExcludesTiming = true
	report.Categorical.ByteReproducible = true
	digest, err := categoricalDigest(report)
	if err != nil {
		return saturationReport{}, err
	}
	report.Categorical.Digest = digest
	return report, nil
}

func validateReport(report saturationReport) error {
	if report.SchemaVersion != 3 || report.Profile != ciProfileName ||
		report.GoVersion == "" || report.OperatingSystem == "" ||
		report.Architecture == "" ||
		report.Claims != (reportClaims{
			Scope:     reportScope,
			Timings:   reportTimingClaim,
			Streaming: reportStreamingClaim,
			Encoding:  reportEncoding,
		}) ||
		report.Topology != (topologyReport{
			Gateway:                   productionGatewayDescription,
			Transport:                 loopbackTransportDescription,
			ControlledUpstream:        true,
			SeededArrivalOrder:        true,
			SchedulerSnapshotBarriers: true,
			GlobalInflightRequests:    1,
			GlobalQueuedRequests:      ciGlobalQueueCapacity,
			ServiceRequestsPerTenant:  ciServiceRequestsPerTenant,
		}) ||
		report.Timing.Thresholds || !report.Timing.OneRunDiagnostics ||
		report.Timing.ByteReproducible ||
		report.Timing.Clock != timingClock ||
		report.Timing.Boundary != timingBoundary ||
		report.Categorical.Projection != categoricalProjectionName ||
		!report.Categorical.ExcludesTiming ||
		!report.Categorical.ByteReproducible ||
		report.Categorical.DigestAlgorithm != "SHA-256" ||
		report.Service.Oracle != serviceOracle ||
		!report.Service.OracleMatch ||
		len(report.Tenants) != len(serviceTenants) ||
		len(report.Service.Expected) !=
			ciServiceRequestsPerTenant*len(serviceTenants) ||
		!reflect.DeepEqual(report.Service.Expected, report.Service.Observed) ||
		len(report.Timing.Samples) != len(report.Service.Observed) ||
		report.Service.UpstreamRequests != uint64(len(report.Service.Observed)) {
		return errHarnessReport
	}

	dispatchCounts := [len(serviceTenants)]uint64{}
	dispatchWork := [len(serviceTenants)]uint64{}
	streamCounts := [len(serviceTenants)]uint64{}
	for index, record := range report.Service.Observed {
		tenantIndex := -1
		for candidate, tenant := range serviceTenants {
			if record.Tenant == tenant.label {
				tenantIndex = candidate
				break
			}
		}
		if tenantIndex < 0 ||
			record.Position != uint64(index+1) ||
			record.Weight != serviceTenants[tenantIndex].weight ||
			record.WorkUnits == 0 ||
			record.WorkUnits > ciMaximumRequestUnits ||
			record.Mode != "buffered" && record.Mode != "sse" {
			return errHarnessReport
		}
		dispatchCounts[tenantIndex]++
		dispatchWork[tenantIndex] += record.WorkUnits
		if record.Mode == "sse" {
			streamCounts[tenantIndex]++
		}

		sample := report.Timing.Samples[index]
		if sample.Position != record.Position ||
			sample.Tenant != record.Tenant ||
			sample.Mode != record.Mode {
			return errHarnessReport
		}
		if record.Mode == "sse" {
			if sample.UpstreamReleaseToSSEEventNS == nil {
				return errHarnessReport
			}
		} else if sample.UpstreamReleaseToSSEEventNS != nil {
			return errHarnessReport
		}
	}
	for index, tenant := range report.Tenants {
		if tenant.Tenant != serviceTenants[index].label ||
			tenant.Weight != serviceTenants[index].weight ||
			tenant.Submitted != 13 || tenant.Admitted != 12 ||
			tenant.Rejected != 1 || tenant.Completed != 10 ||
			tenant.Canceled != 1 || tenant.DeadlineExceeded != 1 ||
			tenant.DispatchedRequests != ciServiceRequestsPerTenant ||
			tenant.DispatchedRequests != dispatchCounts[index] ||
			tenant.DispatchedWorkUnits != dispatchWork[index] ||
			streamCounts[index] != 1 ||
			tenant.Submitted != tenant.Admitted+tenant.Rejected ||
			tenant.Admitted !=
				tenant.Completed+tenant.Canceled+tenant.DeadlineExceeded {
			return errHarnessReport
		}
	}
	if report.ServiceTotals != (outcomeCounts{
		Submitted:        26,
		Admitted:         24,
		Rejected:         2,
		Completed:        20,
		Canceled:         2,
		DeadlineExceeded: 2,
	}) {
		return errHarnessReport
	}
	if report.Control != (controlReport{
		Submitted:        1,
		Admitted:         1,
		Completed:        1,
		UpstreamRequests: 1,
	}) {
		return errHarnessReport
	}
	if report.CapacityProbes != (capacityProbeReport{
		Tenant: publicErrorProbeReport{
			Scope:            tenantProbeScope,
			Submitted:        uint64(len(serviceTenants)),
			Rejected:         uint64(len(serviceTenants)),
			StatusCode:       http.StatusTooManyRequests,
			ErrorCode:        "tenant_capacity_exhausted",
			UpstreamRequests: 0,
		},
		Global: publicErrorProbeReport{
			Scope:            globalProbeScope,
			Submitted:        1,
			Rejected:         1,
			StatusCode:       http.StatusServiceUnavailable,
			ErrorCode:        "overloaded",
			UpstreamRequests: 0,
		},
	}) {
		return errHarnessReport
	}
	if report.Accounting != (accountingReport{
		TotalJobs:              ciTotalRequestCount,
		ServiceSubmissions:     report.ServiceTotals.Submitted,
		ControlSubmissions:     report.Control.Submitted,
		GlobalProbeSubmissions: report.CapacityProbes.Global.Submitted,
		TotalUpstreamRequests:  report.Service.UpstreamRequests + report.Control.UpstreamRequests,
		Reconciled:             true,
		Boundary:               accountingBoundary,
	}) {
		return errHarnessReport
	}
	if report.Timeouts != (timeoutEnvelopeReport{
		ExecutionContextMS:      uint64((30 * time.Second) / time.Millisecond),
		GracefulAttemptMS:       uint64((9 * time.Second) / time.Millisecond),
		AbortReserveMS:          uint64((10 * time.Second) / time.Millisecond),
		MaximumCleanupMS:        uint64((19 * time.Second) / time.Millisecond),
		SeparateCleanupEnvelope: true,
		Boundary:                timeoutBoundary,
	}) {
		return errHarnessReport
	}
	digest, err := categoricalDigest(report)
	if err != nil || digest != report.Categorical.Digest {
		return errHarnessReport
	}
	return nil
}

func categoricalDigest(report saturationReport) (string, error) {
	projection := categoricalProjection{
		SchemaVersion:  report.SchemaVersion,
		Profile:        report.Profile,
		Seed:           report.Seed,
		Topology:       report.Topology,
		ServiceTotals:  report.ServiceTotals,
		Control:        report.Control,
		CapacityProbes: report.CapacityProbes,
		Accounting:     report.Accounting,
		Timeouts:       report.Timeouts,
		Tenants:        report.Tenants,
		Service:        report.Service,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", errHarnessReport
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func marshalReport(report saturationReport) ([]byte, error) {
	if err := validateReport(report); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, errHarnessReport
	}
	return append(encoded, '\n'), nil
}
