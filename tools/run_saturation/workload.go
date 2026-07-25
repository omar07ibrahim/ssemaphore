package main

import (
	"errors"
	"strconv"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
)

const (
	ciProfileName = "ci"

	controlTenantID     admission.TenantID = 99
	globalProbeTenantID admission.TenantID = 77
	tenantAID           admission.TenantID = 1
	tenantBID           admission.TenantID = 2

	ciServiceRequestsPerTenant = 10
	ciInitialServicePerTenant  = 8
	ciMaximumCompletionTokens  = 64
	ciMaximumBodyBytes         = 256
	ciMaximumRequestUnits      = ciMaximumBodyBytes + ciMaximumCompletionTokens
	ciBaseQuantum              = 128
	ciDeficitCap               = 768
	ciQueueCapacityPerTenant   = 10
	ciGlobalQueueCapacity      = 20
	ciTotalRequestCount        = 28

	syntheticModel = "saturation-fixture"
)

var (
	errInvalidProfile  = errors.New("saturation profile is invalid")
	errInvalidWorkload = errors.New("seeded saturation workload is invalid")
)

type saturationProfile struct {
	name                   string
	executionTimeout       time.Duration
	gracefulCleanupTimeout time.Duration
	abortCleanupTimeout    time.Duration
	upstreamAbortTimeout   time.Duration
	runtimeAbortTimeout    time.Duration
	queueDeadline          time.Duration
	defaultQueueTimeout    time.Duration
	bodyReadTimeout        time.Duration
	upstreamTimeout        time.Duration
	streamReadTimeout      time.Duration
	streamEventTimeout     time.Duration
	responseWriteTimeout   time.Duration
	graceTimeout           time.Duration
	forceTimeout           time.Duration
}

func profileByName(name string) (saturationProfile, error) {
	if name != ciProfileName {
		return saturationProfile{}, errInvalidProfile
	}
	return saturationProfile{
		name:                   ciProfileName,
		executionTimeout:       30 * time.Second,
		gracefulCleanupTimeout: 9 * time.Second,
		abortCleanupTimeout:    10 * time.Second,
		upstreamAbortTimeout:   2 * time.Second,
		runtimeAbortTimeout:    8 * time.Second,
		queueDeadline:          time.Second,
		defaultQueueTimeout:    15 * time.Second,
		bodyReadTimeout:        2 * time.Second,
		upstreamTimeout:        5 * time.Second,
		streamReadTimeout:      3 * time.Second,
		streamEventTimeout:     4 * time.Second,
		responseWriteTimeout:   2 * time.Second,
		graceTimeout:           5 * time.Second,
		forceTimeout:           2 * time.Second,
	}, nil
}

type tenantDefinition struct {
	id     admission.TenantID
	label  string
	weight uint64
	token  string
}

var (
	controlTenant = tenantDefinition{
		id:     controlTenantID,
		label:  "control",
		weight: 1,
		token:  "synthetic-control-credential",
	}
	globalProbeTenant = tenantDefinition{
		id:     globalProbeTenantID,
		label:  "global-capacity-probe",
		weight: 1,
		token:  "synthetic-global-probe-credential",
	}
	serviceTenants = [...]tenantDefinition{
		{
			id:     tenantAID,
			label:  "tenant-a",
			weight: 1,
			token:  "synthetic-tenant-a-credential",
		},
		{
			id:     tenantBID,
			label:  "tenant-b",
			weight: 3,
			token:  "synthetic-tenant-b-credential",
		},
	}
)

type jobKind uint8

const (
	jobControl jobKind = iota + 1
	jobService
	jobCancel
	jobDeadline
	jobReject
	jobGlobalReject
)

type saturationJob struct {
	id             uint64
	tenant         tenantDefinition
	kind           jobKind
	stream         bool
	body           []byte
	workUnits      uint64
	queueTimeoutMS uint64
}

func (j saturationJob) mode() string {
	if j.stream {
		return "sse"
	}
	return "buffered"
}

type seededWorkload struct {
	seed uint64

	control         uint64
	initialService  []uint64
	cancellations   []uint64
	deadlines       []uint64
	lateService     []uint64
	rejections      []uint64
	globalRejection uint64

	serviceQueues [len(serviceTenants)][]uint64
	jobs          map[uint64]saturationJob
}

func buildSeededWorkload(seed uint64, profile saturationProfile) (seededWorkload, error) {
	if profile.name != ciProfileName || profile.queueDeadline <= 0 ||
		profile.queueDeadline >= profile.defaultQueueTimeout {
		return seededWorkload{}, errInvalidProfile
	}

	generator := splitMix64{state: seed}
	identifiers := make([]uint64, ciMaximumCompletionTokens)
	for index := range identifiers {
		identifiers[index] = uint64(index + 1)
	}
	shuffle(&generator, identifiers)
	nextIdentifier := func() uint64 {
		identifier := identifiers[0]
		identifiers = identifiers[1:]
		return identifier
	}

	workload := seededWorkload{
		seed: seed,
		jobs: make(map[uint64]saturationJob, ciTotalRequestCount),
	}
	add := func(tenant tenantDefinition, kind jobKind, stream bool) uint64 {
		identifier := nextIdentifier()
		job := makeSaturationJob(identifier, tenant, kind, stream, profile)
		workload.jobs[identifier] = job
		return identifier
	}

	workload.control = add(controlTenant, jobControl, false)
	service := [len(serviceTenants)][]uint64{}
	for tenantIndex, tenant := range serviceTenants {
		for requestIndex := range ciServiceRequestsPerTenant {
			stream := requestIndex == ciServiceRequestsPerTenant-1
			service[tenantIndex] = append(
				service[tenantIndex],
				add(tenant, jobService, stream),
			)
		}
		workload.cancellations = append(
			workload.cancellations,
			add(tenant, jobCancel, false),
		)
		workload.deadlines = append(
			workload.deadlines,
			add(tenant, jobDeadline, false),
		)
		workload.rejections = append(
			workload.rejections,
			add(tenant, jobReject, false),
		)
	}
	workload.globalRejection = add(
		globalProbeTenant,
		jobGlobalReject,
		false,
	)

	for tenantIndex := range serviceTenants {
		workload.initialService = append(
			workload.initialService,
			service[tenantIndex][:ciInitialServicePerTenant]...,
		)
		workload.lateService = append(
			workload.lateService,
			service[tenantIndex][ciInitialServicePerTenant:]...,
		)
	}
	shuffle(&generator, workload.initialService)
	shuffle(&generator, workload.cancellations)
	shuffle(&generator, workload.deadlines)
	shuffle(&generator, workload.lateService)
	shuffle(&generator, workload.rejections)

	for _, identifier := range append(
		append([]uint64(nil), workload.initialService...),
		workload.lateService...,
	) {
		job := workload.jobs[identifier]
		tenantIndex, ok := serviceTenantIndex(job.tenant.id)
		if !ok {
			return seededWorkload{}, errInvalidWorkload
		}
		workload.serviceQueues[tenantIndex] = append(
			workload.serviceQueues[tenantIndex],
			identifier,
		)
	}

	if err := validateSeededWorkload(workload); err != nil {
		return seededWorkload{}, err
	}
	return workload, nil
}

func makeSaturationJob(
	identifier uint64,
	tenant tenantDefinition,
	kind jobKind,
	stream bool,
	profile saturationProfile,
) saturationJob {
	body := make([]byte, 0, 160)
	body = append(
		body,
		`{"model":"saturation-fixture","messages":[{"role":"user","content":""}],"max_completion_tokens":`...,
	)
	body = strconv.AppendUint(body, identifier, 10)
	if stream {
		body = append(body, `,"stream":true}`...)
	} else {
		body = append(body, '}')
	}

	queueTimeoutMS := uint64(0)
	if kind == jobDeadline {
		queueTimeoutMS = uint64(profile.queueDeadline / time.Millisecond)
	}
	return saturationJob{
		id:             identifier,
		tenant:         tenant,
		kind:           kind,
		stream:         stream,
		body:           body,
		workUnits:      uint64(len(body)) + identifier,
		queueTimeoutMS: queueTimeoutMS,
	}
}

func validateSeededWorkload(workload seededWorkload) error {
	if len(workload.jobs) != ciTotalRequestCount ||
		len(workload.initialService) != ciInitialServicePerTenant*len(serviceTenants) ||
		len(workload.cancellations) != len(serviceTenants) ||
		len(workload.deadlines) != len(serviceTenants) ||
		len(workload.lateService) !=
			(ciServiceRequestsPerTenant-ciInitialServicePerTenant)*len(serviceTenants) ||
		len(workload.rejections) != len(serviceTenants) {
		return errInvalidWorkload
	}

	phaseSeen := make(map[uint64]struct{}, len(workload.jobs))
	kindCounts := [len(serviceTenants)][jobReject + 1]uint64{}
	recordPhase := func(identifiers []uint64, want jobKind) bool {
		for _, identifier := range identifiers {
			job, exists := workload.jobs[identifier]
			if !exists || job.kind != want {
				return false
			}
			if _, duplicate := phaseSeen[identifier]; duplicate {
				return false
			}
			phaseSeen[identifier] = struct{}{}
			tenantIndex, ok := serviceTenantIndex(job.tenant.id)
			if !ok || job.tenant != serviceTenants[tenantIndex] {
				return false
			}
			kindCounts[tenantIndex][want]++
		}
		return true
	}

	control, exists := workload.jobs[workload.control]
	if !exists || control.kind != jobControl ||
		control.tenant != controlTenant || control.stream {
		return errInvalidWorkload
	}
	phaseSeen[workload.control] = struct{}{}
	globalRejection, exists := workload.jobs[workload.globalRejection]
	if !exists || globalRejection.kind != jobGlobalReject ||
		globalRejection.tenant != globalProbeTenant ||
		globalRejection.stream {
		return errInvalidWorkload
	}
	if _, duplicate := phaseSeen[workload.globalRejection]; duplicate {
		return errInvalidWorkload
	}
	phaseSeen[workload.globalRejection] = struct{}{}
	if !recordPhase(workload.initialService, jobService) ||
		!recordPhase(workload.cancellations, jobCancel) ||
		!recordPhase(workload.deadlines, jobDeadline) ||
		!recordPhase(workload.lateService, jobService) ||
		!recordPhase(workload.rejections, jobReject) ||
		len(phaseSeen) != len(workload.jobs) {
		return errInvalidWorkload
	}

	streams := [len(serviceTenants)]uint64{}
	for identifier, job := range workload.jobs {
		if identifier == 0 || identifier > ciMaximumCompletionTokens ||
			job.id != identifier || len(job.body) == 0 ||
			uint64(len(job.body)) > ciMaximumBodyBytes ||
			job.workUnits != uint64(len(job.body))+identifier ||
			job.workUnits > ciMaximumRequestUnits {
			return errInvalidWorkload
		}
		if job.stream {
			tenantIndex, ok := serviceTenantIndex(job.tenant.id)
			if !ok || job.kind != jobService {
				return errInvalidWorkload
			}
			streams[tenantIndex]++
		}
		if job.kind == jobDeadline && job.queueTimeoutMS == 0 {
			return errInvalidWorkload
		}
		if job.kind != jobDeadline && job.queueTimeoutMS != 0 {
			return errInvalidWorkload
		}
		switch job.kind {
		case jobControl:
			if identifier != workload.control || job.tenant != controlTenant {
				return errInvalidWorkload
			}
		case jobService, jobCancel, jobDeadline, jobReject:
			tenantIndex, ok := serviceTenantIndex(job.tenant.id)
			if !ok || job.tenant != serviceTenants[tenantIndex] {
				return errInvalidWorkload
			}
		case jobGlobalReject:
			if identifier != workload.globalRejection ||
				job.tenant != globalProbeTenant {
				return errInvalidWorkload
			}
		default:
			return errInvalidWorkload
		}
	}
	for tenantIndex := range serviceTenants {
		if len(workload.serviceQueues[tenantIndex]) != ciServiceRequestsPerTenant ||
			streams[tenantIndex] != 1 ||
			kindCounts[tenantIndex][jobService] != ciServiceRequestsPerTenant ||
			kindCounts[tenantIndex][jobCancel] != 1 ||
			kindCounts[tenantIndex][jobDeadline] != 1 ||
			kindCounts[tenantIndex][jobReject] != 1 {
			return errInvalidWorkload
		}
	}
	expectedQueues := [len(serviceTenants)][]uint64{}
	for _, identifier := range append(
		append([]uint64(nil), workload.initialService...),
		workload.lateService...,
	) {
		job := workload.jobs[identifier]
		tenantIndex, _ := serviceTenantIndex(job.tenant.id)
		expectedQueues[tenantIndex] = append(expectedQueues[tenantIndex], identifier)
	}
	for tenantIndex := range serviceTenants {
		for position, identifier := range expectedQueues[tenantIndex] {
			if workload.serviceQueues[tenantIndex][position] != identifier {
				return errInvalidWorkload
			}
		}
	}
	return nil
}

type dispatchRecord struct {
	Position  uint64 `json:"position"`
	Tenant    string `json:"tenant"`
	Weight    uint64 `json:"weight"`
	WorkUnits uint64 `json:"work_units"`
	Mode      string `json:"mode"`
}

type oracleTenant struct {
	definition tenantDefinition
	queue      []uint64
	deficit    uint64
	visitOpen  bool
}

type oracleDispatch struct {
	identifier uint64
	record     dispatchRecord
}

func expectedDispatchTrace(workload seededWorkload) ([]dispatchRecord, error) {
	plan, err := expectedDispatchPlan(workload)
	if err != nil {
		return nil, err
	}
	trace := make([]dispatchRecord, len(plan))
	for index, step := range plan {
		trace[index] = step.record
	}
	return trace, nil
}

func expectedDispatchPlan(workload seededWorkload) ([]oracleDispatch, error) {
	tenants := make([]oracleTenant, len(serviceTenants))
	remaining := 0
	for index, definition := range serviceTenants {
		tenants[index] = oracleTenant{
			definition: definition,
			queue:      append([]uint64(nil), workload.serviceQueues[index]...),
		}
		remaining += len(tenants[index].queue)
	}

	expectedCount := remaining
	trace := make([]oracleDispatch, 0, expectedCount)
	cursor := 0
	maximumVisits := remaining * int(ciDeficitCap/ciBaseQuantum+2) * len(tenants)
	for visits := 0; remaining > 0 && visits < maximumVisits; visits++ {
		tenant := &tenants[cursor]
		if len(tenant.queue) == 0 {
			tenant.deficit = 0
			tenant.visitOpen = false
			cursor = (cursor + 1) % len(tenants)
			continue
		}
		if !tenant.visitOpen {
			tenant.deficit = boundedOracleAdd(
				tenant.deficit,
				ciBaseQuantum*tenant.definition.weight,
				ciDeficitCap,
			)
			tenant.visitOpen = true
		}

		job, ok := workload.jobs[tenant.queue[0]]
		if !ok || job.kind != jobService || job.tenant.id != tenant.definition.id {
			return nil, errInvalidWorkload
		}
		if job.workUnits > tenant.deficit {
			tenant.visitOpen = false
			cursor = (cursor + 1) % len(tenants)
			continue
		}

		tenant.deficit -= job.workUnits
		tenant.queue = tenant.queue[1:]
		remaining--
		trace = append(trace, oracleDispatch{
			identifier: job.id,
			record: dispatchRecord{
				Position:  uint64(len(trace) + 1),
				Tenant:    tenant.definition.label,
				Weight:    tenant.definition.weight,
				WorkUnits: job.workUnits,
				Mode:      job.mode(),
			},
		})
		if len(tenant.queue) == 0 {
			tenant.deficit = 0
			tenant.visitOpen = false
			cursor = (cursor + 1) % len(tenants)
		}
	}
	if remaining != 0 || len(trace) != expectedCount {
		return nil, errInvalidWorkload
	}
	return trace, nil
}

func boundedOracleAdd(value, addition, maximum uint64) uint64 {
	if value >= maximum || addition > maximum-value {
		return maximum
	}
	return value + addition
}

func serviceTenantIndex(identifier admission.TenantID) (int, bool) {
	for index, tenant := range serviceTenants {
		if tenant.id == identifier {
			return index, true
		}
	}
	return 0, false
}

type splitMix64 struct {
	state uint64
}

func (g *splitMix64) next() uint64 {
	g.state += 0x9e3779b97f4a7c15
	value := g.state
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}

func shuffle[T any](generator *splitMix64, values []T) {
	for index := len(values) - 1; index > 0; index-- {
		swap := int(generator.next() % uint64(index+1))
		values[index], values[swap] = values[swap], values[index]
	}
}
