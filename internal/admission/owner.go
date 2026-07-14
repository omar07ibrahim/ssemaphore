package admission

import (
	"container/heap"
	"container/list"
	"context"
	"errors"
	"time"
)

var (
	errCanceledByClient = errors.New("admission canceled by client")
	errCanceledByDrain  = errors.New("admission canceled by drain")
	errRequestFinished  = errors.New("admission request finished")
)

type entryState uint8

const (
	entryQueued entryState = iota + 1
	entryInflight
)

type accountingPhase uint8

const (
	accountingQueued accountingPhase = iota + 1
	accountingInflight
)

type deadlineSource uint8

const (
	deadlineQueue deadlineSource = iota + 1
	deadlineClient
)

type entry struct {
	id             uint64
	sequence       uint64
	admission      Admission
	clientCtx      context.Context
	deadline       time.Time
	deadlineSource deadlineSource
	state          entryState
	phase          accountingPhase
	tenant         int

	queueElement    *list.Element
	inflightElement *list.Element
	heapIndex       int
	result          chan acquireResult
	permit          *Permit
	cancel          context.CancelCauseFunc
	cancelCause     cancelCause
}

type acquireResult struct {
	permit   *Permit
	decision Decision
}

type tenantState struct {
	config    validatedTenant
	queue     list.List
	deficit   uint64
	visitOpen bool
	counters  Counters
}

type ownerState struct {
	scheduler     *Scheduler
	config        validatedConfig
	tenants       []tenantState
	entries       map[uint64]*entry
	expiry        expiryQueue
	inflight      list.List
	cursor        int
	sequence      uint64
	accepting     bool
	global        Counters
	drainNotified bool
}

type admitCommand struct{ item *entry }

type cancelCommand struct {
	entryID  uint64
	response chan CancelResult
}

type finishCommand struct {
	entryID  uint64
	outcome  ServingOutcome
	response chan TerminalResult
}

type snapshotCommand struct{ response chan Snapshot }

type drainCommand struct{ response chan DrainResult }

type forceCancelCommand struct{ response chan ForceCancelResult }

type stopCommand struct{ acknowledged chan struct{} }

func (s *Scheduler) run() {
	state := newOwnerState(s)
	expiryTimer := s.clock.NewTimer(time.Hour)
	expiryTimer.Stop()
	var expiryChannel <-chan time.Time

	for {
		state.resetExpiryTimer(s.clock.Now(), expiryTimer, &expiryChannel)
		select {
		case rawCommand := <-s.commands:
			now := s.clock.Now()
			state.expireReady(now)
			if state.handle(rawCommand, now) {
				expiryTimer.Stop()
				close(s.done)
				return
			}
			state.pump()
			state.notifyDrained()
		case <-expiryChannel:
			now := s.clock.Now()
			state.expireReady(now)
			state.pump()
			state.notifyDrained()
		}
	}
}

func newOwnerState(scheduler *Scheduler) *ownerState {
	state := &ownerState{
		scheduler: scheduler,
		config:    scheduler.config,
		tenants:   make([]tenantState, len(scheduler.config.tenants)),
		entries:   make(map[uint64]*entry),
		accepting: true,
	}
	for index, tenant := range scheduler.config.tenants {
		state.tenants[index].config = tenant
	}
	heap.Init(&state.expiry)
	return state
}

func (s *ownerState) handle(rawCommand any, now time.Time) bool {
	switch command := rawCommand.(type) {
	case admitCommand:
		s.admit(command.item, now)
	case cancelCommand:
		command.response <- s.cancelEntry(command.entryID)
	case finishCommand:
		command.response <- s.finishEntry(command.entryID, command.outcome)
	case snapshotCommand:
		command.response <- s.snapshot()
	case drainCommand:
		command.response <- s.beginDrain()
	case forceCancelCommand:
		command.response <- s.forceCancelInflight()
	case stopCommand:
		if !s.isDrained() {
			panic("admission: stop requested before drain")
		}
		close(command.acknowledged)
		return true
	default:
		panic("admission: unknown owner command")
	}
	return false
}

func (s *ownerState) admit(item *entry, now time.Time) {
	if !s.accepting {
		s.resolve(item, nil, Decision{Kind: DecisionDraining})
		return
	}
	if item.clientCtx.Err() != nil {
		s.resolve(item, nil, Decision{Kind: DecisionCanceledQueued})
		return
	}
	tenantIndex, exists := s.config.tenantByID[item.admission.Tenant]
	if !exists {
		s.resolve(item, nil, Decision{Kind: DecisionInvalid})
		return
	}

	if !item.deadline.After(now) {
		s.resolve(item, nil, item.deadlineDecision())
		return
	}

	tenant := &s.tenants[tenantIndex]
	if resource := queueLimitFailure(tenant.counters, item.admission, tenant.config.queue); resource != ResourceNone {
		s.resolve(item, nil, Decision{Kind: DecisionTenantRejected, Resource: resource})
		return
	}
	if resource := queueLimitFailure(s.global, item.admission, s.config.globalQueue); resource != ResourceNone {
		s.resolve(item, nil, Decision{Kind: DecisionGlobalRejected, Resource: resource})
		return
	}

	s.sequence++
	item.sequence = s.sequence
	item.state = entryQueued
	item.phase = accountingQueued
	item.tenant = tenantIndex
	item.queueElement = tenant.queue.PushBack(item)
	tenant.counters.addQueued(item.admission)
	s.global.addQueued(item.admission)
	s.entries[item.id] = item
	heap.Push(&s.expiry, item)
}

func (s *ownerState) cancelEntry(entryID uint64) CancelResult {
	item := s.entries[entryID]
	if item == nil {
		return CancelAlreadyTerminal
	}
	if item.state == entryQueued {
		s.removeQueued(item)
		delete(s.entries, item.id)
		s.resolve(item, nil, Decision{Kind: DecisionCanceledQueued})
		return CancelApplied
	}
	if item.cancelCause != 0 {
		return CancelAlreadyRequested
	}
	item.cancelCause = cancelClient
	item.cancel(errCanceledByClient)
	return CancelApplied
}

func (s *ownerState) finishEntry(entryID uint64, outcome ServingOutcome) TerminalResult {
	item := s.entries[entryID]
	if item == nil || item.state != entryInflight || item.phase != accountingInflight {
		return TerminalResult{Outcome: TerminalInternalFailure}
	}

	if item.cancelCause == 0 && item.clientCtx.Err() != nil {
		item.cancelCause = cancelClient
	}
	terminal := terminalOutcome(item.cancelCause, outcome)
	tenant := &s.tenants[item.tenant]
	tenant.counters.removeInflight(item.admission)
	s.global.removeInflight(item.admission)
	item.phase = 0
	s.inflight.Remove(item.inflightElement)
	item.cancel(errRequestFinished)
	delete(s.entries, item.id)
	return TerminalResult{Outcome: terminal, AccountingReleased: true}
}

func (s *ownerState) beginDrain() DrainResult {
	s.accepting = false
	result := DrainResult{}
	for tenantIndex := range s.tenants {
		tenant := &s.tenants[tenantIndex]
		for tenant.queue.Len() > 0 {
			item := tenant.queue.Front().Value.(*entry)
			s.removeQueued(item)
			delete(s.entries, item.id)
			if item.clientCtx.Err() != nil {
				s.resolve(item, nil, Decision{Kind: DecisionCanceledQueued})
			} else {
				s.resolve(item, nil, Decision{Kind: DecisionShutdown})
				result.QueuedShutdownCanceled++
			}
		}
	}
	result.InFlightActiveAtStart = s.global.InflightCount
	return result
}

func (s *ownerState) forceCancelInflight() ForceCancelResult {
	if s.accepting {
		s.beginDrain()
	}
	var signaled uint64
	for element := s.inflight.Front(); element != nil; element = element.Next() {
		item := element.Value.(*entry)
		if item.cancelCause == 0 {
			if item.clientCtx.Err() != nil {
				item.cancelCause = cancelClient
				item.cancel(errCanceledByClient)
				continue
			}
			item.cancelCause = cancelShutdown
			item.cancel(errCanceledByDrain)
			signaled++
		}
	}
	return ForceCancelResult{NewlyShutdownSignaled: signaled}
}

func (s *ownerState) pump() {
	if len(s.tenants) == 0 {
		return
	}
	if s.global.InflightCount == s.config.globalInflight.Count ||
		s.global.InflightWork == s.config.globalInflight.Work {
		return
	}
	for s.global.QueuedCount > 0 {
		dispatched := false
		needsFunding := false
		for range len(s.tenants) {
			tenant := &s.tenants[s.cursor]
			for tenant.queue.Len() > 0 {
				head := tenant.queue.Front().Value.(*entry)
				if head.clientCtx.Err() != nil {
					s.removeQueued(head)
					delete(s.entries, head.id)
					s.resolve(head, nil, Decision{Kind: DecisionCanceledQueued})
					continue
				}
				if !head.deadline.After(s.scheduler.clock.Now()) {
					s.expire(head)
					continue
				}
				break
			}

			if tenant.queue.Len() == 0 {
				tenant.deficit = 0
				tenant.visitOpen = false
				s.advance()
				continue
			}
			if !tenant.visitOpen {
				tenant.deficit = boundedAdd(tenant.deficit, tenant.config.quantum, s.config.deficitCap)
				tenant.visitOpen = true
			}

			for tenant.queue.Len() > 0 {
				head := tenant.queue.Front().Value.(*entry)
				if !inflightFits(tenant.counters, head.admission, tenant.config.inflight) {
					break
				}
				if head.admission.WorkUnits > tenant.deficit {
					needsFunding = true
					tenant.visitOpen = false
					break
				}
				if head.clientCtx.Err() != nil {
					s.removeQueued(head)
					delete(s.entries, head.id)
					s.resolve(head, nil, Decision{Kind: DecisionCanceledQueued})
					continue
				}
				if !head.deadline.After(s.scheduler.clock.Now()) {
					s.expire(head)
					continue
				}
				// Once a head is funded, global capacity is reserved for it. This
				// prevents a stream of smaller requests from starving an older,
				// larger request under the work limit.
				if !inflightFits(s.global, head.admission, s.config.globalInflight) {
					return
				}
				tenant.deficit -= head.admission.WorkUnits
				s.dispatch(head)
				dispatched = true
				if s.global.InflightCount == s.config.globalInflight.Count ||
					s.global.InflightWork == s.config.globalInflight.Work {
					if tenant.queue.Len() == 0 {
						tenant.deficit = 0
						tenant.visitOpen = false
						s.advance()
					}
					return
				}
				if tenant.queue.Len() == 0 {
					tenant.deficit = 0
					tenant.visitOpen = false
					break
				}
			}
			s.advance()
		}
		if s.global.QueuedCount == 0 {
			s.cursor = 0
			return
		}
		if !dispatched && !needsFunding {
			return
		}
	}
}

func (s *ownerState) dispatch(item *entry) {
	tenant := &s.tenants[item.tenant]
	s.removeQueued(item)
	tenant.counters.addInflight(item.admission)
	s.global.addInflight(item.admission)
	item.state = entryInflight
	item.phase = accountingInflight
	permitContext, cancel := context.WithCancelCause(item.clientCtx)
	item.cancel = cancel
	item.inflightElement = s.inflight.PushBack(item)
	item.permit = &Permit{
		scheduler:  s.scheduler,
		entryID:    item.id,
		ctx:        permitContext,
		finishDone: make(chan struct{}),
	}
	s.resolve(item, item.permit, Decision{Kind: DecisionDispatched})
}

func (s *ownerState) removeQueued(item *entry) {
	if item.phase != accountingQueued {
		panic("admission: queued accounting released twice")
	}
	tenant := &s.tenants[item.tenant]
	tenant.queue.Remove(item.queueElement)
	s.expiry.remove(item)
	tenant.counters.removeQueued(item.admission)
	s.global.removeQueued(item.admission)
	item.queueElement = nil
	item.phase = 0
	if tenant.queue.Len() == 0 {
		tenant.deficit = 0
		tenant.visitOpen = false
	}
}

func (s *ownerState) expireReady(now time.Time) {
	for len(s.expiry) > 0 {
		item := s.expiry[0]
		if item.deadline.After(now) {
			return
		}
		s.expire(item)
	}
}

func (s *ownerState) expire(item *entry) {
	s.removeQueued(item)
	delete(s.entries, item.id)
	s.resolve(item, nil, item.deadlineDecision())
}

func (e *entry) deadlineDecision() Decision {
	if e.clientCtx.Err() != nil || e.deadlineSource == deadlineClient {
		return Decision{Kind: DecisionCanceledQueued}
	}
	return Decision{Kind: DecisionQueueExpired}
}

func (s *ownerState) resolve(item *entry, permit *Permit, decision Decision) {
	item.result <- acquireResult{permit: permit, decision: decision}
	close(item.result)
}

func (s *ownerState) advance() {
	s.cursor++
	if s.cursor == len(s.tenants) {
		s.cursor = 0
	}
}

func (s *ownerState) snapshot() Snapshot {
	snapshot := Snapshot{
		Accepting: s.accepting,
		Global:    s.global,
		Tenants:   make([]TenantSnapshot, len(s.tenants)),
	}
	for index, tenant := range s.tenants {
		snapshot.Tenants[index] = TenantSnapshot{
			ID:       tenant.config.id,
			Deficit:  tenant.deficit,
			Counters: tenant.counters,
		}
	}
	return snapshot
}

func (s *ownerState) isDrained() bool {
	return s.global.QueuedCount == 0 && s.global.InflightCount == 0
}

func (s *ownerState) notifyDrained() {
	if s.accepting || !s.isDrained() || s.drainNotified {
		return
	}
	s.drainNotified = true
	close(s.scheduler.drained)
}

func (s *ownerState) resetExpiryTimer(now time.Time, expiryTimer timer, channel *<-chan time.Time) {
	if !expiryTimer.Stop() {
		select {
		case <-expiryTimer.C():
		default:
		}
	}
	if len(s.expiry) == 0 {
		*channel = nil
		return
	}
	duration := s.expiry[0].deadline.Sub(now)
	if duration < 0 {
		duration = 0
	}
	expiryTimer.Reset(duration)
	*channel = expiryTimer.C()
}

func queueLimitFailure(counters Counters, admission Admission, limits QueueLimits) ResourceKind {
	if !fits(counters.QueuedCount, 1, limits.Count) {
		return ResourceCount
	}
	if !fits(counters.QueuedBytes, admission.BodyBytes, limits.Bytes) {
		return ResourceBytes
	}
	if !fits(counters.QueuedWork, admission.WorkUnits, limits.Work) {
		return ResourceWork
	}
	return ResourceNone
}

func inflightFits(counters Counters, admission Admission, limits InflightLimits) bool {
	return fits(counters.InflightCount, 1, limits.Count) &&
		fits(counters.InflightWork, admission.WorkUnits, limits.Work)
}

func fits(current, addition, limit uint64) bool {
	return current <= limit && addition <= limit-current
}

func boundedAdd(value, addition, maximum uint64) uint64 {
	if value >= maximum || addition > maximum-value {
		return maximum
	}
	return value + addition
}

func (c *Counters) addQueued(admission Admission) {
	c.QueuedCount++
	c.QueuedBytes += admission.BodyBytes
	c.QueuedWork += admission.WorkUnits
}

func (c *Counters) removeQueued(admission Admission) {
	if c.QueuedCount == 0 || c.QueuedBytes < admission.BodyBytes || c.QueuedWork < admission.WorkUnits {
		panic("admission: queued accounting underflow")
	}
	c.QueuedCount--
	c.QueuedBytes -= admission.BodyBytes
	c.QueuedWork -= admission.WorkUnits
}

func (c *Counters) addInflight(admission Admission) {
	c.InflightCount++
	c.InflightWork += admission.WorkUnits
}

func (c *Counters) removeInflight(admission Admission) {
	if c.InflightCount == 0 || c.InflightWork < admission.WorkUnits {
		panic("admission: in-flight accounting underflow")
	}
	c.InflightCount--
	c.InflightWork -= admission.WorkUnits
}

func terminalOutcome(cancelCause cancelCause, serving ServingOutcome) TerminalOutcome {
	if cancelCause == cancelShutdown {
		return TerminalShutdown
	}
	if cancelCause == cancelClient {
		return TerminalCanceledInflight
	}
	switch serving {
	case ServingCompleted:
		return TerminalCompleted
	case ServingUpstreamFailed:
		return TerminalUpstreamFailed
	case ServingDownstreamFailed:
		return TerminalDownstreamFailed
	case ServingCanceled:
		return TerminalCanceledInflight
	default:
		return TerminalInternalFailure
	}
}
