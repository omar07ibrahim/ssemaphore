// Package admission owns bounded queue admission, weighted deficit scheduling,
// cancellation linearization, and exact-once in-flight release.
package admission

import (
	"context"
	"sync"
	"time"
)

type TenantID uint32

type QueueLimits struct {
	Count uint64
	Bytes uint64
	Work  uint64
}

type InflightLimits struct {
	Count uint64
	Work  uint64
}

type TenantConfig struct {
	ID       TenantID
	Weight   uint64
	Queue    QueueLimits
	Inflight InflightLimits
}

// Config defines all scheduler safety bounds. Validation caps configurations at
// 1,024 tenants, 65,536 queued requests, 4,096 in-flight requests, 65,536
// funding visits per scheduling opportunity, and a one-hour queue timeout.
type Config struct {
	MaxBodyBytes    uint64
	MaxRequestUnits uint64
	BaseQuantum     uint64
	DeficitCap      uint64
	GlobalQueue     QueueLimits
	GlobalInflight  InflightLimits
	Tenants         []TenantConfig
}

// Admission is the immutable resource reservation presented to the scheduler.
// QueueTimeout starts when Acquire is called, before the owner mailbox accepts
// the request.
type Admission struct {
	Tenant       TenantID
	BodyBytes    uint64
	WorkUnits    uint64
	QueueTimeout time.Duration
}

type DecisionKind uint8

const (
	DecisionDispatched DecisionKind = iota + 1
	DecisionTenantRejected
	DecisionGlobalRejected
	DecisionQueueExpired
	DecisionCanceledQueued
	DecisionCanceledBeforeStart
	DecisionShutdown
	DecisionDraining
	DecisionInvalid
)

type ResourceKind uint8

const (
	ResourceNone ResourceKind = iota
	ResourceCount
	ResourceBytes
	ResourceWork
)

type Decision struct {
	Kind     DecisionKind
	Resource ResourceKind
}

type cancelCause uint8

const (
	cancelClient cancelCause = iota + 1
	cancelShutdown
)

type CancelResult uint8

const (
	CancelApplied CancelResult = iota + 1
	CancelAlreadyRequested
	CancelAlreadyTerminal
)

type ServingOutcome uint8

const (
	ServingCompleted ServingOutcome = iota + 1
	ServingUpstreamFailed
	ServingDownstreamFailed
	ServingCanceled
	ServingInternalFailure
)

type TerminalOutcome uint8

const (
	TerminalCompleted TerminalOutcome = iota + 1
	TerminalUpstreamFailed
	TerminalDownstreamFailed
	TerminalCanceledInflight
	TerminalShutdown
	TerminalInternalFailure
)

type TerminalResult struct {
	Outcome            TerminalOutcome
	AccountingReleased bool
}

type Counters struct {
	QueuedCount   uint64
	QueuedBytes   uint64
	QueuedWork    uint64
	InflightCount uint64
	InflightWork  uint64
}

type TenantSnapshot struct {
	ID       TenantID
	Deficit  uint64
	Counters Counters
}

type Snapshot struct {
	Accepting bool
	Global    Counters
	Tenants   []TenantSnapshot
}

type DrainResult struct {
	QueuedShutdownCanceled uint64
	InFlightActiveAtStart  uint64
}

// ForceCancelResult reports only permits newly attributed to shutdown. A
// permit already canceled by its client remains client-attributed.
type ForceCancelResult struct {
	NewlyShutdownSignaled uint64
}

type Permit struct {
	scheduler *Scheduler
	entryID   uint64
	ctx       context.Context

	finishOnce   sync.Once
	finishDone   chan struct{}
	finishResult TerminalResult
}

// Context is the only context that should be attached to upstream work.
func (p *Permit) Context() context.Context {
	return p.ctx
}

// RequestCancel records a client-side cancellation. It signals Context but
// does not release accounting; Finish remains mandatory. Shutdown cancellation
// is owner-controlled through ForceCancelInflight.
func (p *Permit) RequestCancel() CancelResult {
	return p.scheduler.cancel(p.entryID)
}

// Finish releases in-flight accounting exactly once. Concurrent or repeated
// callers receive the same cached terminal result; AccountingReleased means
// the permit's accounting was released, not that the current caller did it.
func (p *Permit) Finish(outcome ServingOutcome) TerminalResult {
	p.finishOnce.Do(func() {
		p.finishResult = p.scheduler.finish(p.entryID, outcome)
		close(p.finishDone)
	})
	<-p.finishDone
	return p.finishResult
}
