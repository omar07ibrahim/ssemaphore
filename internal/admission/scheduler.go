package admission

import (
	"context"
	"errors"
	"sync/atomic"
)

var errSchedulerClosed = errors.New("admission scheduler is closed")

// Scheduler serializes all queue, fairness, cancellation, and accounting
// transitions through one owner goroutine.
type Scheduler struct {
	config   validatedConfig
	clock    clock
	commands chan any
	drained  chan struct{}
	done     chan struct{}
	nextID   atomic.Uint64
}

// New validates every configured bound before starting the scheduler.
func New(config Config) (*Scheduler, error) {
	return newScheduler(config, systemClock{})
}

// MaxBodyBytes reports the validated per-request body bound.
func (s *Scheduler) MaxBodyBytes() uint64 { return s.config.maxBodyBytes }

// MaxRequestUnits reports the validated per-request work bound.
func (s *Scheduler) MaxRequestUnits() uint64 { return s.config.maxRequestUnits }

// GlobalQueueLimits returns a copy of the validated global queue limits.
func (s *Scheduler) GlobalQueueLimits() QueueLimits { return s.config.globalQueue }

// TenantQueueLimits returns a copy of a configured tenant's validated queue
// limits. The boolean is false when id is not configured.
func (s *Scheduler) TenantQueueLimits(id TenantID) (QueueLimits, bool) {
	index, exists := s.config.tenantByID[id]
	if !exists {
		return QueueLimits{}, false
	}
	return s.config.tenants[index].queue, true
}

// HasTenant reports whether id belongs to the scheduler's validated tenant set.
func (s *Scheduler) HasTenant(id TenantID) bool {
	_, exists := s.config.tenantByID[id]
	return exists
}

func newScheduler(config Config, schedulerClock clock) (*Scheduler, error) {
	validated, err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	if schedulerClock == nil {
		return nil, errors.New("clock must not be nil")
	}

	scheduler := &Scheduler{
		config:   validated,
		clock:    schedulerClock,
		commands: make(chan any, 64),
		drained:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	go scheduler.run()
	return scheduler, nil
}

// Acquire blocks until the request is rejected, expires, is canceled, or owns
// an in-flight Permit. A returned Permit must be finished exactly once by the
// worker, including after cancellation.
func (s *Scheduler) Acquire(ctx context.Context, admission Admission) (*Permit, Decision) {
	if ctx == nil {
		return nil, Decision{Kind: DecisionInvalid}
	}
	if decision := validateAdmission(s.config, admission); decision.Kind != 0 {
		return nil, decision
	}
	if ctx.Err() != nil {
		return nil, Decision{Kind: DecisionCanceledQueued}
	}

	now := s.clock.Now()
	deadline := now.Add(admission.QueueTimeout)
	deadlineSource := deadlineQueue
	if clientDeadline, hasDeadline := ctx.Deadline(); hasDeadline && !clientDeadline.After(deadline) {
		deadline = clientDeadline
		deadlineSource = deadlineClient
	}
	if !deadline.After(now) {
		if deadlineSource == deadlineClient {
			return nil, Decision{Kind: DecisionCanceledQueued}
		}
		return nil, Decision{Kind: DecisionQueueExpired}
	}

	item := &entry{
		id:             s.nextID.Add(1),
		admission:      admission,
		clientCtx:      ctx,
		deadline:       deadline,
		deadlineSource: deadlineSource,
		result:         make(chan acquireResult, 1),
		heapIndex:      -1,
	}
	command := admitCommand{item: item}
	select {
	case s.commands <- command:
	case <-ctx.Done():
		return nil, Decision{Kind: DecisionCanceledQueued}
	case <-s.done:
		return nil, Decision{Kind: DecisionDraining}
	}

	select {
	case result := <-item.result:
		if result.permit != nil && ctx.Err() != nil {
			s.cancel(item.id)
			result.permit.Finish(ServingCanceled)
			return nil, Decision{Kind: DecisionCanceledBeforeStart}
		}
		return result.permit, result.decision
	case <-ctx.Done():
		s.cancel(item.id)
		select {
		case result := <-item.result:
			if result.permit != nil {
				result.permit.Finish(ServingCanceled)
				return nil, Decision{Kind: DecisionCanceledBeforeStart}
			}
			return nil, result.decision
		case <-s.done:
			select {
			case result := <-item.result:
				if result.permit != nil {
					result.permit.Finish(ServingCanceled)
					return nil, Decision{Kind: DecisionCanceledBeforeStart}
				}
				return nil, result.decision
			default:
				return nil, Decision{Kind: DecisionCanceledQueued}
			}
		}
	case <-s.done:
		select {
		case result := <-item.result:
			return result.permit, result.decision
		default:
			return nil, Decision{Kind: DecisionDraining}
		}
	}
}

func (s *Scheduler) Snapshot(ctx context.Context) (Snapshot, error) {
	response := make(chan Snapshot, 1)
	if err := s.send(ctx, snapshotCommand{response: response}); err != nil {
		return Snapshot{}, err
	}
	select {
	case snapshot := <-response:
		return snapshot, nil
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case <-s.done:
		select {
		case snapshot := <-response:
			return snapshot, nil
		default:
			return Snapshot{}, errSchedulerClosed
		}
	}
}

// BeginDrain stops admission and cancels queued requests without signaling
// active permits. Once its command enters the owner mailbox, ctx no longer
// changes the committed result.
func (s *Scheduler) BeginDrain(ctx context.Context) (DrainResult, error) {
	response := make(chan DrainResult, 1)
	if err := s.sendMutating(ctx, drainCommand{response: response}); err != nil {
		return DrainResult{}, err
	}
	select {
	case result := <-response:
		return result, nil
	case <-s.done:
		select {
		case result := <-response:
			return result, nil
		default:
			return DrainResult{}, errSchedulerClosed
		}
	}
}

// ForceCancelInflight ends the graceful phase, starts drain if necessary, and
// signals every active permit not already canceled by its client. Capacity
// remains held until each worker calls Finish. Once accepted by the owner, ctx
// no longer changes the committed result.
func (s *Scheduler) ForceCancelInflight(ctx context.Context) (ForceCancelResult, error) {
	response := make(chan ForceCancelResult, 1)
	if err := s.sendMutating(ctx, forceCancelCommand{response: response}); err != nil {
		return ForceCancelResult{}, err
	}
	select {
	case result := <-response:
		return result, nil
	case <-s.done:
		select {
		case result := <-response:
			return result, nil
		default:
			return ForceCancelResult{}, errSchedulerClosed
		}
	}
}

// WaitDrained waits for BeginDrain and for all permits to be finished. Calling
// it before drain starts blocks; a completed drain wins a race with ctx.
func (s *Scheduler) WaitDrained(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	select {
	case <-s.drained:
		return nil
	default:
	}
	select {
	case <-s.drained:
		return nil
	case <-ctx.Done():
		select {
		case <-s.drained:
			return nil
		default:
			return ctx.Err()
		}
	case <-s.done:
		return nil
	}
}

// Close begins drain, waits for workers to finish, and stops the owner. It does
// not run a grace timer or force cancellation; callers orchestrate that with
// ForceCancelInflight when required.
func (s *Scheduler) Close(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	default:
	}
	if _, err := s.BeginDrain(ctx); err != nil && !errors.Is(err, errSchedulerClosed) {
		return err
	}
	select {
	case <-s.done:
		return nil
	default:
	}
	if err := s.WaitDrained(ctx); err != nil {
		return err
	}

	acknowledged := make(chan struct{})
	// Drained is terminal and no remaining worker can prolong this bounded
	// owner command. Finalization must not lose a race to the grace context.
	if err := s.sendMutating(context.Background(), stopCommand{acknowledged: acknowledged}); errors.Is(err, errSchedulerClosed) {
		return nil
	} else if err != nil {
		return err
	}
	select {
	case <-acknowledged:
		return nil
	case <-s.done:
		return nil
	}
}

func (s *Scheduler) cancel(entryID uint64) CancelResult {
	response := make(chan CancelResult, 1)
	select {
	case s.commands <- cancelCommand{entryID: entryID, response: response}:
	case <-s.done:
		return CancelAlreadyTerminal
	}
	select {
	case result := <-response:
		return result
	case <-s.done:
		select {
		case result := <-response:
			return result
		default:
			return CancelAlreadyTerminal
		}
	}
}

func (s *Scheduler) finish(entryID uint64, outcome ServingOutcome) TerminalResult {
	response := make(chan TerminalResult, 1)
	select {
	case s.commands <- finishCommand{entryID: entryID, outcome: outcome, response: response}:
	case <-s.done:
		return TerminalResult{Outcome: TerminalInternalFailure}
	}
	select {
	case result := <-response:
		return result
	case <-s.done:
		select {
		case result := <-response:
			return result
		default:
			return TerminalResult{Outcome: TerminalInternalFailure}
		}
	}
}

func (s *Scheduler) send(ctx context.Context, command any) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	select {
	case s.commands <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errSchedulerClosed
	}
}

// sendMutating uses ctx only until the owner accepts the command. Once
// accepted, the mutation has a single observable result even if ctx is then
// canceled.
func (s *Scheduler) sendMutating(ctx context.Context, command any) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	select {
	case <-s.done:
		return errSchedulerClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.commands <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errSchedulerClosed
	}
}
