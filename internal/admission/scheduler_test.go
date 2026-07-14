package admission

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

type acquisition struct {
	label    string
	work     uint64
	permit   *Permit
	decision Decision
}

func newTestScheduler(t *testing.T, config Config) (*Scheduler, *fakeClock) {
	t.Helper()
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	scheduler, err := newScheduler(config, clock)
	if err != nil {
		t.Fatalf("newScheduler() error = %v", err)
	}
	return scheduler, clock
}

func closeScheduler(t *testing.T, scheduler *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := scheduler.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func acquireAsync(scheduler *Scheduler, ctx context.Context, label string, admission Admission, results chan<- acquisition) {
	go func() {
		permit, decision := scheduler.Acquire(ctx, admission)
		results <- acquisition{label: label, work: admission.WorkUnits, permit: permit, decision: decision}
	}()
}

func waitSnapshot(t *testing.T, scheduler *Scheduler, predicate func(Snapshot) bool) Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		snapshot, err := scheduler.Snapshot(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Snapshot() error = %v", err)
		}
		if predicate(snapshot) {
			return snapshot
		}
		runtime.Gosched()
	}
	t.Fatal("scheduler snapshot did not reach the expected state")
	return Snapshot{}
}

func acquireNow(t *testing.T, scheduler *Scheduler, admission Admission) *Permit {
	t.Helper()
	permit, decision := scheduler.Acquire(context.Background(), admission)
	if decision.Kind != DecisionDispatched || permit == nil {
		t.Fatalf("Acquire() = (%v, %+v), want dispatched permit", permit, decision)
	}
	return permit
}

func defaultAdmission(tenant TenantID, work uint64) Admission {
	return Admission{Tenant: tenant, BodyBytes: 10, WorkUnits: work, QueueTimeout: time.Minute}
}

func TestNewRunsWithSystemClock(t *testing.T) {
	scheduler, err := New(baseConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	permit := acquireNow(t, scheduler, defaultAdmission(1, 10))
	if terminal := permit.Finish(ServingCompleted); terminal != (TerminalResult{Outcome: TerminalCompleted, AccountingReleased: true}) {
		t.Fatalf("Finish() = %+v", terminal)
	}
	closeScheduler(t, scheduler)
}

func TestWeightedDRRPreservesOpenVisitAtOneInflightSlot(t *testing.T) {
	config := baseConfig()
	scheduler, _ := newTestScheduler(t, config)

	blocker := acquireNow(t, scheduler, defaultAdmission(1, 10))
	results := make(chan acquisition, 8)
	queued := uint64(0)
	for index := range 4 {
		acquireAsync(scheduler, context.Background(), "A", defaultAdmission(1, 10), results)
		queued++
		waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == queued })
		acquireAsync(scheduler, context.Background(), "B", defaultAdmission(2, 10), results)
		queued++
		waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == queued })
		_ = index
	}

	if result := blocker.Finish(ServingCompleted); result.Outcome != TerminalCompleted || !result.AccountingReleased {
		t.Fatalf("blocker Finish() = %+v", result)
	}

	trace := make([]string, 0, 8)
	for range 8 {
		result := <-results
		if result.decision.Kind != DecisionDispatched || result.permit == nil {
			t.Fatalf("queued Acquire() = %+v", result)
		}
		trace = append(trace, result.label)
		result.permit.Finish(ServingCompleted)
	}
	want := []string{"B", "B", "B", "A", "B", "A", "A", "A"}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("dispatch trace = %v, want %v", trace, want)
	}
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
		return snapshot.Global == (Counters{})
	})
	closeScheduler(t, scheduler)
}

func TestWeightedDRRMatchesIndependentOracle(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		t.Run(time.Unix(seed, 0).Format("150405"), func(t *testing.T) {
			config := Config{
				MaxBodyBytes:    10,
				MaxRequestUnits: 100,
				BaseQuantum:     13,
				DeficitCap:      256,
				GlobalQueue:     QueueLimits{Count: 64, Bytes: 640, Work: 6_400},
				GlobalInflight:  InflightLimits{Count: 1, Work: 100},
				Tenants: []TenantConfig{
					{ID: 99, Weight: 1, Queue: QueueLimits{Count: 64, Bytes: 640, Work: 6_400}, Inflight: InflightLimits{Count: 1, Work: 100}},
					{ID: 1, Weight: 1, Queue: QueueLimits{Count: 64, Bytes: 640, Work: 6_400}, Inflight: InflightLimits{Count: 1, Work: 100}},
					{ID: 2, Weight: 3, Queue: QueueLimits{Count: 64, Bytes: 640, Work: 6_400}, Inflight: InflightLimits{Count: 1, Work: 100}},
				},
			}
			scheduler, _ := newTestScheduler(t, config)
			blocker := acquireNow(t, scheduler, Admission{Tenant: 99, BodyBytes: 1, WorkUnits: 1, QueueTimeout: time.Minute})

			random := rand.New(rand.NewSource(seed))
			queues := map[string][]uint64{"A": {}, "B": {}}
			results := make(chan acquisition, 40)
			queued := uint64(0)
			for index := range 40 {
				label := "A"
				tenant := TenantID(1)
				if random.Intn(2) == 1 {
					label = "B"
					tenant = 2
				}
				work := uint64(1 + random.Intn(100))
				queues[label] = append(queues[label], work)
				acquireAsync(scheduler, context.Background(), label, Admission{Tenant: tenant, BodyBytes: 1, WorkUnits: work, QueueTimeout: time.Minute}, results)
				queued++
				waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == queued })
				_ = index
			}

			oracle := newDRROracle(13, map[string]uint64{"A": 1, "B": 3}, queues)
			blocker.Finish(ServingCompleted)
			for range 40 {
				result := <-results
				wantLabel, wantWork := oracle.next()
				if result.label != wantLabel || result.work != wantWork {
					t.Fatalf("seed %d dispatch = (%s,%d), want (%s,%d)", seed, result.label, result.work, wantLabel, wantWork)
				}
				result.permit.Finish(ServingCompleted)
			}
			closeScheduler(t, scheduler)
		})
	}
}

func TestAdmissionDistinguishesTenantAndGlobalLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		first  Admission
		second Admission
		want   Decision
	}{
		{
			name:   "tenant count",
			mutate: func(config *Config) { config.Tenants[0].Queue.Count = 1 },
			first:  defaultAdmission(1, 10), second: defaultAdmission(1, 10),
			want: Decision{Kind: DecisionTenantRejected, Resource: ResourceCount},
		},
		{
			name:   "tenant bytes",
			mutate: func(config *Config) { config.Tenants[0].Queue.Bytes = 150 },
			first:  Admission{Tenant: 1, BodyBytes: 100, WorkUnits: 10, QueueTimeout: time.Minute},
			second: Admission{Tenant: 1, BodyBytes: 100, WorkUnits: 10, QueueTimeout: time.Minute},
			want:   Decision{Kind: DecisionTenantRejected, Resource: ResourceBytes},
		},
		{
			name:   "tenant work",
			mutate: func(config *Config) { config.Tenants[0].Queue.Work = 150 },
			first:  Admission{Tenant: 1, BodyBytes: 10, WorkUnits: 100, QueueTimeout: time.Minute},
			second: Admission{Tenant: 1, BodyBytes: 10, WorkUnits: 100, QueueTimeout: time.Minute},
			want:   Decision{Kind: DecisionTenantRejected, Resource: ResourceWork},
		},
		{
			name: "global count",
			mutate: func(config *Config) {
				config.GlobalQueue.Count = 1
				for index := range config.Tenants {
					config.Tenants[index].Queue.Count = 1
				}
			},
			first: defaultAdmission(1, 10), second: defaultAdmission(2, 10),
			want: Decision{Kind: DecisionGlobalRejected, Resource: ResourceCount},
		},
		{
			name: "global bytes",
			mutate: func(config *Config) {
				config.GlobalQueue.Bytes = 150
				for index := range config.Tenants {
					config.Tenants[index].Queue.Bytes = 150
				}
			},
			first:  Admission{Tenant: 1, BodyBytes: 100, WorkUnits: 10, QueueTimeout: time.Minute},
			second: Admission{Tenant: 2, BodyBytes: 100, WorkUnits: 10, QueueTimeout: time.Minute},
			want:   Decision{Kind: DecisionGlobalRejected, Resource: ResourceBytes},
		},
		{
			name: "global work",
			mutate: func(config *Config) {
				config.GlobalQueue.Work = 150
				for index := range config.Tenants {
					config.Tenants[index].Queue.Work = 150
				}
			},
			first:  Admission{Tenant: 1, BodyBytes: 10, WorkUnits: 100, QueueTimeout: time.Minute},
			second: Admission{Tenant: 2, BodyBytes: 10, WorkUnits: 100, QueueTimeout: time.Minute},
			want:   Decision{Kind: DecisionGlobalRejected, Resource: ResourceWork},
		},
		{
			name: "tenant limit wins when both are full",
			mutate: func(config *Config) {
				config.GlobalQueue.Count = 1
				for index := range config.Tenants {
					config.Tenants[index].Queue.Count = 1
				}
			},
			first: defaultAdmission(1, 10), second: defaultAdmission(1, 10),
			want: Decision{Kind: DecisionTenantRejected, Resource: ResourceCount},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := baseConfig()
			test.mutate(&config)
			scheduler, _ := newTestScheduler(t, config)
			blocker := acquireNow(t, scheduler, defaultAdmission(2, 10))

			firstContext, cancelFirst := context.WithCancel(context.Background())
			firstResult := make(chan acquisition, 1)
			acquireAsync(scheduler, firstContext, "first", test.first, firstResult)
			waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
				return snapshot.Global.QueuedCount == 1 &&
					snapshot.Global.QueuedBytes == test.first.BodyBytes &&
					snapshot.Global.QueuedWork == test.first.WorkUnits
			})

			permit, decision := scheduler.Acquire(context.Background(), test.second)
			if permit != nil || decision != test.want {
				t.Fatalf("second Acquire() = (%v, %+v), want (nil, %+v)", permit, decision, test.want)
			}
			snapshot, err := scheduler.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if snapshot.Global.QueuedCount != 1 || snapshot.Global.QueuedBytes != test.first.BodyBytes || snapshot.Global.QueuedWork != test.first.WorkUnits {
				t.Fatalf("rejection partially mutated counters: %+v", snapshot.Global)
			}

			cancelFirst()
			if result := <-firstResult; result.decision.Kind != DecisionCanceledQueued || result.permit != nil {
				t.Fatalf("first cancellation = %+v", result)
			}
			blocker.Finish(ServingCompleted)
			closeScheduler(t, scheduler)
		})
	}
}

type drrOracle struct {
	order     []string
	queues    map[string][]uint64
	quantum   map[string]uint64
	deficit   map[string]uint64
	visitOpen map[string]bool
	cursor    int
}

func newDRROracle(base uint64, weights map[string]uint64, queues map[string][]uint64) *drrOracle {
	return &drrOracle{
		order:     []string{"A", "B"},
		queues:    map[string][]uint64{"A": append([]uint64(nil), queues["A"]...), "B": append([]uint64(nil), queues["B"]...)},
		quantum:   map[string]uint64{"A": base * weights["A"], "B": base * weights["B"]},
		deficit:   map[string]uint64{"A": 0, "B": 0},
		visitOpen: map[string]bool{"A": false, "B": false},
	}
}

func (o *drrOracle) next() (string, uint64) {
	for {
		label := o.order[o.cursor]
		if len(o.queues[label]) == 0 {
			o.deficit[label] = 0
			o.visitOpen[label] = false
			o.advance()
			continue
		}
		if !o.visitOpen[label] {
			o.deficit[label] += o.quantum[label]
			o.visitOpen[label] = true
		}
		work := o.queues[label][0]
		if work > o.deficit[label] {
			o.visitOpen[label] = false
			o.advance()
			continue
		}
		o.deficit[label] -= work
		o.queues[label] = o.queues[label][1:]
		if len(o.queues[label]) == 0 {
			o.deficit[label] = 0
			o.visitOpen[label] = false
			o.advance()
		}
		return label, work
	}
}

func (o *drrOracle) advance() {
	o.cursor = (o.cursor + 1) % len(o.order)
}

func TestQueueAndInflightCancellation(t *testing.T) {
	scheduler, _ := newTestScheduler(t, baseConfig())
	blocker := acquireNow(t, scheduler, defaultAdmission(1, 10))

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	queuedResult := make(chan acquisition, 1)
	acquireAsync(scheduler, queuedCtx, "queued", defaultAdmission(2, 10), queuedResult)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })
	cancelQueued()
	result := <-queuedResult
	if result.permit != nil || result.decision.Kind != DecisionCanceledQueued {
		t.Fatalf("queued cancellation = %+v", result)
	}
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 0 })
	blocker.Finish(ServingCompleted)

	inflight := acquireNow(t, scheduler, defaultAdmission(1, 10))
	if got := inflight.RequestCancel(); got != CancelApplied {
		t.Fatalf("RequestCancel() = %v, want applied", got)
	}
	select {
	case <-inflight.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("permit context was not canceled")
	}

	nextResult := make(chan acquisition, 1)
	acquireAsync(scheduler, context.Background(), "next", defaultAdmission(2, 10), nextResult)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
		return snapshot.Global.InflightCount == 1 && snapshot.Global.QueuedCount == 1
	})
	select {
	case unexpected := <-nextResult:
		t.Fatalf("capacity released before Finish(): %+v", unexpected)
	default:
	}
	terminal := inflight.Finish(ServingCompleted)
	if terminal.Outcome != TerminalCanceledInflight || !terminal.AccountingReleased {
		t.Fatalf("canceled Finish() = %+v", terminal)
	}
	next := <-nextResult
	if next.decision.Kind != DecisionDispatched || next.permit == nil {
		t.Fatalf("next Acquire() = %+v", next)
	}
	next.permit.Finish(ServingCompleted)
	closeScheduler(t, scheduler)
}

func TestQueueExpiresAtExactDeadline(t *testing.T) {
	scheduler, clock := newTestScheduler(t, baseConfig())
	blocker := acquireNow(t, scheduler, defaultAdmission(1, 10))
	resultChannel := make(chan acquisition, 1)
	request := defaultAdmission(2, 10)
	request.QueueTimeout = 10 * time.Second
	acquireAsync(scheduler, context.Background(), "expiring", request, resultChannel)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })

	clock.Advance(10*time.Second - 1)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })
	clock.Advance(1)
	result := <-resultChannel
	if result.permit != nil || result.decision.Kind != DecisionQueueExpired {
		t.Fatalf("expiry result = %+v", result)
	}
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 0 })
	blocker.Finish(ServingCompleted)
	closeScheduler(t, scheduler)
}

func TestConcurrentFinishReleasesExactlyOnce(t *testing.T) {
	scheduler, _ := newTestScheduler(t, baseConfig())
	permit := acquireNow(t, scheduler, defaultAdmission(1, 10))

	const callers = 64
	results := make(chan TerminalResult, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- permit.Finish(ServingCompleted)
		}()
	}
	group.Wait()
	close(results)
	for result := range results {
		if result != (TerminalResult{Outcome: TerminalCompleted, AccountingReleased: true}) {
			t.Fatalf("Finish() = %+v", result)
		}
	}
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global == (Counters{}) })
	if got := permit.RequestCancel(); got != CancelAlreadyTerminal {
		t.Fatalf("late RequestCancel() = %v, want terminal", got)
	}
	closeScheduler(t, scheduler)
}

func TestDrainCancelsQueuedAndSignalsInflight(t *testing.T) {
	scheduler, _ := newTestScheduler(t, baseConfig())
	inflight := acquireNow(t, scheduler, defaultAdmission(1, 10))
	queuedResults := make(chan acquisition, 2)
	acquireAsync(scheduler, context.Background(), "A", defaultAdmission(1, 10), queuedResults)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })
	acquireAsync(scheduler, context.Background(), "B", defaultAdmission(2, 10), queuedResults)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 2 })

	drain, err := scheduler.BeginDrain(context.Background())
	if err != nil {
		t.Fatalf("BeginDrain() error = %v", err)
	}
	if drain != (DrainResult{QueuedShutdownCanceled: 2, InFlightActiveAtStart: 1}) {
		t.Fatalf("BeginDrain() = %+v", drain)
	}
	for range 2 {
		result := <-queuedResults
		if result.permit != nil || result.decision.Kind != DecisionShutdown {
			t.Fatalf("queued drain result = %+v", result)
		}
	}
	select {
	case <-inflight.Context().Done():
		t.Fatal("graceful drain canceled in-flight context before grace expiry")
	default:
	}

	if permit, decision := scheduler.Acquire(context.Background(), defaultAdmission(1, 10)); permit != nil || decision.Kind != DecisionDraining {
		t.Fatalf("Acquire() during drain = (%v, %+v)", permit, decision)
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := scheduler.WaitDrained(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitDrained() before Finish = %v, want deadline", err)
	}
	cancel()
	forced, err := scheduler.ForceCancelInflight(context.Background())
	if err != nil || forced.NewlyShutdownSignaled != 1 {
		t.Fatalf("ForceCancelInflight() = (%+v, %v), want one signal", forced, err)
	}
	select {
	case <-inflight.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("forced drain did not cancel in-flight context")
	}

	terminal := inflight.Finish(ServingCompleted)
	if terminal.Outcome != TerminalShutdown || !terminal.AccountingReleased {
		t.Fatalf("drained Finish() = %+v", terminal)
	}
	if err := scheduler.WaitDrained(context.Background()); err != nil {
		t.Fatalf("WaitDrained() error = %v", err)
	}
	closeScheduler(t, scheduler)
}

func TestFundedHeadReservesFragmentedGlobalWork(t *testing.T) {
	config := baseConfig()
	config.GlobalInflight = InflightLimits{Count: 2, Work: 100}
	for index := range config.Tenants {
		config.Tenants[index].Inflight = InflightLimits{Count: 2, Work: 100}
	}
	scheduler, _ := newTestScheduler(t, config)

	blocker := acquireNow(t, scheduler, defaultAdmission(2, 1))
	results := make(chan acquisition, 2)
	acquireAsync(scheduler, context.Background(), "large", defaultAdmission(1, 100), results)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
		return snapshot.Global.QueuedCount == 1 && snapshot.Tenants[0].Deficit >= 100
	})
	acquireAsync(scheduler, context.Background(), "small", defaultAdmission(2, 1), results)
	waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 2 })

	select {
	case result := <-results:
		t.Fatalf("later request bypassed the funded head: %+v", result)
	default:
	}
	blocker.Finish(ServingCompleted)
	large := <-results
	if large.label != "large" || large.decision.Kind != DecisionDispatched || large.permit == nil {
		t.Fatalf("first dispatch after defragmentation = %+v, want large", large)
	}
	select {
	case result := <-results:
		t.Fatalf("small request dispatched while large held all work: %+v", result)
	default:
	}
	large.permit.Finish(ServingCompleted)
	small := <-results
	if small.label != "small" || small.decision.Kind != DecisionDispatched || small.permit == nil {
		t.Fatalf("second dispatch = %+v, want small", small)
	}
	small.permit.Finish(ServingCompleted)
	closeScheduler(t, scheduler)
}

func TestEmptyQueueResetsFundedCredit(t *testing.T) {
	tests := []struct {
		name string
		end  func(*fakeClock, context.CancelFunc)
		want DecisionKind
	}{
		{
			name: "client cancellation",
			end:  func(_ *fakeClock, cancel context.CancelFunc) { cancel() },
			want: DecisionCanceledQueued,
		},
		{
			name: "queue expiry",
			end:  func(clock *fakeClock, _ context.CancelFunc) { clock.Advance(10 * time.Second) },
			want: DecisionQueueExpired,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := baseConfig()
			config.GlobalInflight = InflightLimits{Count: 2, Work: 100}
			for index := range config.Tenants {
				config.Tenants[index].Inflight = InflightLimits{Count: 2, Work: 100}
			}
			scheduler, clock := newTestScheduler(t, config)
			blocker := acquireNow(t, scheduler, defaultAdmission(2, 90))

			ctx, cancel := context.WithCancel(context.Background())
			request := defaultAdmission(1, 50)
			request.QueueTimeout = 10 * time.Second
			results := make(chan acquisition, 1)
			acquireAsync(scheduler, ctx, "head", request, results)
			waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
				return snapshot.Global.QueuedCount == 1 && snapshot.Tenants[0].Deficit >= 50
			})

			test.end(clock, cancel)
			result := <-results
			if result.permit != nil || result.decision.Kind != test.want {
				t.Fatalf("terminal queue result = %+v, want %v", result, test.want)
			}
			snapshot := waitSnapshot(t, scheduler, func(snapshot Snapshot) bool {
				return snapshot.Global.QueuedCount == 0
			})
			if snapshot.Tenants[0].Deficit != 0 {
				t.Fatalf("empty tenant retained deficit %d", snapshot.Tenants[0].Deficit)
			}
			cancel()
			blocker.Finish(ServingCompleted)
			closeScheduler(t, scheduler)
		})
	}
}

func TestQueueTimeoutIncludesMailboxDelay(t *testing.T) {
	config, err := validateConfig(baseConfig())
	if err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	scheduler := &Scheduler{
		config:   config,
		clock:    clock,
		commands: make(chan any, 1),
		drained:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	scheduler.commands <- snapshotCommand{response: make(chan Snapshot, 1)}

	results := make(chan acquisition, 1)
	request := defaultAdmission(1, 10)
	request.QueueTimeout = 10 * time.Second
	acquireAsync(scheduler, context.Background(), "delayed", request, results)
	deadline := time.Now().Add(time.Second)
	for scheduler.nextID.Load() != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if scheduler.nextID.Load() != 1 {
		t.Fatal("Acquire did not reach the full mailbox")
	}
	clock.Advance(10 * time.Second)
	<-scheduler.commands
	rawCommand := <-scheduler.commands
	state := newOwnerState(scheduler)
	state.handle(rawCommand, clock.Now())

	result := <-results
	if result.permit != nil || result.decision.Kind != DecisionQueueExpired {
		t.Fatalf("mailbox-delayed Acquire() = %+v", result)
	}
	if state.global != (Counters{}) || len(state.entries) != 0 {
		t.Fatalf("expired mailbox command mutated accounting: %+v", state.global)
	}
}

func TestWaitDrainedRequiresDrainToStart(t *testing.T) {
	scheduler, _ := newTestScheduler(t, baseConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := scheduler.WaitDrained(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitDrained() before BeginDrain = %v, want deadline", err)
	}
	cancel()
	if _, err := scheduler.BeginDrain(context.Background()); err != nil {
		t.Fatalf("BeginDrain() error = %v", err)
	}
	if err := scheduler.WaitDrained(context.Background()); err != nil {
		t.Fatalf("WaitDrained() after empty drain = %v", err)
	}
	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	for range 100 {
		if err := scheduler.WaitDrained(canceled); err != nil {
			t.Fatalf("completed drain lost race to canceled context: %v", err)
		}
	}
	closeScheduler(t, scheduler)
}

func TestDeadlineSourceHasStablePrecedence(t *testing.T) {
	tests := []struct {
		name          string
		clientTimeout time.Duration
		want          DecisionKind
	}{
		{name: "client wins equal deadline", clientTimeout: 10 * time.Second, want: DecisionCanceledQueued},
		{name: "queue expires first", clientTimeout: 20 * time.Second, want: DecisionQueueExpired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock(time.Now())
			scheduler, err := newScheduler(baseConfig(), clock)
			if err != nil {
				t.Fatalf("newScheduler() error = %v", err)
			}
			blocker := acquireNow(t, scheduler, defaultAdmission(1, 10))
			ctx, cancel := context.WithDeadline(context.Background(), clock.Now().Add(test.clientTimeout))
			results := make(chan acquisition, 1)
			request := defaultAdmission(2, 10)
			request.QueueTimeout = 10 * time.Second
			acquireAsync(scheduler, ctx, "queued", request, results)
			waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })

			clock.Advance(10 * time.Second)
			result := <-results
			if result.permit != nil || result.decision.Kind != test.want {
				t.Fatalf("deadline result = %+v, want %v", result, test.want)
			}
			cancel()
			blocker.Finish(ServingCompleted)
			closeScheduler(t, scheduler)
		})
	}
}

func TestMutatingCommandsCommitAfterMailboxAcceptance(t *testing.T) {
	t.Run("begin drain", func(t *testing.T) {
		scheduler := bareScheduler()
		ctx, cancel := context.WithCancel(context.Background())
		type callResult struct {
			result DrainResult
			err    error
		}
		returned := make(chan callResult, 1)
		go func() {
			result, err := scheduler.BeginDrain(ctx)
			returned <- callResult{result: result, err: err}
		}()
		command := (<-scheduler.commands).(drainCommand)
		cancel()
		assertStillCommitted(t, returned)
		want := DrainResult{QueuedShutdownCanceled: 7, InFlightActiveAtStart: 3}
		command.response <- want
		got := <-returned
		if got.result != want || got.err != nil {
			t.Fatalf("BeginDrain() = (%+v, %v)", got.result, got.err)
		}
	})

	t.Run("force cancel", func(t *testing.T) {
		scheduler := bareScheduler()
		ctx, cancel := context.WithCancel(context.Background())
		type callResult struct {
			result ForceCancelResult
			err    error
		}
		returned := make(chan callResult, 1)
		go func() {
			result, err := scheduler.ForceCancelInflight(ctx)
			returned <- callResult{result: result, err: err}
		}()
		command := (<-scheduler.commands).(forceCancelCommand)
		cancel()
		assertStillCommitted(t, returned)
		command.response <- ForceCancelResult{NewlyShutdownSignaled: 9}
		got := <-returned
		if got.result.NewlyShutdownSignaled != 9 || got.err != nil {
			t.Fatalf("ForceCancelInflight() = (%+v, %v)", got.result, got.err)
		}
	})

	t.Run("close stop", func(t *testing.T) {
		scheduler := bareScheduler()
		close(scheduler.drained)
		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)
		go func() { returned <- scheduler.Close(ctx) }()
		drain := (<-scheduler.commands).(drainCommand)
		drain.response <- DrainResult{}
		stop := (<-scheduler.commands).(stopCommand)
		cancel()
		assertStillCommitted(t, returned)
		close(stop.acknowledged)
		if err := <-returned; err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	t.Run("close finalizes after drained context race", func(t *testing.T) {
		scheduler := bareScheduler()
		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)
		go func() { returned <- scheduler.Close(ctx) }()
		drain := (<-scheduler.commands).(drainCommand)
		drain.response <- DrainResult{}
		close(scheduler.drained)
		cancel()

		var stop stopCommand
		select {
		case rawCommand := <-scheduler.commands:
			stop = rawCommand.(stopCommand)
		case err := <-returned:
			t.Fatalf("Close returned before owner finalization: %v", err)
		case <-time.After(time.Second):
			t.Fatal("Close did not send the terminal owner command")
		}
		close(stop.acknowledged)
		if err := <-returned; err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	t.Run("pre-canceled context", func(t *testing.T) {
		scheduler := bareScheduler()
		scheduler.commands = make(chan any, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := scheduler.BeginDrain(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("BeginDrain() error = %v, want canceled", err)
		}
		if len(scheduler.commands) != 0 {
			t.Fatal("pre-canceled mutation entered the mailbox")
		}
	})
}

func TestClientCancellationWinsLaterShutdown(t *testing.T) {
	t.Run("finish", func(t *testing.T) {
		scheduler, _ := newTestScheduler(t, baseConfig())
		ctx, cancel := context.WithCancel(context.Background())
		permit, decision := scheduler.Acquire(ctx, defaultAdmission(1, 10))
		if permit == nil || decision.Kind != DecisionDispatched {
			t.Fatalf("Acquire() = (%v, %+v)", permit, decision)
		}
		cancel()
		<-permit.Context().Done()
		terminal := permit.Finish(ServingCompleted)
		if terminal.Outcome != TerminalCanceledInflight || !terminal.AccountingReleased {
			t.Fatalf("Finish() = %+v, want client cancellation", terminal)
		}
		closeScheduler(t, scheduler)
	})

	t.Run("parent context then force", func(t *testing.T) {
		scheduler, _ := newTestScheduler(t, baseConfig())
		ctx, cancel := context.WithCancel(context.Background())
		permit, decision := scheduler.Acquire(ctx, defaultAdmission(1, 10))
		if permit == nil || decision.Kind != DecisionDispatched {
			t.Fatalf("Acquire() = (%v, %+v)", permit, decision)
		}
		cancel()
		<-permit.Context().Done()
		forced, err := scheduler.ForceCancelInflight(context.Background())
		if err != nil || forced.NewlyShutdownSignaled != 0 {
			t.Fatalf("ForceCancelInflight() = (%+v, %v), want client-owned cancellation", forced, err)
		}
		terminal := permit.Finish(ServingCompleted)
		if terminal.Outcome != TerminalCanceledInflight || !terminal.AccountingReleased {
			t.Fatalf("Finish() = %+v, want client cancellation", terminal)
		}
		closeScheduler(t, scheduler)
	})

	t.Run("queued client before drain", func(t *testing.T) {
		scheduler, _ := newTestScheduler(t, baseConfig())
		blocker := acquireNow(t, scheduler, defaultAdmission(1, 10))
		ctx, cancel := context.WithCancel(context.Background())
		results := make(chan acquisition, 1)
		acquireAsync(scheduler, ctx, "queued", defaultAdmission(2, 10), results)
		waitSnapshot(t, scheduler, func(snapshot Snapshot) bool { return snapshot.Global.QueuedCount == 1 })
		cancel()
		drain, err := scheduler.BeginDrain(context.Background())
		if err != nil || drain.QueuedShutdownCanceled != 0 {
			t.Fatalf("BeginDrain() = (%+v, %v), want no shutdown-owned queued cancellation", drain, err)
		}
		result := <-results
		if result.permit != nil || result.decision.Kind != DecisionCanceledQueued {
			t.Fatalf("queued result = %+v", result)
		}
		blocker.Finish(ServingCompleted)
		closeScheduler(t, scheduler)
	})
}

func bareScheduler() *Scheduler {
	return &Scheduler{
		commands: make(chan any),
		drained:  make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func assertStillCommitted[T any](t *testing.T, returned <-chan T) {
	t.Helper()
	for range 100 {
		runtime.Gosched()
		select {
		case <-returned:
			t.Fatal("accepted mutation returned before its owner response")
		default:
		}
	}
}
