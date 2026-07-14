package admission

import (
	"sync"
	"time"
)

type fakeClock struct {
	mutex  sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, timers: make(map[*fakeTimer]struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(duration time.Duration) timer {
	t := &fakeTimer{clock: c, channel: make(chan time.Time, 1)}
	c.mutex.Lock()
	c.timers[t] = struct{}{}
	t.resetLocked(duration)
	c.mutex.Unlock()
	return t
}

func (c *fakeClock) Advance(duration time.Duration) {
	if duration < 0 {
		panic("fake clock cannot move backwards")
	}
	c.mutex.Lock()
	c.now = c.now.Add(duration)
	for current := range c.timers {
		current.fireIfReadyLocked()
	}
	c.mutex.Unlock()
}

type fakeTimer struct {
	clock    *fakeClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.channel }

func (t *fakeTimer) Reset(duration time.Duration) bool {
	t.clock.mutex.Lock()
	defer t.clock.mutex.Unlock()
	wasActive := t.active
	t.resetLocked(duration)
	return wasActive
}

func (t *fakeTimer) Stop() bool {
	t.clock.mutex.Lock()
	defer t.clock.mutex.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *fakeTimer) resetLocked(duration time.Duration) {
	t.deadline = t.clock.now.Add(duration)
	t.active = true
	t.fireIfReadyLocked()
}

func (t *fakeTimer) fireIfReadyLocked() {
	if !t.active || t.deadline.After(t.clock.now) {
		return
	}
	t.active = false
	select {
	case t.channel <- t.clock.now:
	default:
	}
}
