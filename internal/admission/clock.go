package admission

import "time"

type clock interface {
	Now() time.Time
	NewTimer(time.Duration) timer
}

type timer interface {
	C() <-chan time.Time
	Reset(time.Duration) bool
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(duration time.Duration) timer {
	return systemTimer{timer: time.NewTimer(duration)}
}

type systemTimer struct {
	timer *time.Timer
}

func (t systemTimer) C() <-chan time.Time { return t.timer.C }

func (t systemTimer) Reset(duration time.Duration) bool { return t.timer.Reset(duration) }

func (t systemTimer) Stop() bool { return t.timer.Stop() }
