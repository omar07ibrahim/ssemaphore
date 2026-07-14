package app

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type signalAPI struct {
	notify func(chan<- os.Signal, ...os.Signal)
	stop   func(chan<- os.Signal)
}

func subscribeTerminationSignals() (<-chan os.Signal, func(), error) {
	return subscribeSignals(signalAPI{notify: signal.Notify, stop: signal.Stop})
}

func subscribeSignals(api signalAPI) (<-chan os.Signal, func(), error) {
	if api.notify == nil || api.stop == nil {
		return nil, nil, errGatewayStartFailed
	}
	events := make(chan os.Signal, 1)
	api.notify(events, os.Interrupt, syscall.SIGTERM)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			api.stop(events)
		})
	}
	return events, stop, nil
}
