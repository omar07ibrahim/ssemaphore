package app

import (
	"context"
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/server"
)

const maximumShutdownWait = 3 * time.Hour

type outcome uint8

const (
	outcomeFailure outcome = iota
	outcomeCleanShutdown
)

type runtimeServer interface {
	Serve() error
	Shutdown(context.Context) (server.ShutdownResult, error)
}

// supervise owns the one-way transition from serving to terminal shutdown.
// It deliberately reduces every dependency error to a static outcome so
// runtime errors cannot disclose transport or secret-bearing details.
func supervise(runtime runtimeServer, events <-chan os.Signal, stop func(), shutdownWait time.Duration) outcome {
	if nilRuntimeServer(runtime) || events == nil || stop == nil || shutdownWait <= 0 || shutdownWait > maximumShutdownWait {
		return outcomeFailure
	}

	serveResults := make(chan error, 1)
	go func() {
		serveResults <- runtime.Serve()
	}()

	cleanTrigger := false
	serveReturned := false
	var serveErr error
	select {
	case serveErr = <-serveResults:
		serveReturned = true
	case event, open := <-events:
		cleanTrigger = open && handledSignal(event)
	}

	// Restore the default behavior before cleanup starts. A second termination
	// signal can therefore terminate a process whose graceful shutdown stalls.
	stop()

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), shutdownWait)
	defer cancelShutdown()
	_, shutdownErr := runtime.Shutdown(shutdownContext)
	for !serveReturned {
		select {
		case serveErr = <-serveResults:
			serveReturned = true
		case <-shutdownContext.Done():
			// Prefer results already committed at the deadline over a random
			// select choice between ready result and context channels.
			select {
			case serveErr = <-serveResults:
				serveReturned = true
			default:
			}
			if !serveReturned {
				return outcomeFailure
			}
		}
	}

	if cleanTrigger && serveErr == nil && shutdownErr == nil {
		return outcomeCleanShutdown
	}
	return outcomeFailure
}

func handledSignal(event os.Signal) bool {
	signal, ok := event.(syscall.Signal)
	return ok && (signal == syscall.SIGINT || signal == syscall.SIGTERM)
}

func nilRuntimeServer(runtime runtimeServer) bool {
	if runtime == nil {
		return true
	}
	value := reflect.ValueOf(runtime)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
