package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

var errSSEReadTimeout = errors.New("upstream SSE read timed out")

// relaySSE validates and flushes one event at a time. It deliberately retains
// the terminal marker until clean EOF and a successful body close prove that
// the upstream did not append data after [DONE].
func (h *Handler) relaySSE(
	sink *responseSink,
	request *http.Request,
	permit workPermit,
	upstreamContext context.Context,
	body io.ReadCloser,
) admission.ServingOutcome {
	idleReader := &sseIdleReader{
		reader:  body,
		closer:  body,
		timeout: h.streamReadTimeout,
	}
	decoder, err := contract.NewSSEDecoder(idleReader, h.sseLimits)
	if err != nil {
		if sink.writeError(errInternal) != nil {
			return admission.ServingDownstreamFailed
		}
		return admission.ServingInternalFailure
	}

	for {
		event, eventTimedOut, readErr := h.nextSSEEvent(upstreamContext, body, decoder)
		if readErr != nil {
			return h.finishSSEFailure(
				sink,
				request,
				permit,
				upstreamContext,
				eventTimedOut || idleReader.TimedOut(),
			)
		}
		if contextOutcome, handled := h.handleUpstreamContext(
			sink,
			request,
			permit,
			upstreamContext,
		); handled {
			return contextOutcome
		}

		if event.Kind() == contract.SSEEventKindDone {
			verifyTimedOut, verifyErr := h.verifySSEEOF(upstreamContext, body, decoder)
			if verifyErr != nil {
				return h.finishSSEFailure(
					sink,
					request,
					permit,
					upstreamContext,
					verifyTimedOut || idleReader.TimedOut(),
				)
			}
			if closeErr := body.Close(); closeErr != nil {
				return h.finishSSEFailure(sink, request, permit, upstreamContext, false)
			}
			if contextOutcome, handled := h.handleUpstreamContext(
				sink,
				request,
				permit,
				upstreamContext,
			); handled {
				return contextOutcome
			}
			if writeErr := sink.writeSSEEvent(int64(event.BodyBytes()), event.BodyReader()); writeErr != nil {
				if contextOutcome, handled := h.handleUpstreamContext(
					sink,
					request,
					permit,
					upstreamContext,
				); handled {
					return contextOutcome
				}
				return admission.ServingDownstreamFailed
			}
			return admission.ServingCompleted
		}

		if writeErr := sink.writeSSEEvent(int64(event.BodyBytes()), event.BodyReader()); writeErr != nil {
			if contextOutcome, handled := h.handleUpstreamContext(
				sink,
				request,
				permit,
				upstreamContext,
			); handled {
				return contextOutcome
			}
			return admission.ServingDownstreamFailed
		}
	}
}

func (h *Handler) nextSSEEvent(
	parent context.Context,
	body io.Closer,
	decoder *contract.SSEDecoder,
) (contract.ValidatedSSEEvent, bool, error) {
	eventContext, cancelEvent := context.WithTimeout(parent, h.streamEventTimeout)
	callbackDone := make(chan struct{})
	stopClose := context.AfterFunc(eventContext, func() {
		closeIgnoringPanic(body)
		close(callbackDone)
	})
	event, err := decoder.Next(eventContext)
	if !stopClose() {
		<-callbackDone
		if err == nil {
			err = eventContext.Err()
			if err == nil {
				err = context.Canceled
			}
		}
	}
	timedOut := errors.Is(eventContext.Err(), context.DeadlineExceeded) && parent.Err() == nil
	cancelEvent()
	return event, timedOut, err
}

func (h *Handler) verifySSEEOF(
	parent context.Context,
	body io.Closer,
	decoder *contract.SSEDecoder,
) (bool, error) {
	eventContext, cancelEvent := context.WithTimeout(parent, h.streamEventTimeout)
	callbackDone := make(chan struct{})
	stopClose := context.AfterFunc(eventContext, func() {
		closeIgnoringPanic(body)
		close(callbackDone)
	})
	err := decoder.VerifyEOF(eventContext)
	if !stopClose() {
		<-callbackDone
		if err == nil {
			err = eventContext.Err()
			if err == nil {
				err = context.Canceled
			}
		}
	}
	timedOut := errors.Is(eventContext.Err(), context.DeadlineExceeded) && parent.Err() == nil
	cancelEvent()
	return timedOut, err
}

func (h *Handler) finishSSEFailure(
	sink *responseSink,
	request *http.Request,
	permit workPermit,
	upstreamContext context.Context,
	timedOut bool,
) admission.ServingOutcome {
	if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, upstreamContext); handled {
		return contextOutcome
	}
	if sink.committed {
		return admission.ServingUpstreamFailed
	}
	failure := errBadUpstream
	if timedOut {
		failure = errUpstreamTimeout
	}
	if sink.writeError(failure) != nil {
		return admission.ServingDownstreamFailed
	}
	return admission.ServingUpstreamFailed
}

// sseIdleReader bounds each actual upstream Read. No timer runs while the
// handler is blocked on a downstream write, so backpressure cannot be mistaken
// for an idle upstream. Once the timer fires the stream is permanently failed.
type sseIdleReader struct {
	reader  io.Reader
	closer  io.Closer
	timeout time.Duration

	timedOut atomic.Bool
}

func (r *sseIdleReader) Read(destination []byte) (int, error) {
	if r.timedOut.Load() {
		return 0, errSSEReadTimeout
	}
	callbackDone := make(chan struct{})
	timer := time.AfterFunc(r.timeout, func() {
		r.timedOut.Store(true)
		closeIgnoringPanic(r.closer)
		close(callbackDone)
	})
	n, err := r.reader.Read(destination)
	if !timer.Stop() {
		<-callbackDone
	}
	if r.timedOut.Load() {
		return n, errSSEReadTimeout
	}
	return n, err
}

func (r *sseIdleReader) TimedOut() bool { return r.timedOut.Load() }

func closeIgnoringPanic(closer io.Closer) {
	defer func() {
		_ = recover()
	}()
	_ = closer.Close()
}
