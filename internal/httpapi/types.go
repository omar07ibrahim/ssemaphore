// Package httpapi connects the bounded request contract and admission
// scheduler to a deliberately small non-streaming HTTP surface.
package httpapi

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	chatCompletionsPath = "/v1/chat/completions"
	queueTimeoutHeader  = "X-Ssemaphore-Queue-Timeout-Ms"
	requestIDHeader     = "X-Request-Id"
)

// Credential binds one opaque bearer token to one scheduler tenant. Raw
// tokens are accepted only during construction and are not retained by the
// resulting Handler.
type Credential struct {
	Tenant admission.TenantID
	Token  string
}

// TenantPreDispatchLimit bounds authenticated requests before and during body
// parsing and scheduler acquisition. A request holds its slot until Acquire
// returns, so queued request bodies cannot escape this bound.
type TenantPreDispatchLimit struct {
	Tenant admission.TenantID
	Count  uint64
}

// Config defines finite policy deadlines, response buffering, credentials,
// and the request slots that protect work before scheduler admission.
type Config struct {
	DefaultQueueTimeout  time.Duration
	BodyReadTimeout      time.Duration
	UpstreamTimeout      time.Duration
	MaxResponseBodyBytes uint64

	GlobalPreDispatchLimit uint64
	TenantPreDispatch      []TenantPreDispatchLimit
	Credentials            []Credential
}

// NonStreamingUpstream receives only a validated request and the permit-owned
// context. It cannot observe inbound credentials, headers, URLs, or the
// downstream ResponseWriter. Complete may be called concurrently and must
// return when ctx is canceled.
type NonStreamingUpstream interface {
	Complete(context.Context, contract.Request) (UpstreamResponse, error)
}

// UpstreamResponse is the bounded response candidate returned by an injected
// upstream implementation. Body ownership transfers to the Handler. After
// return, the implementation must not mutate Header or replace Body. Body must
// permit Close concurrently with Read, and Close must unblock a pending Read.
type UpstreamResponse struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

type workPermit interface {
	Context() context.Context
	Finish(admission.ServingOutcome) admission.TerminalResult
}

type admissionGate interface {
	Acquire(context.Context, admission.Admission) (workPermit, admission.Decision)
}

type schedulerGate struct {
	scheduler *admission.Scheduler
}

func (g schedulerGate) Acquire(ctx context.Context, request admission.Admission) (workPermit, admission.Decision) {
	return g.scheduler.Acquire(ctx, request)
}
