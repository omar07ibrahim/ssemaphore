package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

type requestIDSource func() (string, error)

// Handler implements the authenticated, bounded non-streaming HTTP lifecycle.
// It is immutable after construction and safe for concurrent use.
type Handler struct {
	parser              *contract.Parser
	gate                admissionGate
	upstream            NonStreamingUpstream
	responseValidator   *contract.ResponseValidator
	defaultQueueTimeout time.Duration
	bodyReadTimeout     time.Duration
	upstreamTimeout     time.Duration
	globalSlots         chan struct{}
	tenantSlots         map[admission.TenantID]chan struct{}
	credentials         []storedCredential
	requestIDs          requestIDSource
}

// NewHandler validates every integration bound before accepting HTTP work.
func NewHandler(
	config Config,
	parser *contract.Parser,
	scheduler *admission.Scheduler,
	upstream NonStreamingUpstream,
) (*Handler, error) {
	return newHandler(config, parser, scheduler, schedulerGate{scheduler: scheduler}, upstream, secureRequestID)
}

func newHandler(
	config Config,
	parser *contract.Parser,
	scheduler *admission.Scheduler,
	gate admissionGate,
	upstream NonStreamingUpstream,
	requestIDs requestIDSource,
) (*Handler, error) {
	validated, err := validateHandlerConfig(config, parser, scheduler)
	if err != nil {
		return nil, err
	}
	if gate == nil {
		return nil, errors.New("admission gate must not be nil")
	}
	if upstream == nil {
		return nil, errors.New("non-streaming upstream must not be nil")
	}
	if requestIDs == nil {
		return nil, errors.New("request ID source must not be nil")
	}

	return &Handler{
		parser:              parser,
		gate:                gate,
		upstream:            upstream,
		responseValidator:   validated.responseValidator,
		defaultQueueTimeout: validated.defaultQueueTimeout,
		bodyReadTimeout:     validated.bodyReadTimeout,
		upstreamTimeout:     validated.upstreamTimeout,
		globalSlots:         validated.globalSlots,
		tenantSlots:         validated.tenantSlots,
		credentials:         validated.credentials,
		requestIDs:          requestIDs,
	}, nil
}

// ServeHTTP owns all downstream writes. Panics before a response commit are
// converted to a static error while permit cleanup still runs through defer.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if writer == nil || request == nil {
		return
	}
	sink := &responseSink{writer: writer}
	defer func() {
		if recover() != nil && !sink.committed && request.Context().Err() == nil {
			_ = sink.writeError(errInternal)
		}
	}()
	if request.Body != nil {
		body := newOnceReadCloser(request.Body)
		request.Body = body
		defer body.Close()
	}
	h.serve(sink, request)
}

func (h *Handler) serve(sink *responseSink, request *http.Request) {
	if request.Context().Err() != nil {
		return
	}
	requestID, err := h.requestIDs()
	if err != nil || !validRequestID(requestID) {
		_ = sink.writeError(errInternal)
		return
	}
	sink.writer.Header().Set(requestIDHeader, requestID)

	if !exactChatCompletionsPath(request) {
		_ = sink.writeError(errUnsupportedPath)
		return
	}
	if request.Method != http.MethodPost {
		sink.writer.Header().Set("Allow", http.MethodPost)
		_ = sink.writeError(errUnsupportedMethod)
		return
	}

	tenant, authenticated := h.authenticate(request.Header.Values("Authorization"))
	if !authenticated {
		sink.writer.Header().Set("WWW-Authenticate", "Bearer")
		_ = sink.writeError(errInvalidCredential)
		return
	}
	if !validJSONMediaType(request.Header) {
		_ = sink.writeError(errUnsupportedMedia)
		return
	}
	queueTimeout, valid := h.queueTimeout(request.Header.Values(queueTimeoutHeader))
	if !valid {
		_ = sink.writeError(errInvalidRequest)
		return
	}
	if request.ContentLength < -1 {
		_ = sink.writeError(errInvalidRequest)
		return
	}
	if request.ContentLength > int64(h.parser.MaxBodyBytes()) {
		_ = sink.writeError(errRequestTooLarge)
		return
	}

	slots, acquired := h.acquirePreDispatch(tenant)
	if !acquired {
		if slots.tenantFull {
			_ = sink.writeError(errTenantCapacity)
		} else {
			_ = sink.writeError(errOverloaded)
		}
		return
	}
	defer slots.release()

	parsed, parseErr := h.parseRequest(sink.writer, request)
	if parseErr != nil {
		if request.Context().Err() != nil {
			return
		}
		var contractErr *contract.ParseError
		if errors.As(parseErr, &contractErr) && contractErr.Class() == contract.ErrorClassRequestTooLarge {
			_ = sink.writeError(errRequestTooLarge)
			return
		}
		_ = sink.writeError(errInvalidRequest)
		return
	}

	permit, decision := h.gate.Acquire(request.Context(), admission.Admission{
		Tenant:       tenant,
		BodyBytes:    parsed.BodyBytes(),
		WorkUnits:    parsed.ReservationUnits(),
		QueueTimeout: queueTimeout,
	})
	slots.release()
	if decision.Kind != admission.DecisionDispatched {
		if permit != nil {
			permit.Finish(admission.ServingInternalFailure)
			decision = admission.Decision{Kind: admission.DecisionInvalid}
		}
		h.writeAdmissionDecision(sink, request, decision)
		return
	}
	if permit == nil {
		h.writeAdmissionDecision(sink, request, admission.Decision{Kind: admission.DecisionInvalid})
		return
	}

	outcome := admission.ServingInternalFailure
	defer func() {
		permit.Finish(outcome)
	}()

	if request.Context().Err() != nil {
		outcome = admission.ServingCanceled
		return
	}
	if permit.Context() == nil {
		_ = sink.writeError(errInternal)
		return
	}
	if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, permit.Context()); handled {
		outcome = contextOutcome
		return
	}

	upstreamContext, cancelUpstream := context.WithTimeout(permit.Context(), h.upstreamTimeout)
	defer cancelUpstream()
	response, upstreamErr := h.upstream.Complete(upstreamContext, parsed)
	if response.Body != nil {
		response.Body = newOnceReadCloser(response.Body)
		defer response.Body.Close()
		stopClose := context.AfterFunc(upstreamContext, func() {
			_ = response.Body.Close()
		})
		defer stopClose()
	}

	if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, upstreamContext); handled {
		outcome = contextOutcome
		return
	}
	if upstreamErr != nil || !validUpstreamMetadata(response) {
		outcome = admission.ServingUpstreamFailed
		if sink.writeError(errBadUpstream) != nil {
			outcome = admission.ServingDownstreamFailed
		}
		return
	}

	validatedResponse, validationErr := h.responseValidator.Parse(upstreamContext, response.Body)
	if validationErr != nil {
		if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, upstreamContext); handled {
			outcome = contextOutcome
			return
		}
		outcome = admission.ServingUpstreamFailed
		if sink.writeError(errBadUpstream) != nil {
			outcome = admission.ServingDownstreamFailed
		}
		return
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, upstreamContext); handled {
			outcome = contextOutcome
			return
		}
		outcome = admission.ServingUpstreamFailed
		if sink.writeError(errBadUpstream) != nil {
			outcome = admission.ServingDownstreamFailed
		}
		return
	}
	if contextOutcome, handled := h.handleUpstreamContext(sink, request, permit, upstreamContext); handled {
		outcome = contextOutcome
		return
	}

	outcome = admission.ServingDownstreamFailed
	if sink.writeJSONReader(
		http.StatusOK,
		int64(validatedResponse.BodyBytes()),
		validatedResponse.BodyReader(),
	) != nil {
		return
	}
	outcome = admission.ServingCompleted
}

func (h *Handler) parseRequest(writer http.ResponseWriter, request *http.Request) (contract.Request, error) {
	if request.Body == nil {
		return contract.Request{}, errors.New("request body must not be nil")
	}
	body := newOnceReadCloser(request.Body)
	defer body.Close()
	readContext, cancelRead := context.WithTimeout(request.Context(), h.bodyReadTimeout)
	defer cancelRead()
	stopClose := context.AfterFunc(readContext, func() {
		_ = body.Close()
	})
	defer stopClose()

	boundedBody := http.MaxBytesReader(writer, body, int64(h.parser.MaxBodyBytes()+1))
	defer boundedBody.Close()
	return h.parser.Parse(readContext, boundedBody)
}

func (h *Handler) authenticate(values []string) (admission.TenantID, bool) {
	if len(values) != 1 {
		return 0, false
	}
	value := values[0]
	if len(value) < len("Bearer ")+1 || !strings.EqualFold(value[:len("Bearer")], "Bearer") || value[len("Bearer")] != ' ' {
		return 0, false
	}
	token := value[len("Bearer "):]
	if !validBearerToken(token) {
		return 0, false
	}
	digest := sha256.Sum256([]byte(token))
	var matched uint32
	var tenant uint32
	for _, credential := range h.credentials {
		equal := uint32(subtle.ConstantTimeCompare(digest[:], credential.digest[:]))
		mask := uint32(0) - equal
		tenant = tenant&^mask | uint32(credential.tenant)&mask
		matched |= equal
	}
	return admission.TenantID(tenant), matched == 1
}

func (h *Handler) queueTimeout(values []string) (time.Duration, bool) {
	if len(values) == 0 {
		return h.defaultQueueTimeout, true
	}
	if len(values) != 1 || values[0] == "" || len(values[0]) > 10 {
		return 0, false
	}
	for index := range len(values[0]) {
		if values[0][index] < '0' || values[0][index] > '9' {
			return 0, false
		}
	}
	milliseconds, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil || milliseconds == 0 {
		return 0, false
	}
	timeout := time.Duration(milliseconds) * time.Millisecond
	if timeout <= 0 || timeout > h.defaultQueueTimeout {
		return 0, false
	}
	return timeout, true
}

type preDispatchLease struct {
	tenant     chan struct{}
	global     chan struct{}
	tenantFull bool
	once       sync.Once
}

func (h *Handler) acquirePreDispatch(tenant admission.TenantID) (*preDispatchLease, bool) {
	lease := &preDispatchLease{tenant: h.tenantSlots[tenant], global: h.globalSlots}
	select {
	case lease.tenant <- struct{}{}:
	default:
		lease.tenantFull = true
		return lease, false
	}
	select {
	case lease.global <- struct{}{}:
		return lease, true
	default:
		<-lease.tenant
		lease.tenant = nil
		return lease, false
	}
}

func (l *preDispatchLease) release() {
	l.once.Do(func() {
		if l.global != nil {
			<-l.global
		}
		if l.tenant != nil {
			<-l.tenant
		}
	})
}

func (h *Handler) writeAdmissionDecision(sink *responseSink, request *http.Request, decision admission.Decision) {
	if request.Context().Err() != nil {
		return
	}
	switch decision.Kind {
	case admission.DecisionTenantRejected:
		_ = sink.writeError(errTenantCapacity)
	case admission.DecisionGlobalRejected:
		_ = sink.writeError(errOverloaded)
	case admission.DecisionQueueExpired:
		_ = sink.writeError(errQueueDeadline)
	case admission.DecisionShutdown, admission.DecisionDraining:
		_ = sink.writeError(errDraining)
	case admission.DecisionCanceledQueued, admission.DecisionCanceledBeforeStart:
		return
	default:
		_ = sink.writeError(errInternal)
	}
}

func (h *Handler) handleUpstreamContext(
	sink *responseSink,
	request *http.Request,
	permit workPermit,
	upstreamContext context.Context,
) (admission.ServingOutcome, bool) {
	if request.Context().Err() != nil {
		return admission.ServingCanceled, true
	}
	if permit.Context().Err() != nil {
		if request.Context().Err() != nil {
			return admission.ServingCanceled, true
		}
		_ = sink.writeError(errDraining)
		return admission.ServingCanceled, true
	}
	if errors.Is(upstreamContext.Err(), context.DeadlineExceeded) {
		if request.Context().Err() != nil {
			return admission.ServingCanceled, true
		}
		if permit.Context().Err() != nil {
			if request.Context().Err() != nil {
				return admission.ServingCanceled, true
			}
			_ = sink.writeError(errDraining)
			return admission.ServingCanceled, true
		}
		if sink.writeError(errUpstreamTimeout) != nil {
			return admission.ServingDownstreamFailed, true
		}
		return admission.ServingUpstreamFailed, true
	}
	return 0, false
}

func validJSONMediaType(header http.Header) bool {
	if len(header.Values("Content-Encoding")) != 0 {
		return false
	}
	values := header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return false
	}
	for name, value := range parameters {
		if !strings.EqualFold(name, "charset") || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

func validUpstreamMetadata(response UpstreamResponse) bool {
	return response.StatusCode == http.StatusOK &&
		response.Body != nil &&
		validJSONMediaType(response.Header)
}

func exactChatCompletionsPath(request *http.Request) bool {
	if request.URL == nil || request.URL.IsAbs() || request.URL.Opaque != "" ||
		request.URL.RawPath != "" || request.URL.RawQuery != "" || request.URL.ForceQuery {
		return false
	}
	return request.URL.Path == chatCompletionsPath
}

func secureRequestID() (string, error) {
	var bytes [16]byte
	if _, err := io.ReadFull(rand.Reader, bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func validRequestID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' && (value[index] < 'a' || value[index] > 'f') {
			return false
		}
	}
	return true
}

type onceReadCloser struct {
	reader io.Reader
	closer io.Closer
	once   sync.Once
	err    error
}

func newOnceReadCloser(body io.ReadCloser) *onceReadCloser {
	return &onceReadCloser{reader: body, closer: body}
}

func (b *onceReadCloser) Read(buffer []byte) (int, error) {
	return b.reader.Read(buffer)
}

func (b *onceReadCloser) Close() error {
	b.once.Do(func() {
		b.err = b.closer.Close()
	})
	return b.err
}
