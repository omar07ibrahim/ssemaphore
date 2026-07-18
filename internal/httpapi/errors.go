package httpapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
)

type publicError struct {
	status  int
	code    string
	message string
}

var (
	errUnsupportedPath   = publicError{http.StatusNotFound, "unsupported_path", "The requested endpoint is not available."}
	errUnsupportedMethod = publicError{http.StatusMethodNotAllowed, "unsupported_method", "Only POST is supported for this endpoint."}
	errInvalidCredential = publicError{http.StatusUnauthorized, "invalid_tenant_credential", "A valid tenant bearer credential is required."}
	errUnsupportedMedia  = publicError{http.StatusUnsupportedMediaType, "unsupported_media_type", "The request must contain unencoded application/json."}
	errInvalidRequest    = publicError{http.StatusBadRequest, "invalid_request", "The request does not match the supported contract."}
	errRequestTooLarge   = publicError{http.StatusRequestEntityTooLarge, "request_too_large", "The request exceeds a configured safety limit."}
	errTenantCapacity    = publicError{http.StatusTooManyRequests, "tenant_capacity_exhausted", "The tenant has no request capacity available."}
	errOverloaded        = publicError{http.StatusServiceUnavailable, "overloaded", "The service has no global request capacity available."}
	errQueueDeadline     = publicError{http.StatusServiceUnavailable, "queue_deadline_exceeded", "The request could not be admitted before its queue deadline."}
	errDraining          = publicError{http.StatusServiceUnavailable, "draining", "The service is draining and cannot serve this request."}
	errUpstreamTimeout   = publicError{http.StatusGatewayTimeout, "upstream_timeout", "The upstream did not complete before its deadline."}
	errBadUpstream       = publicError{http.StatusBadGateway, "invalid_upstream_response", "The upstream response could not be safely relayed."}
	errInternal          = publicError{http.StatusInternalServerError, "internal_error", "The request could not be completed safely."}
)

type responseSink struct {
	writer    http.ResponseWriter
	committed bool
	streaming bool
}

func (s *responseSink) writeJSON(status int, body []byte) error {
	return s.writeJSONReader(status, int64(len(body)), bytes.NewReader(body))
}

func (s *responseSink) writeJSONReader(status int, length int64, body io.Reader) error {
	if s.committed {
		return errors.New("response already committed")
	}
	header := s.writer.Header()
	header.Set("Content-Type", "application/json")
	header.Set("Content-Length", strconv.FormatInt(length, 10))
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	s.committed = true
	s.writer.WriteHeader(status)
	written, err := io.CopyN(s.writer, body, length)
	if err == nil && written != length {
		return io.ErrShortWrite
	}
	return err
}

func (s *responseSink) writeError(failure publicError) error {
	body := []byte(`{"error":{"code":"` + failure.code + `","message":"` + failure.message + `"}}` + "\n")
	return s.writeJSON(failure.status, body)
}

// writeSSEEvent commits the canonical streaming response on its first call,
// writes one already-validated event in full, and flushes that event before it
// returns. It never copies an upstream response header or sets Content-Length.
func (s *responseSink) writeSSEEvent(length int64, body io.Reader) error {
	if length <= 0 {
		return errors.New("SSE event length must be positive")
	}
	if body == nil {
		return errors.New("SSE event body must not be nil")
	}
	if s.committed && !s.streaming {
		return errors.New("response already committed")
	}
	if !supportsResponseFlush(s.writer) {
		return http.ErrNotSupported
	}
	if !s.committed {
		header := s.writer.Header()
		requestID := header.Get(requestIDHeader)
		clear(header)
		header.Set("Content-Type", "text/event-stream")
		header.Set("Cache-Control", "no-store")
		header.Set("X-Content-Type-Options", "nosniff")
		if requestID != "" {
			header.Set(requestIDHeader, requestID)
		}
		s.streaming = true
		s.committed = true
		s.writer.WriteHeader(http.StatusOK)
	}

	written, err := io.CopyN(s.writer, body, length)
	if err == nil && written != length {
		return io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	return http.NewResponseController(s.writer).Flush()
}

func supportsResponseFlush(writer http.ResponseWriter) bool {
	seen := make(map[http.ResponseWriter]struct{}, 4)
	for range 64 {
		if writer == nil {
			return false
		}
		if concrete := reflect.TypeOf(writer); concrete.Comparable() {
			if _, duplicate := seen[writer]; duplicate {
				return false
			}
			seen[writer] = struct{}{}
		}
		if _, ok := writer.(interface{ FlushError() error }); ok {
			return true
		}
		if _, ok := writer.(http.Flusher); ok {
			return true
		}
		unwrapper, ok := writer.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		writer = unwrapper.Unwrap()
	}
	return false
}
