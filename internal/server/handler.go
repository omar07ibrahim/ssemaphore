package server

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

var errHandlerWaitContextNil = errors.New("handler wait context must not be nil")

// trackedHandler prevents new application work once forced shutdown begins and
// records application handlers because net/http.Server.Close does not wait for
// their goroutines to return.
type trackedHandler struct {
	inner http.Handler

	mu        sync.Mutex
	accepting bool
	active    uint64
	drained   chan struct{}
}

func newTrackedHandler(inner http.Handler) *trackedHandler {
	return &trackedHandler{
		inner:     inner,
		accepting: true,
		drained:   make(chan struct{}),
	}
}

func (h *trackedHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Go's HTTP/1 server deliberately passes the HTTP/2 connection preface to
	// Handler so applications can implement h2c themselves. This server never
	// upgrades protocols, so reject that parser exception before application
	// work or admission accounting.
	if request.ProtoMajor != 1 {
		writer.Header().Set("Connection", "close")
		writer.WriteHeader(http.StatusHTTPVersionNotSupported)
		return
	}
	if !h.begin() {
		// A request can cross net/http's dispatch boundary immediately before
		// Server.Close. Keep this path content-free and synchronous.
		writer.Header().Set("Connection", "close")
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer h.finish()
	h.inner.ServeHTTP(writer, request)
}

func (h *trackedHandler) begin() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.accepting {
		return false
	}
	h.active++
	return true
}

func (h *trackedHandler) finish() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active == 0 {
		panic("server: active handler count underflow")
	}
	h.active--
	if !h.accepting && h.active == 0 {
		close(h.drained)
	}
}

func (h *trackedHandler) seal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.accepting {
		return
	}
	h.accepting = false
	if h.active == 0 {
		close(h.drained)
	}
}

func (h *trackedHandler) wait(ctx context.Context) error {
	if ctx == nil {
		return errHandlerWaitContextNil
	}
	select {
	case <-h.drained:
		return nil
	default:
	}
	select {
	case <-h.drained:
		return nil
	case <-ctx.Done():
		select {
		case <-h.drained:
			return nil
		default:
			return ctx.Err()
		}
	}
}
