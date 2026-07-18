package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

func TestNewEnforcesHandlerSchedulerOwnershipAndOwnsValidatedListener(t *testing.T) {
	ownedScheduler := newServerConstructionScheduler(t)
	otherScheduler := newServerConstructionScheduler(t)
	handler := newServerConstructionHandler(t, ownedScheduler)

	rejectedListener := newServerConstructionListener(t)
	if server, err := New(serverConstructionConfig(), rejectedListener, handler, otherScheduler); err == nil || server != nil {
		t.Fatalf("New() with split scheduler ownership = (%v, %v), want rejection", server, err)
	}
	// New must not take ownership on rejection. A first Close returns nil; a
	// listener already closed by New would return net.ErrClosed here.
	if err := rejectedListener.Close(); err != nil {
		t.Fatalf("rejected listener was modified by New: %v", err)
	}

	ownedListener := newServerConstructionListener(t)
	server, err := New(serverConstructionConfig(), ownedListener, handler, ownedScheduler)
	if err != nil {
		t.Fatalf("New() with matching ownership error = %v", err)
	}
	if server == nil {
		t.Fatal("New() with matching ownership returned nil server")
	}
	result, err := server.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown() before Serve error = %v", err)
	}
	if result.Forced {
		t.Fatalf("Shutdown() before Serve result = %+v, want graceful", result)
	}
	if err := ownedListener.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("owned listener Close after Shutdown = %v, want net.ErrClosed", err)
	}
}

func newServerConstructionHandler(t *testing.T, scheduler *admission.Scheduler) *httpapi.Handler {
	t.Helper()
	parser, err := contract.NewParser("portfolio-model", contract.Limits{
		MaxBodyBytes:        512,
		MaxMessageCount:     4,
		MaxMessageTextBytes: 128,
		MaxCompletionTokens: 64,
		CompletionWeight:    1,
		MaxRequestUnits:     1024,
	})
	if err != nil {
		t.Fatalf("contract.NewParser() error = %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		DefaultQueueTimeout:    time.Second,
		BodyReadTimeout:        time.Second,
		UpstreamTimeout:        time.Second,
		StreamReadTimeout:      250 * time.Millisecond,
		StreamEventTimeout:     500 * time.Millisecond,
		MaxResponseBodyBytes:   512,
		MaxStreamEventBytes:    256,
		MaxStreamEvents:        8,
		GlobalPreDispatchLimit: 1,
		TenantPreDispatch: []httpapi.TenantPreDispatchLimit{{
			Tenant: 1,
			Count:  1,
		}},
		Credentials: []httpapi.Credential{{
			Tenant: 1,
			Token:  "construction-test-token",
		}},
	}, parser, scheduler, serverConstructionUpstream{})
	if err != nil {
		t.Fatalf("httpapi.NewHandler() error = %v", err)
	}
	return handler
}

type serverConstructionUpstream struct{}

func (serverConstructionUpstream) Complete(context.Context, contract.Request) (httpapi.UpstreamResponse, error) {
	return httpapi.UpstreamResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"object":"chat.completion","choices":[]}`)),
	}, nil
}

func newServerConstructionScheduler(t *testing.T) *admission.Scheduler {
	t.Helper()
	scheduler, err := admission.New(admission.Config{
		MaxBodyBytes:    512,
		MaxRequestUnits: 1024,
		BaseQuantum:     1024,
		DeficitCap:      2048,
		GlobalQueue:     admission.QueueLimits{Count: 2, Bytes: 1024, Work: 2048},
		GlobalInflight:  admission.InflightLimits{Count: 1, Work: 1024},
		Tenants: []admission.TenantConfig{{
			ID:       1,
			Weight:   1,
			Queue:    admission.QueueLimits{Count: 2, Bytes: 1024, Work: 2048},
			Inflight: admission.InflightLimits{Count: 1, Work: 1024},
		}},
	})
	if err != nil {
		t.Fatalf("admission.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Close(ctx); err != nil {
			t.Errorf("Scheduler.Close() cleanup error = %v", err)
		}
	})
	return scheduler
}

func newServerConstructionListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("net.ListenTCP() error = %v", err)
	}
	return listener
}

func serverConstructionConfig() Config {
	return Config{
		HeaderReadTimeout:       time.Second,
		ResponseWriteTimeout:    time.Second,
		IdleTimeout:             time.Second,
		GraceTimeout:            time.Second,
		ForceTimeout:            time.Second,
		HeaderReadEnvelopeBytes: absoluteMinHeaderReadEnvelopeBytes,
		MaxConnections:          1,
	}
}
