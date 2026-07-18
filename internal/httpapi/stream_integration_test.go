package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/omar07ibrahim/ssemaphore/internal/admission"
	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const streamIntegrationRequest = `{"model":"portfolio-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":8,"stream":true}`

type streamIntegrationUpstream struct {
	body  io.ReadCloser
	calls atomic.Int32
}

func (u *streamIntegrationUpstream) Complete(context.Context, contract.Request) (UpstreamResponse, error) {
	u.calls.Add(1)
	return UpstreamResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       u.body,
	}, nil
}

func streamIntegrationRequestFor() *http.Request {
	request := httptest.NewRequest(http.MethodPost, chatCompletionsPath, strings.NewReader(streamIntegrationRequest))
	request.Header.Set("Authorization", "Bearer tenant-one-primary")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestNewHandlerHoldsRealPermitUntilTerminalSSEFlush(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	body := newRelayTestBody(relayTestChunkOne + relayTestDone)
	body.block = true
	upstream := &streamIntegrationUpstream{body: body}
	handler, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	writer := newRelayTestWriter()
	done := relayTestRunAsync(handler, writer, streamIntegrationRequestFor())

	writer.waitFlush(t)
	body.waitBlocked(t)
	snapshot := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global.InflightCount == 1
	})
	if snapshot.Global.QueuedCount != 0 {
		t.Fatalf("scheduler counters before terminal EOF = %+v, want one inflight and none queued", snapshot.Global)
	}
	status, responseBody, flushes, _ := writer.snapshot()
	if status != http.StatusOK || responseBody != relayTestChunkOne || flushes != 1 {
		t.Fatalf("response before terminal EOF = (%d, %q, %d flushes), want one committed chunk", status, responseBody, flushes)
	}

	body.allowEOF()
	relayTestWaitDone(t, done)
	status, responseBody, flushes, _ = writer.snapshot()
	if want := relayTestChunkOne + relayTestDone; status != http.StatusOK || responseBody != want || flushes != 2 {
		t.Fatalf("terminal response = (%d, %q, %d flushes), want exact %q", status, responseBody, flushes, want)
	}
	if upstream.calls.Load() != 1 || body.closeCalls.Load() != 1 {
		t.Fatalf("upstream lifecycle calls = complete:%d close:%d, want 1/1", upstream.calls.Load(), body.closeCalls.Load())
	}
	terminal := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global == (admission.Counters{})
	})
	if terminal.Global != (admission.Counters{}) {
		t.Fatalf("terminal global counters = %+v, want zero", terminal.Global)
	}
}

func TestNewHandlerReleasesRealPermitAfterCommittedSSEFailure(t *testing.T) {
	parser := configTestNewParser(t, configTestMaxBodyBytes, configTestMaxRequestUnits)
	scheduler := configTestNewScheduler(t, nil)
	body := newRelayTestBody(relayTestChunkOne + "data: not-json\n\n")
	upstream := &streamIntegrationUpstream{body: body}
	handler, err := NewHandler(configTestBaseHandlerConfig(), parser, scheduler, upstream)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	writer := newRelayTestWriter()

	handler.ServeHTTP(writer, streamIntegrationRequestFor())

	status, responseBody, flushes, _ := writer.snapshot()
	if status != http.StatusOK || responseBody != relayTestChunkOne || flushes != 1 {
		t.Fatalf("failed stream = (%d, %q, %d flushes), want one truncated chunk", status, responseBody, flushes)
	}
	if upstream.calls.Load() != 1 || body.closeCalls.Load() != 1 {
		t.Fatalf("upstream lifecycle calls = complete:%d close:%d, want 1/1", upstream.calls.Load(), body.closeCalls.Load())
	}
	terminal := waitForIntegrationSnapshot(t, scheduler, func(snapshot admission.Snapshot) bool {
		return snapshot.Global == (admission.Counters{})
	})
	if terminal.Global != (admission.Counters{}) {
		t.Fatalf("terminal global counters = %+v, want zero", terminal.Global)
	}
}
