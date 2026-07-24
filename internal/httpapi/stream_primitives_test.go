package httpapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type streamPrimitiveWriter struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	short    bool
	writeErr error
	flushErr error
	flushes  int
}

func newStreamPrimitiveWriter() *streamPrimitiveWriter {
	return &streamPrimitiveWriter{header: make(http.Header)}
}

func (w *streamPrimitiveWriter) Header() http.Header { return w.header }

func (w *streamPrimitiveWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *streamPrimitiveWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short && len(body) != 0 {
		written := len(body) - 1
		_, _ = w.body.Write(body[:written])
		return written, nil
	}
	return w.body.Write(body)
}

func (w *streamPrimitiveWriter) FlushError() error {
	w.flushes++
	return w.flushErr
}

type streamPrimitiveUnsupportedWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newStreamPrimitiveUnsupportedWriter() *streamPrimitiveUnsupportedWriter {
	return &streamPrimitiveUnsupportedWriter{header: make(http.Header)}
}

func (w *streamPrimitiveUnsupportedWriter) Header() http.Header { return w.header }

func (w *streamPrimitiveUnsupportedWriter) WriteHeader(status int) { w.status = status }

func (w *streamPrimitiveUnsupportedWriter) Write(body []byte) (int, error) {
	return w.body.Write(body)
}

type streamPrimitiveCyclicWriter struct {
	*streamPrimitiveUnsupportedWriter
}

func (w *streamPrimitiveCyclicWriter) Unwrap() http.ResponseWriter { return w }

func TestValidStreamingUpstreamMetadata(t *testing.T) {
	validBody := func() io.ReadCloser { return io.NopCloser(strings.NewReader("unused")) }
	tests := []struct {
		name     string
		response UpstreamResponse
		want     bool
	}{
		{
			name: "exact content type",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       validBody(),
			},
			want: true,
		},
		{
			name: "UTF-8 charset",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"Text/Event-Stream; Charset=UTF-8"}},
				Body:       validBody(),
			},
			want: true,
		},
		{
			name: "non-200 status",
			response: UpstreamResponse{
				StatusCode: http.StatusBadGateway,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       validBody(),
			},
		},
		{
			name: "nil body",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			},
		},
		{
			name: "missing content type",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       validBody(),
			},
		},
		{
			name: "duplicate content type",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream", "text/event-stream"}},
				Body:       validBody(),
			},
		},
		{
			name: "comma-joined content type",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream, text/event-stream"}},
				Body:       validBody(),
			},
		},
		{
			name: "wrong media type",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       validBody(),
			},
		},
		{
			name: "wrong charset",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream; charset=iso-8859-1"}},
				Body:       validBody(),
			},
		},
		{
			name: "unsupported parameter",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream; version=1"}},
				Body:       validBody(),
			},
		},
		{
			name: "content encoding",
			response: UpstreamResponse{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Type":     []string{"text/event-stream"},
					"Content-Encoding": []string{"identity"},
				},
				Body: validBody(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validStreamingUpstreamMetadata(test.response); got != test.want {
				t.Fatalf("validStreamingUpstreamMetadata() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestResponseSinkWritesAndFlushesCanonicalSSEEvents(t *testing.T) {
	writer := newStreamPrimitiveWriter()
	writer.header.Set(requestIDHeader, "0123456789abcdef0123456789abcdef")
	writer.header.Set("Content-Length", "999")
	writer.header.Set("Content-Encoding", "gzip")
	writer.header.Set("X-Upstream-Secret", "do-not-relay")
	sink := &responseSink{writer: writer}
	events := []string{
		"data: {\"object\":\"chat.completion.chunk\"}\n\n",
		"data: [DONE]\n\n",
	}

	for _, event := range events {
		if err := sink.writeSSEEvent(int64(len(event)), strings.NewReader(event)); err != nil {
			t.Fatalf("writeSSEEvent() error = %v", err)
		}
	}

	if writer.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", writer.status)
	}
	if got, want := writer.body.String(), strings.Join(events, ""); got != want {
		t.Fatalf("body = %q, want exact %q", got, want)
	}
	if writer.flushes != len(events) {
		t.Fatalf("flush count = %d, want %d", writer.flushes, len(events))
	}
	wantHeader := http.Header{
		"Cache-Control":          []string{"no-store"},
		"Content-Type":           []string{"text/event-stream"},
		"X-Content-Type-Options": []string{"nosniff"},
		requestIDHeader:          []string{"0123456789abcdef0123456789abcdef"},
	}
	if got := writer.header; !reflect.DeepEqual(got, wantHeader) {
		t.Fatalf("headers = %#v, want exact %#v", got, wantHeader)
	}
	if !sink.committed || !sink.streaming {
		t.Fatalf("sink state = committed:%t streaming:%t, want true/true", sink.committed, sink.streaming)
	}
}

func TestResponseSinkRejectsUnsupportedFlusherBeforeCommit(t *testing.T) {
	tests := []struct {
		name   string
		writer http.ResponseWriter
	}{
		{name: "unsupported", writer: newStreamPrimitiveUnsupportedWriter()},
		{
			name: "cyclic unwrap",
			writer: &streamPrimitiveCyclicWriter{
				streamPrimitiveUnsupportedWriter: newStreamPrimitiveUnsupportedWriter(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.writer.Header().Set("X-Preexisting", "untouched")
			sink := &responseSink{writer: test.writer}
			event := "data: [DONE]\n\n"
			err := sink.writeSSEEvent(int64(len(event)), strings.NewReader(event))
			if !errors.Is(err, http.ErrNotSupported) {
				t.Fatalf("writeSSEEvent() error = %v, want http.ErrNotSupported", err)
			}
			if sink.committed || sink.streaming {
				t.Fatal("unsupported flusher committed a streaming response")
			}
			if got := test.writer.Header(); !reflect.DeepEqual(got, http.Header{"X-Preexisting": []string{"untouched"}}) {
				t.Fatalf("pre-commit headers = %#v, want untouched", got)
			}
		})
	}
}

func TestResponseSinkClassifiesShortWriteBeforeFlush(t *testing.T) {
	writer := newStreamPrimitiveWriter()
	writer.short = true
	sink := &responseSink{writer: writer}
	event := "data: [DONE]\n\n"

	err := sink.writeSSEEvent(int64(len(event)), strings.NewReader(event))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeSSEEvent() error = %v, want io.ErrShortWrite", err)
	}
	if !sink.committed || writer.status != http.StatusOK {
		t.Fatal("short write did not retain the committed response boundary")
	}
	if writer.flushes != 0 {
		t.Fatalf("flush count = %d, want 0 after short write", writer.flushes)
	}
}

func TestResponseSinkReturnsFlushFailureAfterExactWrite(t *testing.T) {
	flushErr := errors.New("flush failed")
	writer := newStreamPrimitiveWriter()
	writer.flushErr = flushErr
	sink := &responseSink{writer: writer}
	event := "data: [DONE]\n\n"

	err := sink.writeSSEEvent(int64(len(event)), strings.NewReader(event))
	if !errors.Is(err, flushErr) {
		t.Fatalf("writeSSEEvent() error = %v, want flush failure", err)
	}
	if got := writer.body.String(); got != event {
		t.Fatalf("body = %q, want exact %q before flush failure", got, event)
	}
	if writer.flushes != 1 {
		t.Fatalf("flush count = %d, want 1", writer.flushes)
	}
	if !sink.committed || writer.status != http.StatusOK {
		t.Fatal("flush failure did not retain the committed response boundary")
	}
}
