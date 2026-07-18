package contract

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

const (
	testSSEChunkPayload = `{"id":"opaque","object":"chat.completion.chunk","choices":[{"delta":{"content":"Bakı \uD83D\uDE00"}}]}`
	testSSEChunkWire    = "data: " + testSSEChunkPayload + "\n\n"
	testSSEDoneWire     = "data: [DONE]\n\n"
	readerErrorCanary   = "SSE-READER-CANARY-CONTENT"
)

func testSSELimits() SSELimits {
	return SSELimits{
		MaxTotalBytes: 4096,
		MaxEventBytes: 2048,
		MaxEvents:     16,
	}
}

func newTestSSEDecoder(t *testing.T, reader io.Reader, limits SSELimits) *SSEDecoder {
	t.Helper()
	decoder, err := NewSSEDecoder(reader, limits)
	if err != nil {
		t.Fatalf("NewSSEDecoder() error = %v", err)
	}
	return decoder
}

func nextSSEReason(t *testing.T, body []byte) SSEReason {
	t.Helper()
	decoder := newTestSSEDecoder(t, bytes.NewReader(body), testSSELimits())
	_, err := decoder.Next(context.Background())
	if err == nil {
		t.Fatal("Next() unexpectedly succeeded")
	}
	var streamErr *SSEError
	if !errors.As(err, &streamErr) {
		t.Fatalf("Next() error type = %T, want *SSEError", err)
	}
	return streamErr.Reason()
}

func TestSSEDecoderAcceptsStrictChunkAndTerminalEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chunkWire string
		doneWire  string
		oneByte   bool
	}{
		{name: "LF", chunkWire: testSSEChunkWire, doneWire: testSSEDoneWire},
		{
			name:      "CRLF",
			chunkWire: "data:" + testSSEChunkPayload + "\r\n\r\n",
			doneWire:  "data:[DONE]\r\n\r\n",
			oneByte:   true,
		},
		{
			name:      "mixed accepted line endings",
			chunkWire: "data: " + testSSEChunkPayload + "\r\n\n",
			doneWire:  "data: [DONE]\n\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := test.chunkWire + test.doneWire
			var reader io.Reader = strings.NewReader(body)
			if test.oneByte {
				reader = iotest.OneByteReader(reader)
			}
			decoder := newTestSSEDecoder(t, reader, testSSELimits())

			chunk, err := decoder.Next(context.Background())
			if err != nil {
				t.Fatalf("chunk Next() error = %v", err)
			}
			requireSSEEvent(t, chunk, SSEEventKindChunk, []byte(test.chunkWire))

			done, err := decoder.Next(context.Background())
			if err != nil {
				t.Fatalf("done Next() error = %v", err)
			}
			requireSSEEvent(t, done, SSEEventKindDone, []byte(test.doneWire))

			if _, err := decoder.Next(context.Background()); !errors.Is(err, errSSEVerifyEOFRequired) {
				t.Fatalf("Next() before VerifyEOF() error = %v", err)
			}
			if err := decoder.VerifyEOF(context.Background()); err != nil {
				t.Fatalf("VerifyEOF() error = %v", err)
			}
			if err := decoder.VerifyEOF(context.Background()); err != nil {
				t.Fatalf("second VerifyEOF() error = %v", err)
			}
			if _, err := decoder.Next(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatalf("Next() after verification error = %v, want io.EOF", err)
			}
		})
	}
}

func requireSSEEvent(t *testing.T, event ValidatedSSEEvent, kind SSEEventKind, body []byte) {
	t.Helper()
	if event.Kind() != kind {
		t.Fatalf("Kind() = %v, want %v", event.Kind(), kind)
	}
	if event.BodyBytes() != uint64(len(body)) {
		t.Fatalf("BodyBytes() = %d, want %d", event.BodyBytes(), len(body))
	}
	if cap(event.body) != len(event.body) {
		t.Fatalf("retained event capacity = %d, want exact length %d", cap(event.body), len(event.body))
	}
	copyOne := event.BodyCopy()
	if !bytes.Equal(copyOne, body) {
		t.Fatalf("BodyCopy() = %q, want %q", copyOne, body)
	}
	copyOne[0] = '!'
	if !bytes.Equal(event.BodyCopy(), body) {
		t.Fatal("BodyCopy() exposed mutable event state")
	}
	firstReader := event.BodyReader()
	if _, exposesBacking := firstReader.(io.WriterTo); exposesBacking {
		t.Fatal("BodyReader() exposes private event bytes through io.WriterTo")
	}
	first := make([]byte, 1)
	if _, err := io.ReadFull(firstReader, first); err != nil {
		t.Fatalf("first BodyReader() error = %v", err)
	}
	fromSecond, err := io.ReadAll(event.BodyReader())
	if err != nil || !bytes.Equal(fromSecond, body) {
		t.Fatal("BodyReader() cursors are not independent")
	}
}

func TestSSEDecoderRejectsUnsupportedEventFraming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		body   []byte
		reason SSEReason
	}{
		{name: "empty event LF", body: []byte("\n"), reason: SSEReasonEmptyEvent},
		{name: "empty event CRLF", body: []byte("\r\n"), reason: SSEReasonEmptyEvent},
		{name: "comment", body: []byte(": ping\n\n"), reason: SSEReasonUnsupportedField},
		{name: "event field", body: []byte("event: message\n\n"), reason: SSEReasonUnsupportedField},
		{name: "id field", body: []byte("id: opaque\n\n"), reason: SSEReasonUnsupportedField},
		{name: "retry field", body: []byte("retry: 1000\n\n"), reason: SSEReasonUnsupportedField},
		{name: "unknown field", body: []byte("future: value\n\n"), reason: SSEReasonUnsupportedField},
		{name: "case folded data", body: []byte("Data: {}\n\n"), reason: SSEReasonUnsupportedField},
		{name: "field without colon", body: []byte("data\n\n"), reason: SSEReasonUnsupportedField},
		{name: "two data fields", body: []byte("data: {}\ndata: {}\n\n"), reason: SSEReasonMultipleFields},
		{name: "comment after data", body: []byte("data: {}\n: ping\n\n"), reason: SSEReasonMultipleFields},
		{name: "event after data", body: []byte("data: {}\nevent: message\n\n"), reason: SSEReasonMultipleFields},
		{name: "empty data", body: []byte("data:\n\n"), reason: SSEReasonEmptyData},
		{name: "one stripped space", body: []byte("data: \n\n"), reason: SSEReasonEmptyData},
		{name: "embedded carriage return", body: []byte("data: {}\rjunk\n\n"), reason: SSEReasonInvalidLineEnding},
		{name: "bare carriage return", body: []byte("data: {}\r"), reason: SSEReasonInvalidLineEnding},
		{name: "invalid UTF-8 field", body: []byte{'d', 'a', 't', 0xff, ':', ' ', '{', '}', '\n', '\n'}, reason: SSEReasonInvalidUTF8},
		{name: "invalid UTF-8 delimiter", body: []byte{'d', 'a', 't', 'a', ':', ' ', '{', '}', '\n', 0xff, '\n'}, reason: SSEReasonInvalidUTF8},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if reason := nextSSEReason(t, test.body); reason != test.reason {
				t.Fatalf("reason = %v, want %v", reason, test.reason)
			}
		})
	}
}

func TestSSEDecoderRejectsInvalidChunkObjects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
		reason  SSEReason
	}{
		{name: "malformed", payload: `{"object":`, reason: SSEReasonMalformedJSON},
		{name: "trailing value", payload: `{"object":"chat.completion.chunk"} {}`, reason: SSEReasonTrailingJSON},
		{name: "trailing junk", payload: `{"object":"chat.completion.chunk"} no`, reason: SSEReasonMalformedJSON},
		{name: "duplicate object", payload: `{"object":"chat.completion.chunk","\u006fbject":"chat.completion.chunk"}`, reason: SSEReasonDuplicateKey},
		{name: "duplicate opaque key", payload: `{"object":"chat.completion.chunk","opaque":{"key":1,"\u006bey":2}}`, reason: SSEReasonDuplicateKey},
		{name: "lone high surrogate", payload: `{"object":"chat.completion.chunk","value":"\uD800"}`, reason: SSEReasonInvalidUnicodeEscape},
		{name: "lone low surrogate", payload: `{"object":"chat.completion.chunk","value":"\uDC00"}`, reason: SSEReasonInvalidUnicodeEscape},
		{name: "root array", payload: `[{"object":"chat.completion.chunk"}]`, reason: SSEReasonRootNotObject},
		{name: "root string", payload: `"chat.completion.chunk"`, reason: SSEReasonRootNotObject},
		{name: "missing object", payload: `{"id":"opaque"}`, reason: SSEReasonMissingObject},
		{name: "null object", payload: `{"object":null}`, reason: SSEReasonWrongObjectType},
		{name: "object object", payload: `{"object":{"value":"chat.completion.chunk"}}`, reason: SSEReasonWrongObjectType},
		{name: "wrong object", payload: `{"object":"chat.completion"}`, reason: SSEReasonUnsupportedObject},
		{
			name: "nesting limit",
			payload: `{"object":"chat.completion.chunk","opaque":` +
				strings.Repeat("[", maxJSONDepth) + `0` + strings.Repeat("]", maxJSONDepth) + `}`,
			reason: SSEReasonNestingLimit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := []byte("data: " + test.payload + "\n\n")
			if reason := nextSSEReason(t, body); reason != test.reason {
				t.Fatalf("reason = %v, want %v", reason, test.reason)
			}
		})
	}
}

func TestSSEDecoderRequiresChunkThenDoneThenCleanEOF(t *testing.T) {
	t.Parallel()

	t.Run("done before chunk", func(t *testing.T) {
		if reason := nextSSEReason(t, []byte(testSSEDoneWire)); reason != SSEReasonDoneBeforeChunk {
			t.Fatalf("reason = %v, want done-before-chunk", reason)
		}
	})

	t.Run("empty stream", func(t *testing.T) {
		if reason := nextSSEReason(t, nil); reason != SSEReasonMissingDone {
			t.Fatalf("reason = %v, want missing-done", reason)
		}
	})

	t.Run("chunk without done", func(t *testing.T) {
		decoder := newTestSSEDecoder(t, strings.NewReader(testSSEChunkWire), testSSELimits())
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("chunk Next() error = %v", err)
		}
		_, err := decoder.Next(context.Background())
		requireSSEReason(t, err, SSEReasonMissingDone)
	})

	t.Run("partial first line", func(t *testing.T) {
		if reason := nextSSEReason(t, []byte(`data: {"object":"chat.completion.chunk"}`)); reason != SSEReasonTruncatedEvent {
			t.Fatalf("reason = %v, want truncated-event", reason)
		}
	})

	t.Run("missing delimiter line", func(t *testing.T) {
		if reason := nextSSEReason(t, []byte("data: {\"object\":\"chat.completion.chunk\"}\n")); reason != SSEReasonTruncatedEvent {
			t.Fatalf("reason = %v, want truncated-event", reason)
		}
	})

	t.Run("trailing byte after done", func(t *testing.T) {
		body := testSSEChunkWire + testSSEDoneWire + " "
		decoder := newTestSSEDecoder(t, strings.NewReader(body), testSSELimits())
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("chunk Next() error = %v", err)
		}
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("done Next() error = %v", err)
		}
		requireSSEReason(t, decoder.VerifyEOF(context.Background()), SSEReasonTrailingData)
	})

	t.Run("verify before done", func(t *testing.T) {
		decoder := newTestSSEDecoder(t, strings.NewReader(testSSEChunkWire), testSSELimits())
		requireSSEReason(t, decoder.VerifyEOF(context.Background()), SSEReasonMissingDone)
	})
}

func requireSSEReason(t *testing.T, err error, want SSEReason) {
	t.Helper()
	var streamErr *SSEError
	if !errors.As(err, &streamErr) {
		t.Fatalf("error type = %T, want *SSEError", err)
	}
	if streamErr.Reason() != want {
		t.Fatalf("reason = %v, want %v", streamErr.Reason(), want)
	}
}

func TestSSEDecoderEnforcesAllLimitsAndPhysicalReadBound(t *testing.T) {
	t.Parallel()

	t.Run("event exact and one over", func(t *testing.T) {
		padding := strings.Repeat("x", 64)
		chunk := "data: {\"object\":\"chat.completion.chunk\",\"padding\":\"" + padding + "\"}\n\n"
		stream := chunk + testSSEDoneWire
		limits := SSELimits{MaxTotalBytes: uint64(len(stream)), MaxEventBytes: uint64(len(chunk)), MaxEvents: 2}
		decoder := newTestSSEDecoder(t, strings.NewReader(stream), limits)
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("exact event Next() error = %v", err)
		}

		limits.MaxEventBytes--
		decoder = newTestSSEDecoder(t, strings.NewReader(stream), limits)
		_, err := decoder.Next(context.Background())
		requireSSEReason(t, err, SSEReasonEventBytesLimit)
	})

	t.Run("total exact and one over", func(t *testing.T) {
		padding := strings.Repeat("y", 64)
		chunk := "data: {\"object\":\"chat.completion.chunk\",\"padding\":\"" + padding + "\"}\n\n"
		stream := chunk + testSSEDoneWire
		limits := SSELimits{MaxTotalBytes: uint64(len(stream)), MaxEventBytes: uint64(len(chunk)), MaxEvents: 2}
		decoder := newTestSSEDecoder(t, strings.NewReader(stream), limits)
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("chunk Next() error = %v", err)
		}
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("done Next() error = %v", err)
		}
		if err := decoder.VerifyEOF(context.Background()); err != nil {
			t.Fatalf("exact total VerifyEOF() error = %v", err)
		}

		limits.MaxTotalBytes--
		reader := &sseCountingReader{body: []byte(stream + strings.Repeat("z", 4096))}
		decoder = newTestSSEDecoder(t, reader, limits)
		if _, err := decoder.Next(context.Background()); err != nil {
			t.Fatalf("bounded chunk Next() error = %v", err)
		}
		_, err := decoder.Next(context.Background())
		requireSSEReason(t, err, SSEReasonTotalBytesLimit)
		if reader.bytesRead > int(limits.MaxTotalBytes+1) {
			t.Fatalf("physical bytes read = %d, limit+1 = %d", reader.bytesRead, limits.MaxTotalBytes+1)
		}
	})

	t.Run("event count", func(t *testing.T) {
		body := testSSEChunkWire + testSSEChunkWire + testSSEDoneWire
		limits := testSSELimits()
		limits.MaxEvents = 2
		decoder := newTestSSEDecoder(t, strings.NewReader(body), limits)
		for range 2 {
			if _, err := decoder.Next(context.Background()); err != nil {
				t.Fatalf("chunk Next() error = %v", err)
			}
		}
		_, err := decoder.Next(context.Background())
		requireSSEReason(t, err, SSEReasonEventCountLimit)
	})

	t.Run("event prefetch at most limit plus one", func(t *testing.T) {
		padding := strings.Repeat("p", 2048)
		body := []byte("data: {\"object\":\"chat.completion.chunk\",\"padding\":\"" + padding + "\"}\n\n")
		limits := testSSELimits()
		limits.MaxEventBytes = uint64(len(minimumSSEChunkEvent))
		reader := &sseCountingReader{body: append(body, bytes.Repeat([]byte{'x'}, 4096)...)}
		decoder := newTestSSEDecoder(t, reader, limits)
		_, err := decoder.Next(context.Background())
		requireSSEReason(t, err, SSEReasonEventBytesLimit)
		if reader.bytesRead > int(limits.MaxEventBytes+1) {
			t.Fatalf("physical bytes read = %d, event limit+1 = %d", reader.bytesRead, limits.MaxEventBytes+1)
		}
	})
}

func TestValidateSSELimitsRejectsUnsafeOrUnusableConfiguration(t *testing.T) {
	t.Parallel()
	base := testSSELimits()
	tests := []struct {
		name   string
		mutate func(*SSELimits)
	}{
		{name: "zero total", mutate: func(l *SSELimits) { l.MaxTotalBytes = 0 }},
		{name: "total hard ceiling", mutate: func(l *SSELimits) { l.MaxTotalBytes = AbsoluteMaxSSETotalBytes + 1 }},
		{name: "zero event", mutate: func(l *SSELimits) { l.MaxEventBytes = 0 }},
		{name: "event hard ceiling", mutate: func(l *SSELimits) {
			l.MaxEventBytes = AbsoluteMaxSSEEventBytes + 1
			l.MaxTotalBytes = AbsoluteMaxSSEEventBytes + 1
		}},
		{name: "event above total", mutate: func(l *SSELimits) { l.MaxEventBytes = l.MaxTotalBytes + 1 }},
		{name: "one event", mutate: func(l *SSELimits) { l.MaxEvents = 1 }},
		{name: "events hard ceiling", mutate: func(l *SSELimits) { l.MaxEvents = AbsoluteMaxSSEEvents + 1 }},
		{name: "event cannot fit chunk", mutate: func(l *SSELimits) { l.MaxEventBytes = uint64(len(minimumSSEChunkEvent) - 1) }},
		{name: "total cannot fit stream", mutate: func(l *SSELimits) {
			l.MaxTotalBytes = uint64(len(minimumSSEChunkEvent) + len(minimumSSEDoneEvent) - 1)
			l.MaxEventBytes = uint64(len(minimumSSEChunkEvent))
		}},
		{name: "maximum integers", mutate: func(l *SSELimits) {
			l.MaxTotalBytes = math.MaxUint64
			l.MaxEventBytes = math.MaxUint64
			l.MaxEvents = math.MaxUint64
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			limits := base
			test.mutate(&limits)
			if err := ValidateSSELimits(limits); err == nil {
				t.Fatal("ValidateSSELimits() unexpectedly succeeded")
			}
			if _, err := NewSSEDecoder(strings.NewReader(""), limits); err == nil {
				t.Fatal("NewSSEDecoder() unexpectedly succeeded")
			}
		})
	}

	hard := SSELimits{
		MaxTotalBytes: AbsoluteMaxSSETotalBytes,
		MaxEventBytes: AbsoluteMaxSSEEventBytes,
		MaxEvents:     AbsoluteMaxSSEEvents,
	}
	if err := ValidateSSELimits(hard); err != nil {
		t.Fatalf("ValidateSSELimits() at hard ceilings error = %v", err)
	}
	if _, err := NewSSEDecoder(nil, base); err == nil {
		t.Fatal("NewSSEDecoder(nil) unexpectedly succeeded")
	}
}

func TestSSEDecoderPropagatesCancellationAndClosesReaderFailures(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	decoder := newTestSSEDecoder(t, strings.NewReader(testSSEChunkWire), testSSELimits())
	if _, err := decoder.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Next() error = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	reader := &cancelingReader{cancel: cancel, body: []byte(testSSEChunkWire)}
	decoder = newTestSSEDecoder(t, reader, testSSELimits())
	if _, err := decoder.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-read Next() error = %v, want context.Canceled", err)
	}

	decoder = newTestSSEDecoder(t, sseFailingReader{}, testSSELimits())
	_, err := decoder.Next(context.Background())
	requireSSEReason(t, err, SSEReasonBodyReadFailure)
	if strings.Contains(err.Error(), readerErrorCanary) {
		t.Fatal("SSEError exposed a reader-controlled error string")
	}
	if _, again := decoder.Next(context.Background()); again != err {
		t.Fatal("failed decoder did not return the same safe failure")
	}

	readerWithLateFailure := &sseDataErrorReader{
		body: []byte(testSSEChunkWire + testSSEDoneWire),
		err:  errors.New(readerErrorCanary),
	}
	decoder = newTestSSEDecoder(t, readerWithLateFailure, testSSELimits())
	if _, err := decoder.Next(context.Background()); err != nil {
		t.Fatalf("chunk before late failure Next() error = %v", err)
	}
	if _, err := decoder.Next(context.Background()); err != nil {
		t.Fatalf("done before late failure Next() error = %v", err)
	}
	err = decoder.VerifyEOF(context.Background())
	requireSSEReason(t, err, SSEReasonBodyReadFailure)
	if strings.Contains(err.Error(), readerErrorCanary) {
		t.Fatal("VerifyEOF() exposed a reader-controlled error string")
	}

	body := testSSEChunkWire + testSSEDoneWire
	decoder = newTestSSEDecoder(t, strings.NewReader(body), testSSELimits())
	if _, err := decoder.Next(context.Background()); err != nil {
		t.Fatalf("chunk Next() error = %v", err)
	}
	if _, err := decoder.Next(context.Background()); err != nil {
		t.Fatalf("done Next() error = %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := decoder.VerifyEOF(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled VerifyEOF() error = %v, want context.Canceled", err)
	}

	decoder = newTestSSEDecoder(t, strings.NewReader(body), testSSELimits())
	if _, err := decoder.Next(nil); err == nil {
		t.Fatal("Next(nil) unexpectedly succeeded")
	}
	if err := decoder.VerifyEOF(nil); err == nil {
		t.Fatal("VerifyEOF(nil) unexpectedly succeeded")
	}
}

type sseCountingReader struct {
	body      []byte
	bytesRead int
}

func (r *sseCountingReader) Read(destination []byte) (int, error) {
	if len(r.body) == 0 {
		return 0, io.EOF
	}
	n := copy(destination, r.body)
	r.body = r.body[n:]
	r.bytesRead += n
	return n, nil
}

type sseFailingReader struct{}

func (sseFailingReader) Read([]byte) (int, error) {
	return 0, errors.New(readerErrorCanary)
}

type sseDataErrorReader struct {
	body []byte
	err  error
	done bool
}

func (r *sseDataErrorReader) Read(destination []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(destination, r.body), r.err
}

func FuzzSSEDecoder(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(testSSEChunkWire + testSSEDoneWire),
		[]byte("data: {\"object\":\"chat.completion.chunk\"}\r\n\r\ndata:[DONE]\r\n\r\n"),
		[]byte(": ping\n\n"),
		[]byte{0xff, '\n', '\n'},
	} {
		f.Add(seed)
	}

	limits := testSSELimits()
	f.Fuzz(func(t *testing.T, body []byte) {
		first := decodeSSEForFuzz(t, body, limits)
		second := decodeSSEForFuzz(t, body, limits)
		if !reflect.DeepEqual(first, second) {
			t.Fatal("SSE decoding is not deterministic")
		}
	})
}

type sseFuzzResult struct {
	events  []sseFuzzEvent
	reason  SSEReason
	context string
	clean   bool
}

type sseFuzzEvent struct {
	kind SSEEventKind
	body string
}

func decodeSSEForFuzz(t *testing.T, body []byte, limits SSELimits) sseFuzzResult {
	t.Helper()
	decoder := newTestSSEDecoder(t, bytes.NewReader(body), limits)
	result := sseFuzzResult{}
	for range len(body) + 1 {
		event, err := decoder.Next(context.Background())
		if err != nil {
			if errors.Is(err, io.EOF) {
				result.clean = true
				return result
			}
			var streamErr *SSEError
			if errors.As(err, &streamErr) {
				result.reason = streamErr.Reason()
			} else {
				result.context = err.Error()
			}
			return result
		}
		wire := event.BodyCopy()
		if event.Kind() != SSEEventKindChunk && event.Kind() != SSEEventKindDone {
			t.Fatal("decoder returned an invalid event kind")
		}
		if uint64(len(wire)) > limits.MaxEventBytes || !utf8.Valid(wire) {
			t.Fatal("decoder returned an invalid or oversized event")
		}
		result.events = append(result.events, sseFuzzEvent{kind: event.Kind(), body: string(wire)})
		if event.Kind() == SSEEventKindDone {
			err = decoder.VerifyEOF(context.Background())
			if err == nil {
				result.clean = true
				return result
			}
			var streamErr *SSEError
			if errors.As(err, &streamErr) {
				result.reason = streamErr.Reason()
			} else {
				result.context = err.Error()
			}
			return result
		}
	}
	t.Fatal("decoder did not terminate within the input-derived event bound")
	return result
}
