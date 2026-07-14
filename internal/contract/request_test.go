package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

func testLimits() Limits {
	return Limits{
		MaxBodyBytes:        4096,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 256,
		MaxCompletionTokens: 512,
		CompletionWeight:    4,
		MaxRequestUnits:     8192,
	}
}

func newTestParser(t *testing.T, limits Limits) *Parser {
	t.Helper()
	parser, err := NewParser("local-model", limits)
	if err != nil {
		t.Fatalf("NewParser() error = %v", err)
	}
	return parser
}

func TestParserReportsValidatedIntegrationLimits(t *testing.T) {
	t.Parallel()
	limits := testLimits()
	parser := newTestParser(t, limits)
	limits.MaxBodyBytes = 1
	limits.MaxRequestUnits = 1

	if got := parser.MaxBodyBytes(); got != testLimits().MaxBodyBytes {
		t.Fatalf("MaxBodyBytes() = %d, want %d", got, testLimits().MaxBodyBytes)
	}
	if got := parser.MaxRequestUnits(); got != testLimits().MaxRequestUnits {
		t.Fatalf("MaxRequestUnits() = %d, want %d", got, testLimits().MaxRequestUnits)
	}
}

func TestRequestBodyReaderDoesNotExposePrivateBackingBytes(t *testing.T) {
	t.Parallel()
	parser := newTestParser(t, testLimits())
	body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":32}`)
	request, err := parser.Parse(context.Background(), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	firstReader := request.BodyReader()
	if _, exposesBackingSlice := firstReader.(io.WriterTo); exposesBackingSlice {
		t.Fatal("BodyReader() exposes its private backing bytes through io.WriterTo")
	}
	fromFirstReader, readErr := io.ReadAll(firstReader)
	if readErr != nil || !bytes.Equal(fromFirstReader, body) {
		t.Fatalf("first BodyReader() = (%q, %v), want exact body", fromFirstReader, readErr)
	}
	fromSecondReader, readErr := io.ReadAll(request.BodyReader())
	if readErr != nil || !bytes.Equal(fromSecondReader, body) {
		t.Fatal("BodyReader() cursors are not independent")
	}
}

func parseReason(t *testing.T, parser *Parser, body []byte) (ErrorClass, Reason) {
	t.Helper()
	_, err := parser.Parse(context.Background(), bytes.NewReader(body))
	if err == nil {
		t.Fatal("Parse() unexpectedly succeeded")
	}
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("Parse() error type = %T, want *ParseError", err)
	}
	return parseErr.Class(), parseErr.Reason()
}

func TestParseAcceptsNonStreamingSubset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantRoles      []Role
		wantTextBytes  uint64
		wantTokens     uint64
		oneByteAtATime bool
	}{
		{
			name:          "minimal",
			body:          `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":32}`,
			wantRoles:     []Role{RoleUser},
			wantTextBytes: 5,
			wantTokens:    32,
		},
		{
			name: "all roles and optional values",
			body: `{
                "model":"local-model",
                "messages":[
                  {"role":"developer","content":"rules"},
                  {"role":"system","content":""},
                  {"role":"user","content":"Bakı"},
                  {"role":"assistant","content":"\uD83D\uDE00"}
                ],
                "max_completion_tokens":64,
                "stream":false,
                "n":1
              }`,
			wantRoles:      []Role{RoleDeveloper, RoleSystem, RoleUser, RoleAssistant},
			wantTextBytes:  uint64(len("rules") + len("Bakı") + len("😀")),
			wantTokens:     64,
			oneByteAtATime: true,
		},
		{
			name:          "legal surrounding whitespace",
			body:          " \n\t{\"model\":\"local-model\",\"messages\":[{\"content\":\"ok\",\"role\":\"user\"}],\"max_completion_tokens\":1}\r\n ",
			wantRoles:     []Role{RoleUser},
			wantTextBytes: 2,
			wantTokens:    1,
		},
		{
			name:          "escaped backslash is not a surrogate",
			body:          `{"model":"local-model","messages":[{"role":"user","content":"\\uD800"}],"max_completion_tokens":1}`,
			wantRoles:     []Role{RoleUser},
			wantTextBytes: uint64(len(`\uD800`)),
			wantTokens:    1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parser := newTestParser(t, testLimits())
			var reader io.Reader = strings.NewReader(test.body)
			if test.oneByteAtATime {
				reader = iotest.OneByteReader(reader)
			}

			request, err := parser.Parse(context.Background(), reader)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if request.Model() != "local-model" {
				t.Fatalf("Model = %q, want local-model", request.Model())
			}
			if request.MaxCompletionTokens() != test.wantTokens {
				t.Fatalf("MaxCompletionTokens = %d, want %d", request.MaxCompletionTokens(), test.wantTokens)
			}
			if request.BodyBytes() != uint64(len(test.body)) {
				t.Fatalf("BodyBytes = %d, want %d", request.BodyBytes(), len(test.body))
			}
			if !bytes.Equal(request.BodyCopy(), []byte(test.body)) {
				t.Fatal("Body does not preserve the exact validated input")
			}
			fromReader, readErr := io.ReadAll(request.BodyReader())
			if readErr != nil || !bytes.Equal(fromReader, []byte(test.body)) {
				t.Fatal("BodyReader does not expose the exact validated input")
			}
			if request.MessageTextBytes() != test.wantTextBytes {
				t.Fatalf("MessageTextBytes = %d, want %d", request.MessageTextBytes(), test.wantTextBytes)
			}
			if request.ReservationUnits() != uint64(len(test.body))+4*test.wantTokens {
				t.Fatalf("ReservationUnits = %d, want %d", request.ReservationUnits(), uint64(len(test.body))+4*test.wantTokens)
			}
			messages := request.Messages()
			roles := make([]Role, len(messages))
			for index := range messages {
				roles[index] = messages[index].Role
			}
			if !reflect.DeepEqual(roles, test.wantRoles) {
				t.Fatalf("roles = %v, want %v", roles, test.wantRoles)
			}
		})
	}
}

func TestParseRejectsAmbiguousOrUnsupportedJSON(t *testing.T) {
	t.Parallel()

	valid := `{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":32}`
	tests := []struct {
		name   string
		body   []byte
		reason Reason
	}{
		{name: "empty", body: nil, reason: ReasonMalformedJSON},
		{name: "whitespace", body: []byte(" \n\t"), reason: ReasonMalformedJSON},
		{name: "root array", body: []byte("[]"), reason: ReasonRootNotObject},
		{name: "root null", body: []byte("null"), reason: ReasonRootNotObject},
		{name: "truncated", body: []byte(`{"model":`), reason: ReasonMalformedJSON},
		{name: "trailing value", body: []byte(valid + ` {}`), reason: ReasonTrailingJSON},
		{name: "trailing junk", body: []byte(valid + ` nope`), reason: ReasonMalformedJSON},
		{name: "byte order mark", body: append([]byte{0xef, 0xbb, 0xbf}, []byte(valid)...), reason: ReasonMalformedJSON},
		{name: "invalid UTF-8", body: append([]byte(valid[:len(valid)-1]), 0xff, '}'), reason: ReasonInvalidUTF8},
		{name: "lone high surrogate", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"\uD800"}],"max_completion_tokens":1}`), reason: ReasonInvalidUnicodeEscape},
		{name: "lone low surrogate", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"\uDC00"}],"max_completion_tokens":1}`), reason: ReasonInvalidUnicodeEscape},
		{name: "wrong surrogate pair", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"\uD800\u0041"}],"max_completion_tokens":1}`), reason: ReasonInvalidUnicodeEscape},
		{name: "duplicate root key", body: []byte(`{"model":"local-model","model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonDuplicateKey},
		{name: "escaped duplicate root key", body: []byte(`{"model":"local-model","\u006dodel":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonDuplicateKey},
		{name: "duplicate message key", body: []byte(`{"model":"local-model","messages":[{"role":"user","role":"assistant","content":"x"}],"max_completion_tokens":1}`), reason: ReasonDuplicateKey},
		{name: "escaped duplicate in unknown object", body: []byte(`{"unknown":{"key":1,"\u006bey":2}}`), reason: ReasonDuplicateKey},
		{name: "unknown root field", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"temperature":0}`), reason: ReasonUnknownField},
		{name: "unknown message field", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x","name":"n"}],"max_completion_tokens":1}`), reason: ReasonUnknownField},
		{name: "missing model", body: []byte(`{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonMissingField},
		{name: "missing messages", body: []byte(`{"model":"local-model","max_completion_tokens":1}`), reason: ReasonMissingField},
		{name: "missing completion", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}]}`), reason: ReasonMissingField},
		{name: "wrong model", body: []byte(`{"model":"other","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonUnsupportedValue},
		{name: "null model", body: []byte(`{"model":null,"messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "object model", body: []byte(`{"model":{},"messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "null messages", body: []byte(`{"model":"local-model","messages":null,"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "object messages", body: []byte(`{"model":"local-model","messages":{},"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "empty messages", body: []byte(`{"model":"local-model","messages":[],"max_completion_tokens":1}`), reason: ReasonUnsupportedValue},
		{name: "message is null", body: []byte(`{"model":"local-model","messages":[null],"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "missing role", body: []byte(`{"model":"local-model","messages":[{"content":"x"}],"max_completion_tokens":1}`), reason: ReasonMissingField},
		{name: "missing content", body: []byte(`{"model":"local-model","messages":[{"role":"user"}],"max_completion_tokens":1}`), reason: ReasonMissingField},
		{name: "null content", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":null}],"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "array content", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":[]}],"max_completion_tokens":1}`), reason: ReasonWrongType},
		{name: "invalid role", body: []byte(`{"model":"local-model","messages":[{"role":"tool","content":"x"}],"max_completion_tokens":1}`), reason: ReasonUnsupportedValue},
		{name: "stream true", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"stream":true}`), reason: ReasonUnsupportedValue},
		{name: "stream null", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"stream":null}`), reason: ReasonWrongType},
		{name: "stream options", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"stream_options":{}}`), reason: ReasonUnsupportedValue},
		{name: "n zero", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":0}`), reason: ReasonUnsupportedValue},
		{name: "n two", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":2}`), reason: ReasonUnsupportedValue},
		{name: "n fraction", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":1.0}`), reason: ReasonWrongType},
		{name: "n exponent", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":1e0}`), reason: ReasonWrongType},
		{name: "n negative", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":-1}`), reason: ReasonWrongType},
		{name: "n overflow", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":18446744073709551616}`), reason: ReasonWrongType},
		{name: "n null", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":null}`), reason: ReasonWrongType},
		{name: "n string", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1,"n":"1"}`), reason: ReasonWrongType},
		{name: "completion zero", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":0}`), reason: ReasonCompletionLimit},
		{name: "completion fraction", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1.5}`), reason: ReasonWrongType},
		{name: "completion exponent", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1e0}`), reason: ReasonWrongType},
		{name: "completion negative", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":-1}`), reason: ReasonWrongType},
		{name: "completion null", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":null}`), reason: ReasonWrongType},
		{name: "completion string", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":"1"}`), reason: ReasonWrongType},
		{name: "completion overflow", body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":18446744073709551616}`), reason: ReasonWrongType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, reason := parseReason(t, newTestParser(t, testLimits()), test.body)
			if reason != test.reason {
				t.Fatalf("reason = %v, want %v", reason, test.reason)
			}
		})
	}
}

func TestParseEnforcesEveryRequestLimit(t *testing.T) {
	t.Parallel()

	minimal := `{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`

	t.Run("body exact and one over", func(t *testing.T) {
		limits := testLimits()
		limits.MaxBodyBytes = uint64(len(minimal))
		if _, err := newTestParser(t, limits).Parse(context.Background(), iotest.OneByteReader(strings.NewReader(minimal))); err != nil {
			t.Fatalf("exact limit Parse() error = %v", err)
		}
		limits.MaxBodyBytes--
		_, err := newTestParser(t, limits).Parse(context.Background(), iotest.OneByteReader(strings.NewReader(minimal)))
		var parseErr *ParseError
		if !errors.As(err, &parseErr) {
			t.Fatalf("one-over error type = %T, want *ParseError", err)
		}
		class, reason := parseErr.Class(), parseErr.Reason()
		if class != ErrorClassRequestTooLarge || reason != ReasonBodyBytesLimit {
			t.Fatalf("got (%v, %v), want request-too-large/body limit", class, reason)
		}
	})

	t.Run("message count", func(t *testing.T) {
		body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"a"},{"role":"assistant","content":"b"}],"max_completion_tokens":1}`)
		limits := testLimits()
		limits.MaxMessageCount = 2
		if _, err := newTestParser(t, limits).Parse(context.Background(), bytes.NewReader(body)); err != nil {
			t.Fatalf("exact limit Parse() error = %v", err)
		}
		limits.MaxMessageCount = 1
		class, reason := parseReason(t, newTestParser(t, limits), body)
		if class != ErrorClassRequestTooLarge || reason != ReasonMessageCountLimit {
			t.Fatalf("got (%v, %v), want request-too-large/message count", class, reason)
		}
	})

	t.Run("escaped decoded text bytes", func(t *testing.T) {
		body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"\u00e9"}],"max_completion_tokens":1}`)
		limits := testLimits()
		limits.MaxMessageTextBytes = uint64(len("é"))
		request, err := newTestParser(t, limits).Parse(context.Background(), bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		if request.MessageTextBytes() != uint64(len("é")) {
			t.Fatalf("MessageTextBytes = %d, want %d", request.MessageTextBytes(), len("é"))
		}
	})

	t.Run("decoded text bytes", func(t *testing.T) {
		body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"é"}],"max_completion_tokens":1}`)
		limits := testLimits()
		limits.MaxMessageTextBytes = uint64(len("é"))
		request, err := newTestParser(t, limits).Parse(context.Background(), bytes.NewReader(body))
		if err != nil {
			t.Fatalf("exact limit Parse() error = %v", err)
		}
		if request.MessageTextBytes() != uint64(len("é")) {
			t.Fatalf("MessageTextBytes = %d, want %d", request.MessageTextBytes(), len("é"))
		}
		limits.MaxMessageTextBytes--
		class, reason := parseReason(t, newTestParser(t, limits), body)
		if class != ErrorClassRequestTooLarge || reason != ReasonMessageBytesLimit {
			t.Fatalf("got (%v, %v), want request-too-large/message bytes", class, reason)
		}
	})

	t.Run("completion", func(t *testing.T) {
		exact := []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":512}`)
		if _, err := newTestParser(t, testLimits()).Parse(context.Background(), bytes.NewReader(exact)); err != nil {
			t.Fatalf("exact limit Parse() error = %v", err)
		}
		body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":513}`)
		_, reason := parseReason(t, newTestParser(t, testLimits()), body)
		if reason != ReasonCompletionLimit {
			t.Fatalf("reason = %v, want completion limit", reason)
		}
	})

	t.Run("reservation exact and one over", func(t *testing.T) {
		limits := testLimits()
		limits.MaxRequestUnits = uint64(len(minimal)) + limits.CompletionWeight
		request, err := newTestParser(t, limits).Parse(context.Background(), strings.NewReader(minimal))
		if err != nil {
			t.Fatalf("exact limit Parse() error = %v", err)
		}
		if request.ReservationUnits() != limits.MaxRequestUnits {
			t.Fatalf("ReservationUnits = %d, want %d", request.ReservationUnits(), limits.MaxRequestUnits)
		}
		limits.MaxRequestUnits--
		_, reason := parseReason(t, newTestParser(t, limits), []byte(minimal))
		if reason != ReasonReservationLimit {
			t.Fatalf("reason = %v, want reservation limit", reason)
		}
	})
}

func TestParseBoundsNestingBeforeSchemaValidation(t *testing.T) {
	t.Parallel()
	body := []byte(`{"unknown":` + strings.Repeat("[", maxJSONDepth+1) + `0` + strings.Repeat("]", maxJSONDepth+1) + `}`)
	_, reason := parseReason(t, newTestParser(t, testLimits()), body)
	if reason != ReasonNestingLimit {
		t.Fatalf("reason = %v, want nesting limit", reason)
	}
}

func TestParseReturnsOnlyClosedReadFailure(t *testing.T) {
	t.Parallel()
	parser := newTestParser(t, testLimits())
	_, err := parser.Parse(context.Background(), failingReader{})
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error type = %T, want *ParseError", err)
	}
	if parseErr.Class() != ErrorClassBodyReadFailure || parseErr.Reason() != ReasonBodyReadFailure {
		t.Fatalf("got (%v, %v), want body read failure", parseErr.Class(), parseErr.Reason())
	}
	if strings.Contains(err.Error(), failingReaderCanary) {
		t.Fatal("ParseError retained a reader-controlled error string")
	}
}

func TestBodyLimitWinsOverReaderError(t *testing.T) {
	t.Parallel()
	limits := testLimits()
	limits.MaxBodyBytes = 128
	reader := dataAndErrorReader{
		body: bytes.Repeat([]byte{'x'}, 129),
		err:  errors.New(failingReaderCanary),
	}
	_, err := newTestParser(t, limits).Parse(context.Background(), &reader)
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error type = %T, want *ParseError", err)
	}
	if parseErr.Class() != ErrorClassRequestTooLarge || parseErr.Reason() != ReasonBodyBytesLimit {
		t.Fatalf("got (%v, %v), want body limit", parseErr.Class(), parseErr.Reason())
	}
}

func TestParsePropagatesContextCancellation(t *testing.T) {
	t.Parallel()
	parser := newTestParser(t, testLimits())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := parser.Parse(ctx, strings.NewReader("{}")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Parse() error = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	reader := cancelingReader{cancel: cancel, body: []byte("{}")}
	if _, err := parser.Parse(ctx, &reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-read Parse() error = %v, want context.Canceled", err)
	}

	staged := newStagedCancellationContext(3)
	readerWithEOF := dataAndErrorReader{body: []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`), err: io.EOF}
	if _, err := parser.Parse(staged, &readerWithEOF); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-read Parse() error = %v, want context.Canceled", err)
	}
}

func TestRequestAccessorsDoNotExposeMutableState(t *testing.T) {
	t.Parallel()
	body := `{"model":"local-model","messages":[{"role":"user","content":"secret"}],"max_completion_tokens":1}`
	request, err := newTestParser(t, testLimits()).Parse(context.Background(), strings.NewReader(body))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cap(request.body) != len(request.body) {
		t.Fatalf("retained body capacity = %d, want exact length %d", cap(request.body), len(request.body))
	}

	bodyCopy := request.BodyCopy()
	bodyCopy[0] = '!'
	if !bytes.Equal(request.BodyCopy(), []byte(body)) {
		t.Fatal("BodyCopy exposed mutable parser state")
	}
	messages := request.Messages()
	messages[0].Content = "changed"
	if request.Messages()[0].Content != "secret" {
		t.Fatal("Messages exposed mutable parser state")
	}
}

func TestParserSupportsConcurrentUse(t *testing.T) {
	t.Parallel()
	parser := newTestParser(t, testLimits())
	body := []byte(`{"model":"local-model","messages":[{"role":"user","content":"x"}],"max_completion_tokens":1}`)

	var group sync.WaitGroup
	errorsSeen := make(chan error, 128)
	for range 128 {
		group.Add(1)
		go func() {
			defer group.Done()
			request, err := parser.Parse(context.Background(), bytes.NewReader(body))
			if err != nil {
				errorsSeen <- err
				return
			}
			if request.ReservationUnits() != uint64(len(body))+testLimits().CompletionWeight {
				errorsSeen <- errors.New("unexpected reservation")
			}
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent Parse() error = %v", err)
	}
}

func TestNewParserRejectsUnsafeConfiguration(t *testing.T) {
	t.Parallel()
	base := testLimits()
	tests := []struct {
		name   string
		model  string
		mutate func(*Limits)
	}{
		{name: "empty model", model: "", mutate: func(*Limits) {}},
		{name: "invalid model UTF-8", model: string([]byte{0xff}), mutate: func(*Limits) {}},
		{name: "model hard ceiling", model: strings.Repeat("m", absoluteMaxModelBytes+1), mutate: func(*Limits) {}},
		{name: "zero body", model: "local-model", mutate: func(l *Limits) { l.MaxBodyBytes = 0 }},
		{name: "body hard ceiling", model: "local-model", mutate: func(l *Limits) { l.MaxBodyBytes = AbsoluteMaxBodyBytes + 1 }},
		{name: "body cannot fit minimal request", model: "local-model", mutate: func(l *Limits) { l.MaxBodyBytes = 1 }},
		{name: "unreadable body bound", model: "local-model", mutate: func(l *Limits) { l.MaxBodyBytes = math.MaxInt64 }},
		{name: "zero messages", model: "local-model", mutate: func(l *Limits) { l.MaxMessageCount = 0 }},
		{name: "zero message bytes", model: "local-model", mutate: func(l *Limits) { l.MaxMessageTextBytes = 0 }},
		{name: "zero completion", model: "local-model", mutate: func(l *Limits) { l.MaxCompletionTokens = 0 }},
		{name: "zero weight", model: "local-model", mutate: func(l *Limits) { l.CompletionWeight = 0 }},
		{name: "zero request units", model: "local-model", mutate: func(l *Limits) { l.MaxRequestUnits = 0 }},
		{name: "units cannot fit minimal request", model: "local-model", mutate: func(l *Limits) { l.MaxRequestUnits = 1 }},
		{name: "completion multiplication overflow", model: "local-model", mutate: func(l *Limits) {
			l.CompletionWeight = math.MaxUint64
			l.MaxCompletionTokens = 2
		}},
		{name: "maximum reservation addition overflow", model: "local-model", mutate: func(l *Limits) {
			l.MaxBodyBytes = AbsoluteMaxBodyBytes
			l.CompletionWeight = math.MaxUint64 - AbsoluteMaxBodyBytes + 1
			l.MaxCompletionTokens = 1
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			limits := base
			test.mutate(&limits)
			if _, err := NewParser(test.model, limits); err == nil {
				t.Fatal("NewParser() unexpectedly succeeded")
			}
		})
	}
}

func TestNewParserAcceptsHardBodyCeiling(t *testing.T) {
	t.Parallel()
	limits := testLimits()
	limits.MaxBodyBytes = AbsoluteMaxBodyBytes
	if _, err := NewParser("local-model", limits); err != nil {
		t.Fatalf("NewParser() at hard ceiling error = %v", err)
	}
}

func TestMinimumJSONStringBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value string
		want  uint64
	}{
		{value: "model", want: uint64(len(`"model"`))},
		{value: `a"b\c`, want: uint64(len(`"a\"b\\c"`))},
		{value: "line\nfeed", want: uint64(len(`"line\nfeed"`))},
		{value: "\x01", want: uint64(len(`"\u0001"`))},
		{value: "Bakı😀", want: uint64(len(`"Bakı😀"`))},
	}
	for _, test := range tests {
		if got := minimumJSONStringBytes(test.value); got != test.want {
			t.Errorf("minimumJSONStringBytes(%q) = %d, want %d", test.value, got, test.want)
		}
	}
}

func TestReservationUnitsChecksArithmetic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		body, tokens, weight uint64
		maximum              uint64
		want                 uint64
		wantReason           Reason
	}{
		{name: "exact maximum", body: 10, tokens: 5, weight: 4, maximum: 30, want: 30},
		{name: "limit", body: 10, tokens: 5, weight: 4, maximum: 29, wantReason: ReasonReservationLimit},
		{name: "multiply overflow", body: 1, tokens: 2, weight: math.MaxUint64, maximum: math.MaxUint64, wantReason: ReasonReservationOverflow},
		{name: "addition overflow", body: math.MaxUint64, tokens: 1, weight: 1, maximum: math.MaxUint64, wantReason: ReasonReservationOverflow},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, reason := reservationUnits(test.body, test.tokens, test.weight, test.maximum)
			if got != test.want || reason != test.wantReason {
				t.Fatalf("reservationUnits() = (%d, %v), want (%d, %v)", got, reason, test.want, test.wantReason)
			}
		})
	}
}

const failingReaderCanary = "CANARY-READER-CONTENT"

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New(failingReaderCanary)
}

type cancelingReader struct {
	cancel context.CancelFunc
	body   []byte
	done   bool
}

type dataAndErrorReader struct {
	body []byte
	err  error
	done bool
}

func (r *dataAndErrorReader) Read(destination []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(destination, r.body), r.err
}

type stagedCancellationContext struct {
	context.Context
	cancelAt int32
	calls    atomic.Int32
	done     chan struct{}
	once     sync.Once
}

func newStagedCancellationContext(cancelAt int32) *stagedCancellationContext {
	return &stagedCancellationContext{
		Context:  context.Background(),
		cancelAt: cancelAt,
		done:     make(chan struct{}),
	}
}

func (c *stagedCancellationContext) Done() <-chan struct{} { return c.done }

func (c *stagedCancellationContext) Err() error {
	if c.calls.Add(1) < c.cancelAt {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.Canceled
}

func (r *cancelingReader) Read(destination []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(destination, r.body)
	r.cancel()
	return n, nil
}

func FuzzParseRequest(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"model":"local-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":32}`),
		[]byte(`{"model":"local-model","model":"other"}`),
		[]byte(`{"model":"local-model","messages":[{"role":"user","content":"\uD83D\uDE00"}],"max_completion_tokens":1}`),
		[]byte{0xff},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	limits := Limits{
		MaxBodyBytes:        4096,
		MaxMessageCount:     8,
		MaxMessageTextBytes: 512,
		MaxCompletionTokens: 1024,
		CompletionWeight:    7,
		MaxRequestUnits:     16384,
	}
	parser, err := NewParser("local-model", limits)
	if err != nil {
		f.Fatalf("NewParser() error = %v", err)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		request, parseErr := parser.Parse(context.Background(), bytes.NewReader(body))
		if parseErr != nil {
			return
		}
		if !utf8.Valid(body) || !json.Valid(body) {
			t.Fatal("Parse() accepted invalid JSON or UTF-8")
		}
		if request.Model() != "local-model" || len(request.Messages()) == 0 || uint64(len(request.Messages())) > limits.MaxMessageCount {
			t.Fatal("Parse() success violated normalized request invariants")
		}
		if request.BodyBytes() != uint64(len(body)) || !bytes.Equal(request.BodyCopy(), body) {
			t.Fatal("Parse() success did not preserve exact body bytes")
		}
		if request.MaxCompletionTokens() == 0 || request.MaxCompletionTokens() > limits.MaxCompletionTokens {
			t.Fatal("Parse() success violated completion bounds")
		}
		var textBytes uint64
		for _, message := range request.Messages() {
			if message.Role < RoleDeveloper || message.Role > RoleAssistant {
				t.Fatal("Parse() success produced an invalid role")
			}
			textBytes += uint64(len(message.Content))
		}
		if textBytes != request.MessageTextBytes() || textBytes > limits.MaxMessageTextBytes {
			t.Fatal("Parse() success violated text-byte accounting")
		}
		wantUnits := uint64(len(body)) + limits.CompletionWeight*request.MaxCompletionTokens()
		if request.ReservationUnits() != wantUnits || wantUnits > limits.MaxRequestUnits {
			t.Fatal("Parse() success violated reservation accounting")
		}

		again, againErr := parser.Parse(context.Background(), bytes.NewReader(body))
		if againErr != nil || !reflect.DeepEqual(request, again) {
			t.Fatal("Parse() is not deterministic")
		}
	})
}

func FuzzReservationUnitsAgainstBigInt(f *testing.F) {
	f.Add(uint64(10), uint64(5), uint64(4), uint64(30))
	f.Add(uint64(math.MaxUint64), uint64(1), uint64(1), uint64(math.MaxUint64))
	f.Add(uint64(1), uint64(2), uint64(math.MaxUint64), uint64(math.MaxUint64))

	maximumUint64 := new(big.Int).SetUint64(math.MaxUint64)
	f.Fuzz(func(t *testing.T, body, tokens, weight, maximum uint64) {
		got, reason := reservationUnits(body, tokens, weight, maximum)

		wantBig := new(big.Int).Mul(new(big.Int).SetUint64(tokens), new(big.Int).SetUint64(weight))
		wantBig.Add(wantBig, new(big.Int).SetUint64(body))
		switch {
		case wantBig.Sign() == 0 || wantBig.Cmp(maximumUint64) > 0:
			if got != 0 || reason != ReasonReservationOverflow {
				t.Fatalf("overflow result = (%d, %v), want (0, %v)", got, reason, ReasonReservationOverflow)
			}
		case wantBig.Cmp(new(big.Int).SetUint64(maximum)) > 0:
			if got != 0 || reason != ReasonReservationLimit {
				t.Fatalf("limit result = (%d, %v), want (0, %v)", got, reason, ReasonReservationLimit)
			}
		default:
			want := wantBig.Uint64()
			if got != want || reason != 0 {
				t.Fatalf("result = (%d, %v), want (%d, 0)", got, reason, want)
			}
		}
	})
}
