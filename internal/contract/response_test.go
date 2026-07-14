package contract

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
)

func newTestResponseValidator(t *testing.T, maximum uint64) *ResponseValidator {
	t.Helper()
	validator, err := NewResponseValidator(ResponseLimits{MaxBodyBytes: maximum})
	if err != nil {
		t.Fatalf("NewResponseValidator() error = %v", err)
	}
	return validator
}

func responseParseReason(t *testing.T, validator *ResponseValidator, body []byte) ResponseReason {
	t.Helper()
	_, err := validator.Parse(context.Background(), bytes.NewReader(body))
	if err == nil {
		t.Fatal("Parse() unexpectedly succeeded")
	}
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("Parse() error type = %T, want *ResponseError", err)
	}
	return responseErr.Reason()
}

func TestResponseValidatorAcceptsOpaqueChatCompletion(t *testing.T) {
	t.Parallel()
	body := []byte(`{
      "id":"opaque-id",
      "choices":[{"message":{"content":"Bakı \uD83D\uDE00","role":"assistant"},"finish_reason":null}],
      "usage":{"prompt_tokens":12,"details":{"cached":true}},
      "object":"chat.completion",
      "future_field":[null,false,1.25,{"nested":"value"}]
    }`)
	validator := newTestResponseValidator(t, uint64(len(body)))

	response, err := validator.Parse(context.Background(), iotest.OneByteReader(bytes.NewReader(body)))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if response.BodyBytes() != uint64(len(body)) {
		t.Fatalf("BodyBytes() = %d, want %d", response.BodyBytes(), len(body))
	}
	if !bytes.Equal(response.BodyCopy(), body) {
		t.Fatal("BodyCopy() did not preserve the exact upstream body")
	}
	fromReader, readErr := io.ReadAll(response.BodyReader())
	if readErr != nil || !bytes.Equal(fromReader, body) {
		t.Fatal("BodyReader() did not preserve the exact upstream body")
	}
}

func TestResponseValidatorEnforcesExactBodyLimit(t *testing.T) {
	t.Parallel()
	body := []byte(`{"object":"chat.completion","opaque":"value"}`)
	validator := newTestResponseValidator(t, uint64(len(body)))
	if _, err := validator.Parse(
		context.Background(),
		iotest.OneByteReader(bytes.NewReader(body)),
	); err != nil {
		t.Fatalf("exact-limit Parse() error = %v", err)
	}

	oneOver := append(bytes.Clone(body), ' ')
	if reason := responseParseReason(t, validator, oneOver); reason != ResponseReasonBodyBytesLimit {
		t.Fatalf("one-over reason = %v, want body-bytes limit", reason)
	}
}

func TestResponseValidatorRejectsInvalidBoundary(t *testing.T) {
	t.Parallel()

	valid := `{"object":"chat.completion"}`
	tests := []struct {
		name   string
		body   []byte
		reason ResponseReason
	}{
		{name: "empty", body: nil, reason: ResponseReasonMalformedJSON},
		{name: "malformed", body: []byte(`{"object":`), reason: ResponseReasonMalformedJSON},
		{name: "invalid UTF-8", body: []byte{'{', '"', 'o', 'b', 'j', 'e', 'c', 't', '"', ':', '"', 0xff, '"', '}'}, reason: ResponseReasonInvalidUTF8},
		{name: "lone high surrogate", body: []byte(`{"object":"chat.completion","value":"\uD800"}`), reason: ResponseReasonInvalidUnicodeEscape},
		{name: "lone low surrogate", body: []byte(`{"object":"chat.completion","value":"\uDC00"}`), reason: ResponseReasonInvalidUnicodeEscape},
		{name: "duplicate root key", body: []byte(`{"object":"chat.completion","\u006fbject":"chat.completion"}`), reason: ResponseReasonDuplicateKey},
		{name: "duplicate opaque key", body: []byte(`{"object":"chat.completion","opaque":{"key":1,"\u006bey":2}}`), reason: ResponseReasonDuplicateKey},
		{name: "trailing value", body: []byte(valid + ` {}`), reason: ResponseReasonTrailingJSON},
		{name: "trailing junk", body: []byte(valid + ` no`), reason: ResponseReasonMalformedJSON},
		{name: "root array", body: []byte(`[{"object":"chat.completion"}]`), reason: ResponseReasonRootNotObject},
		{name: "root string", body: []byte(`"chat.completion"`), reason: ResponseReasonRootNotObject},
		{name: "missing object", body: []byte(`{"id":"opaque"}`), reason: ResponseReasonMissingObject},
		{name: "null object", body: []byte(`{"object":null}`), reason: ResponseReasonWrongObjectType},
		{name: "object object", body: []byte(`{"object":{"value":"chat.completion"}}`), reason: ResponseReasonWrongObjectType},
		{name: "wrong object value", body: []byte(`{"object":"chat.completion.chunk"}`), reason: ResponseReasonUnsupportedObject},
		{
			name: "nesting limit",
			body: []byte(`{"object":"chat.completion","opaque":` +
				strings.Repeat("[", maxJSONDepth) + `0` + strings.Repeat("]", maxJSONDepth) + `}`),
			reason: ResponseReasonNestingLimit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validator := newTestResponseValidator(t, 4096)
			if reason := responseParseReason(t, validator, test.body); reason != test.reason {
				t.Fatalf("reason = %v, want %v", reason, test.reason)
			}
		})
	}
}

func TestResponseValidatorReturnsClosedReadFailure(t *testing.T) {
	t.Parallel()
	reader := &responseDataAndErrorReader{
		body: []byte(`{"object":`),
		err:  errors.New(responseReaderCanary),
	}
	_, err := newTestResponseValidator(t, 4096).Parse(context.Background(), reader)
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want *ResponseError", err)
	}
	if responseErr.Reason() != ResponseReasonBodyReadFailure {
		t.Fatalf("reason = %v, want body-read failure", responseErr.Reason())
	}
	if strings.Contains(err.Error(), responseReaderCanary) {
		t.Fatal("ResponseError exposed a reader-controlled error string")
	}
}

func TestResponseBodyLimitWinsOverReaderFailure(t *testing.T) {
	t.Parallel()
	reader := &responseDataAndErrorReader{
		body: bytes.Repeat([]byte{'x'}, 65),
		err:  errors.New(responseReaderCanary),
	}
	_, err := newTestResponseValidator(t, 64).Parse(context.Background(), reader)
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want *ResponseError", err)
	}
	if responseErr.Reason() != ResponseReasonBodyBytesLimit {
		t.Fatalf("reason = %v, want body-bytes limit", responseErr.Reason())
	}
}

func TestResponseValidatorPropagatesCancellation(t *testing.T) {
	t.Parallel()
	validator := newTestResponseValidator(t, 4096)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validator.Parse(ctx, strings.NewReader(`{"object":"chat.completion"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Parse() error = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	reader := &responseCancelingReader{
		cancel: cancel,
		body:   []byte(`{"object":"chat.completion"}`),
	}
	if _, err := validator.Parse(ctx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-read Parse() error = %v, want context.Canceled", err)
	}
}

func TestValidatedResponseDoesNotExposeMutableState(t *testing.T) {
	t.Parallel()
	body := []byte(`{"object":"chat.completion","value":"private"}`)
	response, err := newTestResponseValidator(t, 4096).Parse(context.Background(), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cap(response.body) != len(response.body) {
		t.Fatalf("retained body capacity = %d, want exact length %d", cap(response.body), len(response.body))
	}

	copyOne := response.BodyCopy()
	copyOne[0] = '!'
	if !bytes.Equal(response.BodyCopy(), body) {
		t.Fatal("BodyCopy() exposed mutable validator state")
	}

	firstReader := response.BodyReader()
	if _, exposesBackingSlice := firstReader.(io.WriterTo); exposesBackingSlice {
		t.Fatal("BodyReader() exposes its private backing bytes through io.WriterTo")
	}
	firstByte := make([]byte, 1)
	if _, readErr := io.ReadFull(firstReader, firstByte); readErr != nil {
		t.Fatalf("first BodyReader() error = %v", readErr)
	}
	fromSecondReader, readErr := io.ReadAll(response.BodyReader())
	if readErr != nil || !bytes.Equal(fromSecondReader, body) {
		t.Fatal("BodyReader() cursors are not independent")
	}
}

func TestResponseValidatorSupportsConcurrentUse(t *testing.T) {
	t.Parallel()
	validator := newTestResponseValidator(t, 4096)
	body := []byte(`{"object":"chat.completion","value":1}`)

	var group sync.WaitGroup
	errorsSeen := make(chan error, 64)
	for range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			response, err := validator.Parse(context.Background(), bytes.NewReader(body))
			if err != nil {
				errorsSeen <- err
				return
			}
			if response.BodyBytes() != uint64(len(body)) {
				errorsSeen <- errors.New("unexpected body length")
			}
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent Parse() error = %v", err)
	}
}

func TestNewResponseValidatorRejectsUnsafeConfiguration(t *testing.T) {
	t.Parallel()
	for _, maximum := range []uint64{0, AbsoluteMaxResponseBodyBytes + 1, math.MaxUint64} {
		if _, err := NewResponseValidator(ResponseLimits{MaxBodyBytes: maximum}); err == nil {
			t.Fatalf("NewResponseValidator(%d) unexpectedly succeeded", maximum)
		}
	}
	if _, err := NewResponseValidator(ResponseLimits{MaxBodyBytes: AbsoluteMaxResponseBodyBytes}); err != nil {
		t.Fatalf("NewResponseValidator() at the 32-bit-safe hard ceiling error = %v", err)
	}
}

func TestResponseValidatorRejectsNilInputs(t *testing.T) {
	t.Parallel()
	validator := newTestResponseValidator(t, 4096)
	if _, err := validator.Parse(nil, strings.NewReader(`{}`)); err == nil {
		t.Fatal("Parse(nil, reader) unexpectedly succeeded")
	}
	if _, err := validator.Parse(context.Background(), nil); err == nil {
		t.Fatal("Parse(ctx, nil) unexpectedly succeeded")
	}
}

const responseReaderCanary = "RESPONSE-READER-CANARY-CONTENT"

type responseDataAndErrorReader struct {
	body []byte
	err  error
	done bool
}

func (r *responseDataAndErrorReader) Read(destination []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(destination, r.body), r.err
}

type responseCancelingReader struct {
	cancel context.CancelFunc
	body   []byte
	done   bool
}

func (r *responseCancelingReader) Read(destination []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(destination, r.body)
	r.cancel()
	return n, nil
}
