package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	// AbsoluteMaxResponseBodyBytes keeps the buffered non-streaming relay
	// allocation bounded on both 32-bit and 64-bit hosts. Sixteen MiB matches
	// the request hard limit while leaving room for the exact-capacity copy.
	AbsoluteMaxResponseBodyBytes = 16 << 20

	chatCompletionObject = "chat.completion"
)

// ResponseLimits bounds every response-owned allocation created by
// ResponseValidator.
type ResponseLimits struct {
	MaxBodyBytes uint64
}

// ResponseValidator is immutable after construction and safe for concurrent
// use.
type ResponseValidator struct {
	maxBodyBytes uint64
}

// ResponseReason is a closed, content-free explanation of an upstream
// response validation failure.
type ResponseReason uint8

const (
	ResponseReasonBodyBytesLimit ResponseReason = iota + 1
	ResponseReasonBodyReadFailure
	ResponseReasonInvalidUTF8
	ResponseReasonInvalidUnicodeEscape
	ResponseReasonMalformedJSON
	ResponseReasonTrailingJSON
	ResponseReasonDuplicateKey
	ResponseReasonNestingLimit
	ResponseReasonRootNotObject
	ResponseReasonMissingObject
	ResponseReasonWrongObjectType
	ResponseReasonUnsupportedObject
)

// ResponseError contains no upstream error, field value, or response body.
type ResponseError struct {
	reason ResponseReason
}

func (e *ResponseError) Error() string {
	switch e.reason {
	case ResponseReasonBodyBytesLimit:
		return "upstream response body exceeds its configured limit"
	case ResponseReasonBodyReadFailure:
		return "upstream response body could not be read"
	case ResponseReasonInvalidUTF8:
		return "upstream response body is not valid UTF-8"
	case ResponseReasonInvalidUnicodeEscape:
		return "upstream response contains an unpaired Unicode surrogate"
	case ResponseReasonMalformedJSON:
		return "upstream response body is not one valid JSON value"
	case ResponseReasonTrailingJSON:
		return "upstream response body contains a trailing JSON value"
	case ResponseReasonDuplicateKey:
		return "upstream response contains a duplicate object key"
	case ResponseReasonNestingLimit:
		return "upstream response exceeds the JSON nesting limit"
	case ResponseReasonRootNotObject:
		return "upstream response root must be an object"
	case ResponseReasonMissingObject:
		return "upstream response is missing its object field"
	case ResponseReasonWrongObjectType:
		return "upstream response object field has the wrong JSON type"
	case ResponseReasonUnsupportedObject:
		return "upstream response object field has an unsupported value"
	default:
		return "upstream response validation failed"
	}
}

func (e *ResponseError) Reason() ResponseReason { return e.reason }

func responseError(reason ResponseReason) error {
	return &ResponseError{reason: reason}
}

// NewResponseValidator validates operator-owned response limits before any
// upstream work is accepted.
func NewResponseValidator(limits ResponseLimits) (*ResponseValidator, error) {
	if limits.MaxBodyBytes == 0 {
		return nil, errors.New("maximum response body bytes must be positive")
	}
	if limits.MaxBodyBytes > AbsoluteMaxResponseBodyBytes {
		return nil, errors.New("maximum response body bytes exceeds the hard safety limit")
	}

	return &ResponseValidator{maxBodyBytes: limits.MaxBodyBytes}, nil
}

// ValidatedResponse is one bounded, validated non-streaming Chat Completions
// response. Its exact body remains privately owned and cannot be mutated by a
// caller.
type ValidatedResponse struct {
	body []byte
}

func (r ValidatedResponse) BodyBytes() uint64 { return uint64(len(r.body)) }

// BodyReader returns a new read-only cursor over the validated body.
func (r ValidatedResponse) BodyReader() io.Reader {
	return &responseBodyReader{body: r.body}
}

// BodyCopy returns an independent copy for integrations that require
// byte-slice ownership.
func (r ValidatedResponse) BodyCopy() []byte {
	return bytes.Clone(r.body)
}

// responseBodyReader intentionally implements only io.Reader. In particular,
// it does not expose bytes.Reader's WriteTo fast path, which would hand the
// privately owned backing slice to a caller-supplied writer.
type responseBodyReader struct {
	body   []byte
	offset int
}

func (r *responseBodyReader) Read(destination []byte) (int, error) {
	if r.offset >= len(r.body) {
		return 0, io.EOF
	}
	n := copy(destination, r.body[r.offset:])
	r.offset += n
	return n, nil
}

// Parse reads at most MaxBodyBytes+1 bytes, then validates one non-streaming
// Chat Completions response. Cancellation is checked around reads; a caller
// must still close a blocked response body when its context is canceled.
func (v *ResponseValidator) Parse(ctx context.Context, r io.Reader) (ValidatedResponse, error) {
	if ctx == nil {
		return ValidatedResponse{}, errors.New("context must not be nil")
	}
	if r == nil {
		return ValidatedResponse{}, errors.New("reader must not be nil")
	}

	body, readErr := io.ReadAll(io.LimitReader(
		contextReader{ctx: ctx, reader: r},
		int64(v.maxBodyBytes+1),
	))
	if uint64(len(body)) > v.maxBodyBytes {
		return ValidatedResponse{}, responseError(ResponseReasonBodyBytesLimit)
	}
	if err := ctx.Err(); err != nil {
		return ValidatedResponse{}, err
	}
	if readErr != nil {
		return ValidatedResponse{}, responseError(ResponseReasonBodyReadFailure)
	}

	// io.ReadAll may retain spare capacity proportional to its growth strategy.
	// Keep only one exact-capacity response-owned allocation.
	compactedBody := make([]byte, len(body))
	copy(compactedBody, body)
	body = compactedBody

	if !utf8.Valid(body) {
		return ValidatedResponse{}, responseError(ResponseReasonInvalidUTF8)
	}
	if err := ctx.Err(); err != nil {
		return ValidatedResponse{}, err
	}
	if !validUnicodeEscapes(ctx, body) {
		if err := ctx.Err(); err != nil {
			return ValidatedResponse{}, err
		}
		return ValidatedResponse{}, responseError(ResponseReasonInvalidUnicodeEscape)
	}
	if err := ctx.Err(); err != nil {
		return ValidatedResponse{}, err
	}
	if reason := validateJSONDocument(ctx, body, maxJSONDepth); reason != 0 {
		if err := ctx.Err(); err != nil {
			return ValidatedResponse{}, err
		}
		return ValidatedResponse{}, responseError(responseReasonFromJSON(reason))
	}
	if err := ctx.Err(); err != nil {
		return ValidatedResponse{}, err
	}
	if reason := validateChatCompletionObject(ctx, body); reason != 0 {
		if err := ctx.Err(); err != nil {
			return ValidatedResponse{}, err
		}
		return ValidatedResponse{}, responseError(reason)
	}
	if err := ctx.Err(); err != nil {
		return ValidatedResponse{}, err
	}

	return ValidatedResponse{body: body}, nil
}

func responseReasonFromJSON(reason Reason) ResponseReason {
	switch reason {
	case ReasonTrailingJSON:
		return ResponseReasonTrailingJSON
	case ReasonDuplicateKey:
		return ResponseReasonDuplicateKey
	case ReasonNestingLimit:
		return ResponseReasonNestingLimit
	default:
		return ResponseReasonMalformedJSON
	}
}

func validateChatCompletionObject(ctx context.Context, body []byte) ResponseReason {
	decoder := newContextDecoder(ctx, body)
	opening, err := decoder.Token()
	if err != nil {
		return ResponseReasonMalformedJSON
	}
	if opening != json.Delim('{') {
		return ResponseReasonRootNotObject
	}

	objectSet := false
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		if keyErr != nil {
			return ResponseReasonMalformedJSON
		}
		key, ok := keyToken.(string)
		if !ok {
			return ResponseReasonMalformedJSON
		}

		if key == "object" {
			value, reason := readString(decoder)
			if reason == ReasonWrongType {
				return ResponseReasonWrongObjectType
			}
			if reason != 0 {
				return ResponseReasonMalformedJSON
			}
			if value != chatCompletionObject {
				return ResponseReasonUnsupportedObject
			}
			objectSet = true
			continue
		}

		// The strict document scan already bounded depth and rejected duplicate
		// keys. Consume all other fields opaquely without narrowing the upstream
		// response contract.
		if reason := scanJSONValue(decoder, 2, maxJSONDepth); reason != 0 {
			return responseReasonFromJSON(reason)
		}
	}

	closing, closeErr := decoder.Token()
	if closeErr != nil || closing != json.Delim('}') {
		return ResponseReasonMalformedJSON
	}
	if !objectSet {
		return ResponseReasonMissingObject
	}
	return 0
}
