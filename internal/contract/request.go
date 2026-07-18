// Package contract validates the deliberately small Chat Completions request
// surface accepted by SSEmaphore.
package contract

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/bits"
	"slices"
	"unicode/utf8"
)

const (
	maxJSONDepth = 32

	// AbsoluteMaxBodyBytes prevents operator configuration from turning one
	// request into an unbounded allocation, even on a 64-bit host.
	AbsoluteMaxBodyBytes  = 16 << 20
	absoluteMaxModelBytes = 1024
)

// Limits bounds every request-owned allocation and scheduling quantity created
// by Parser.
type Limits struct {
	MaxBodyBytes        uint64
	MaxMessageCount     uint64
	MaxMessageTextBytes uint64
	MaxCompletionTokens uint64
	CompletionWeight    uint64
	MaxRequestUnits     uint64
}

// Parser is immutable after construction and safe for concurrent use.
type Parser struct {
	publicModel string
	limits      Limits
}

// MaxBodyBytes reports the validated request-body bound used by this parser.
func (p *Parser) MaxBodyBytes() uint64 { return p.limits.MaxBodyBytes }

// MaxRequestUnits reports the validated reservation bound used by this parser.
func (p *Parser) MaxRequestUnits() uint64 { return p.limits.MaxRequestUnits }

// ErrorClass is the stable HTTP-facing category of a parse failure.
type ErrorClass uint8

const (
	ErrorClassInvalidRequest ErrorClass = iota + 1
	ErrorClassRequestTooLarge
	ErrorClassBodyReadFailure
)

// Reason is a closed, content-free explanation of a parse failure.
type Reason uint8

const (
	ReasonBodyBytesLimit Reason = iota + 1
	ReasonBodyReadFailure
	ReasonInvalidUTF8
	ReasonInvalidUnicodeEscape
	ReasonMalformedJSON
	ReasonTrailingJSON
	ReasonDuplicateKey
	ReasonNestingLimit
	ReasonRootNotObject
	ReasonUnknownField
	ReasonMissingField
	ReasonWrongType
	ReasonUnsupportedValue
	ReasonMessageCountLimit
	ReasonMessageBytesLimit
	ReasonCompletionLimit
	ReasonReservationOverflow
	ReasonReservationLimit
)

// ParseError never retains decoder errors, field names, values, or body data.
type ParseError struct {
	class  ErrorClass
	reason Reason
}

func (e *ParseError) Error() string {
	switch e.reason {
	case ReasonBodyBytesLimit:
		return "request body exceeds its configured limit"
	case ReasonBodyReadFailure:
		return "request body could not be read"
	case ReasonInvalidUTF8:
		return "request body is not valid UTF-8"
	case ReasonInvalidUnicodeEscape:
		return "request contains an unpaired Unicode surrogate"
	case ReasonMalformedJSON:
		return "request body is not one valid JSON value"
	case ReasonTrailingJSON:
		return "request body contains a trailing JSON value"
	case ReasonDuplicateKey:
		return "request contains a duplicate object key"
	case ReasonNestingLimit:
		return "request exceeds the JSON nesting limit"
	case ReasonRootNotObject:
		return "request root must be an object"
	case ReasonUnknownField:
		return "request contains an unsupported field"
	case ReasonMissingField:
		return "request is missing a required field"
	case ReasonWrongType:
		return "request field has the wrong JSON type"
	case ReasonUnsupportedValue:
		return "request field has an unsupported value"
	case ReasonMessageCountLimit:
		return "request exceeds the configured message-count limit"
	case ReasonMessageBytesLimit:
		return "request exceeds the configured message-text limit"
	case ReasonCompletionLimit:
		return "request exceeds the configured completion limit"
	case ReasonReservationOverflow:
		return "request reservation arithmetic overflowed"
	case ReasonReservationLimit:
		return "request exceeds the configured reservation limit"
	default:
		return "request validation failed"
	}
}

func (e *ParseError) Class() ErrorClass { return e.class }

func (e *ParseError) Reason() Reason { return e.reason }

func parseError(class ErrorClass, reason Reason) error {
	return &ParseError{class: class, reason: reason}
}

// NewParser validates all operator-owned configuration before accepting work.
func NewParser(publicModel string, limits Limits) (*Parser, error) {
	if publicModel == "" {
		return nil, errors.New("public model must not be empty")
	}
	if !utf8.ValidString(publicModel) {
		return nil, errors.New("public model must be valid UTF-8")
	}
	if len(publicModel) > absoluteMaxModelBytes {
		return nil, errors.New("public model exceeds its hard safety limit")
	}
	if limits.MaxBodyBytes == 0 {
		return nil, errors.New("maximum body bytes must be positive")
	}
	if limits.MaxBodyBytes > AbsoluteMaxBodyBytes {
		return nil, errors.New("maximum body bytes exceeds the hard safety limit")
	}
	if limits.MaxBodyBytes >= uint64(maxInt()) {
		return nil, errors.New("maximum body bytes must fit a bounded reader")
	}
	if limits.MaxMessageCount == 0 {
		return nil, errors.New("maximum message count must be positive")
	}
	if limits.MaxMessageCount > uint64(maxInt()) {
		return nil, errors.New("maximum message count must fit an allocation")
	}
	if limits.MaxMessageTextBytes == 0 {
		return nil, errors.New("maximum message text bytes must be positive")
	}
	if limits.MaxCompletionTokens == 0 {
		return nil, errors.New("maximum completion tokens must be positive")
	}
	if limits.CompletionWeight == 0 {
		return nil, errors.New("completion weight must be positive")
	}
	if limits.MaxRequestUnits == 0 {
		return nil, errors.New("maximum request units must be positive")
	}
	hi, maximumCompletionUnits := bits.Mul64(limits.CompletionWeight, limits.MaxCompletionTokens)
	if hi != 0 {
		return nil, errors.New("configured completion reservation can overflow")
	}
	if _, carry := bits.Add64(limits.MaxBodyBytes, maximumCompletionUnits, 0); carry != 0 {
		return nil, errors.New("configured maximum reservation can overflow")
	}
	minimumBodyBytes := minimumJSONStringBytes(publicModel) + uint64(len(
		`{"model":,"messages":[{"role":"user","content":""}],"max_completion_tokens":1}`,
	))
	if minimumBodyBytes > limits.MaxBodyBytes {
		return nil, errors.New("maximum body bytes cannot fit the smallest valid request")
	}
	if _, reason := reservationUnits(
		minimumBodyBytes,
		1,
		limits.CompletionWeight,
		limits.MaxRequestUnits,
	); reason != 0 {
		return nil, errors.New("maximum request units cannot fit the smallest valid request")
	}

	return &Parser{publicModel: publicModel, limits: limits}, nil
}

func minimumJSONStringBytes(value string) uint64 {
	length := uint64(2) // opening and closing quotation marks
	for _, runeValue := range value {
		switch runeValue {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			length += 2
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x0b, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
			0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
			0x1d, 0x1e, 0x1f:
			length += 6
		default:
			length += uint64(utf8.RuneLen(runeValue))
		}
	}
	return length
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

// Role is a closed representation of the text-message roles in the v0 subset.
type Role uint8

const (
	RoleDeveloper Role = iota + 1
	RoleSystem
	RoleUser
	RoleAssistant
)

func (r Role) String() string {
	switch r {
	case RoleDeveloper:
		return "developer"
	case RoleSystem:
		return "system"
	case RoleUser:
		return "user"
	case RoleAssistant:
		return "assistant"
	default:
		return "unknown"
	}
}

// Message is one validated text-only message.
type Message struct {
	Role    Role
	Content string
}

// RequestMode is the validated response mode selected by a request.
type RequestMode uint8

const (
	RequestModeNonStreaming RequestMode = iota + 1
	RequestModeStreaming
)

func (m RequestMode) String() string {
	switch m {
	case RequestModeNonStreaming:
		return "non_streaming"
	case RequestModeStreaming:
		return "streaming"
	default:
		return "unknown"
	}
}

// Request is one validated request. Body is the exact bounded JSON
// representation received from the client and is retained only for dispatch.
type Request struct {
	model               string
	messages            []Message
	mode                RequestMode
	maxCompletionTokens uint64
	bodyBytes           uint64
	messageTextBytes    uint64
	reservationUnits    uint64
	body                []byte
}

func (r Request) Model() string { return r.model }

// Messages returns a copy so callers cannot mutate the validated request.
func (r Request) Messages() []Message { return slices.Clone(r.messages) }

func (r Request) Mode() RequestMode { return r.mode }

func (r Request) MaxCompletionTokens() uint64 { return r.maxCompletionTokens }

func (r Request) BodyBytes() uint64 { return r.bodyBytes }

func (r Request) MessageTextBytes() uint64 { return r.messageTextBytes }

func (r Request) ReservationUnits() uint64 { return r.reservationUnits }

// BodyReader returns a new read-only cursor over the validated body. The
// underlying bytes remain owned by Request and are not exposed for mutation.
func (r Request) BodyReader() io.Reader {
	return &requestBodyReader{body: r.body}
}

// BodyCopy returns an independent copy for tests or integrations that require
// byte-slice ownership.
func (r Request) BodyCopy() []byte {
	return bytes.Clone(r.body)
}

// requestBodyReader intentionally implements only io.Reader. In particular,
// it does not expose bytes.Reader's WriteTo fast path, which would hand the
// privately owned backing slice to a caller-supplied writer.
type requestBodyReader struct {
	body   []byte
	offset int
}

func (r *requestBodyReader) Read(destination []byte) (int, error) {
	if r.offset >= len(r.body) {
		return 0, io.EOF
	}
	n := copy(destination, r.body[r.offset:])
	r.offset += n
	return n, nil
}

// Parse reads at most MaxBodyBytes+1 bytes and validates the v0 request
// subset. Cancellation is checked around reads; an HTTP handler must
// still close a blocked request body when its context is canceled.
func (p *Parser) Parse(ctx context.Context, r io.Reader) (Request, error) {
	if ctx == nil {
		return Request{}, errors.New("context must not be nil")
	}
	if r == nil {
		return Request{}, errors.New("reader must not be nil")
	}

	body, readErr := io.ReadAll(io.LimitReader(
		contextReader{ctx: ctx, reader: r},
		int64(p.limits.MaxBodyBytes+1),
	))
	if uint64(len(body)) > p.limits.MaxBodyBytes {
		return Request{}, parseError(ErrorClassRequestTooLarge, ReasonBodyBytesLimit)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}
	if readErr != nil {
		return Request{}, parseError(ErrorClassBodyReadFailure, ReasonBodyReadFailure)
	}
	compactedBody := make([]byte, len(body))
	copy(compactedBody, body)
	body = compactedBody
	if !utf8.Valid(body) {
		return Request{}, parseError(ErrorClassInvalidRequest, ReasonInvalidUTF8)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}
	if !validUnicodeEscapes(ctx, body) {
		if err := ctx.Err(); err != nil {
			return Request{}, err
		}
		return Request{}, parseError(ErrorClassInvalidRequest, ReasonInvalidUnicodeEscape)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}
	if reason := validateJSONDocument(ctx, body, maxJSONDepth); reason != 0 {
		if err := ctx.Err(); err != nil {
			return Request{}, err
		}
		return Request{}, parseError(ErrorClassInvalidRequest, reason)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}

	request, reason := p.decodeRequest(ctx, body)
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}
	if reason != 0 {
		class := ErrorClassInvalidRequest
		if reason == ReasonMessageCountLimit || reason == ReasonMessageBytesLimit {
			class = ErrorClassRequestTooLarge
		}
		return Request{}, parseError(class, reason)
	}

	request.body = body
	request.bodyBytes = uint64(len(body))
	request.reservationUnits, reason = reservationUnits(
		request.bodyBytes,
		request.maxCompletionTokens,
		p.limits.CompletionWeight,
		p.limits.MaxRequestUnits,
	)
	if reason != 0 {
		return Request{}, parseError(ErrorClassInvalidRequest, reason)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}

	return request, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := r.reader.Read(p)
	if err == nil {
		if contextErr := r.ctx.Err(); contextErr != nil {
			return n, contextErr
		}
	}
	return n, err
}

func reservationUnits(bodyBytes, completionTokens, completionWeight, maximum uint64) (uint64, Reason) {
	hi, completionUnits := bits.Mul64(completionWeight, completionTokens)
	if hi != 0 {
		return 0, ReasonReservationOverflow
	}
	units, carry := bits.Add64(bodyBytes, completionUnits, 0)
	if carry != 0 || units == 0 {
		return 0, ReasonReservationOverflow
	}
	if units > maximum {
		return 0, ReasonReservationLimit
	}
	return units, 0
}
