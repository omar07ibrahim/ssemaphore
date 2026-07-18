package contract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/bits"
	"unicode/utf8"
)

const (
	// AbsoluteMaxSSETotalBytes keeps every configured stream byte counter within
	// a signed 32-bit integer while the decoder itself accounts in uint64.
	AbsoluteMaxSSETotalBytes = 1 << 30

	// AbsoluteMaxSSEEventBytes bounds the only response-sized allocation owned
	// by the decoder. It matches the non-streaming response hard ceiling.
	AbsoluteMaxSSEEventBytes = AbsoluteMaxResponseBodyBytes

	// AbsoluteMaxSSEEvents prevents an operator configuration from turning one
	// stream into an effectively unbounded sequence of tiny events.
	AbsoluteMaxSSEEvents = 1 << 20

	chatCompletionChunkObject = "chat.completion.chunk"
	sseReadBufferBytes        = 4096

	minimumSSEChunkEvent = "data: {\"object\":\"chat.completion.chunk\"}\n\n"
	minimumSSEDoneEvent  = "data: [DONE]\n\n"
)

// SSELimits bounds all wire input and event-owned allocation accepted by one
// decoder. The terminal [DONE] event counts toward both bytes and events.
type SSELimits struct {
	MaxTotalBytes uint64
	MaxEventBytes uint64
	MaxEvents     uint64
}

// SSEEventKind is the closed semantic kind of one validated event.
type SSEEventKind uint8

const (
	SSEEventKindChunk SSEEventKind = iota + 1
	SSEEventKindDone
)

func (k SSEEventKind) String() string {
	switch k {
	case SSEEventKindChunk:
		return "chunk"
	case SSEEventKindDone:
		return "done"
	default:
		return "unknown"
	}
}

// SSEReason is a closed, content-free explanation of an upstream event-stream
// validation failure.
type SSEReason uint8

const (
	SSEReasonTotalBytesLimit SSEReason = iota + 1
	SSEReasonEventBytesLimit
	SSEReasonEventCountLimit
	SSEReasonBodyReadFailure
	SSEReasonInvalidUTF8
	SSEReasonInvalidLineEnding
	SSEReasonEmptyEvent
	SSEReasonUnsupportedField
	SSEReasonMultipleFields
	SSEReasonEmptyData
	SSEReasonInvalidUnicodeEscape
	SSEReasonMalformedJSON
	SSEReasonTrailingJSON
	SSEReasonDuplicateKey
	SSEReasonNestingLimit
	SSEReasonRootNotObject
	SSEReasonMissingObject
	SSEReasonWrongObjectType
	SSEReasonUnsupportedObject
	SSEReasonDoneBeforeChunk
	SSEReasonTruncatedEvent
	SSEReasonMissingDone
	SSEReasonTrailingData
)

// SSEError never retains a reader error, field, payload, or wire bytes.
type SSEError struct {
	reason SSEReason
}

func (e *SSEError) Error() string {
	switch e.reason {
	case SSEReasonTotalBytesLimit:
		return "upstream SSE stream exceeds its configured byte limit"
	case SSEReasonEventBytesLimit:
		return "upstream SSE event exceeds its configured byte limit"
	case SSEReasonEventCountLimit:
		return "upstream SSE stream exceeds its configured event limit"
	case SSEReasonBodyReadFailure:
		return "upstream SSE stream could not be read"
	case SSEReasonInvalidUTF8:
		return "upstream SSE event is not valid UTF-8"
	case SSEReasonInvalidLineEnding:
		return "upstream SSE event has an unsupported line ending"
	case SSEReasonEmptyEvent:
		return "upstream SSE stream contains an empty event"
	case SSEReasonUnsupportedField:
		return "upstream SSE event contains an unsupported field"
	case SSEReasonMultipleFields:
		return "upstream SSE event contains more than one field"
	case SSEReasonEmptyData:
		return "upstream SSE event contains empty data"
	case SSEReasonInvalidUnicodeEscape:
		return "upstream SSE event contains an unpaired Unicode surrogate"
	case SSEReasonMalformedJSON:
		return "upstream SSE event data is not one valid JSON value"
	case SSEReasonTrailingJSON:
		return "upstream SSE event data contains a trailing JSON value"
	case SSEReasonDuplicateKey:
		return "upstream SSE event data contains a duplicate object key"
	case SSEReasonNestingLimit:
		return "upstream SSE event data exceeds the JSON nesting limit"
	case SSEReasonRootNotObject:
		return "upstream SSE event data root must be an object"
	case SSEReasonMissingObject:
		return "upstream SSE event data is missing its object field"
	case SSEReasonWrongObjectType:
		return "upstream SSE event object field has the wrong JSON type"
	case SSEReasonUnsupportedObject:
		return "upstream SSE event object field has an unsupported value"
	case SSEReasonDoneBeforeChunk:
		return "upstream SSE stream ended before a completion chunk"
	case SSEReasonTruncatedEvent:
		return "upstream SSE stream ended inside an event"
	case SSEReasonMissingDone:
		return "upstream SSE stream ended before its terminal marker"
	case SSEReasonTrailingData:
		return "upstream SSE stream contains data after its terminal marker"
	default:
		return "upstream SSE validation failed"
	}
}

func (e *SSEError) Reason() SSEReason { return e.reason }

func sseError(reason SSEReason) error { return &SSEError{reason: reason} }

// ValidatedSSEEvent owns one exact, bounded wire event. Its backing bytes are
// private and cannot be mutated by a caller.
type ValidatedSSEEvent struct {
	kind SSEEventKind
	body []byte
}

func (e ValidatedSSEEvent) Kind() SSEEventKind { return e.kind }

func (e ValidatedSSEEvent) BodyBytes() uint64 { return uint64(len(e.body)) }

// BodyReader returns a new read-only cursor over the exact event bytes.
func (e ValidatedSSEEvent) BodyReader() io.Reader {
	return &sseEventBodyReader{body: e.body}
}

// BodyCopy returns an independent copy of the exact event bytes.
func (e ValidatedSSEEvent) BodyCopy() []byte { return bytes.Clone(e.body) }

type sseEventBodyReader struct {
	body   []byte
	offset int
}

func (r *sseEventBodyReader) Read(destination []byte) (int, error) {
	if r.offset >= len(r.body) {
		return 0, io.EOF
	}
	n := copy(destination, r.body[r.offset:])
	r.offset += n
	return n, nil
}

type sseDecoderState uint8

const (
	sseDecoderOpen sseDecoderState = iota + 1
	sseDecoderDoneReturned
	sseDecoderVerified
	sseDecoderFailed
)

var errSSEVerifyEOFRequired = errors.New("SSE terminal event requires EOF verification")

// SSEDecoder validates one strict event stream. It is stateful, belongs to one
// reader, and must not be used concurrently. A caller must invoke VerifyEOF
// after receiving SSEEventKindDone and before treating the stream as complete.
type SSEDecoder struct {
	reader *bufio.Reader
	limits SSELimits

	state      sseDecoderState
	totalBytes uint64
	events     uint64
	chunks     uint64
	failure    error
}

// ValidateSSELimits validates reusable operator policy without reading input.
func ValidateSSELimits(limits SSELimits) error {
	if limits.MaxTotalBytes == 0 {
		return errors.New("maximum SSE total bytes must be positive")
	}
	if limits.MaxTotalBytes > AbsoluteMaxSSETotalBytes || limits.MaxTotalBytes >= uint64(maxInt()) {
		return errors.New("maximum SSE total bytes exceeds its hard safety limit")
	}
	if limits.MaxEventBytes == 0 {
		return errors.New("maximum SSE event bytes must be positive")
	}
	if limits.MaxEventBytes > AbsoluteMaxSSEEventBytes || limits.MaxEventBytes >= uint64(maxInt()) {
		return errors.New("maximum SSE event bytes exceeds its hard safety limit")
	}
	if limits.MaxEventBytes > limits.MaxTotalBytes {
		return errors.New("maximum SSE event bytes exceeds the total byte limit")
	}
	if limits.MaxEvents < 2 {
		return errors.New("maximum SSE events must fit a chunk and terminal event")
	}
	if limits.MaxEvents > AbsoluteMaxSSEEvents || limits.MaxEvents > uint64(maxInt()) {
		return errors.New("maximum SSE events exceeds its hard safety limit")
	}
	minimumEventBytes := uint64(max(len(minimumSSEChunkEvent), len(minimumSSEDoneEvent)))
	if limits.MaxEventBytes < minimumEventBytes {
		return errors.New("maximum SSE event bytes cannot fit the smallest valid event")
	}
	minimumTotalBytes := uint64(len(minimumSSEChunkEvent) + len(minimumSSEDoneEvent))
	if limits.MaxTotalBytes < minimumTotalBytes {
		return errors.New("maximum SSE total bytes cannot fit the smallest valid stream")
	}
	return nil
}

// NewSSEDecoder binds one validated limit set to one reader without reading
// input. The same limits may be preflighted with ValidateSSELimits.
func NewSSEDecoder(reader io.Reader, limits SSELimits) (*SSEDecoder, error) {
	if err := ValidateSSELimits(limits); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("SSE reader must not be nil")
	}

	wireReader := io.LimitReader(reader, int64(limits.MaxTotalBytes+1))
	readBufferBytes := min(
		sseReadBufferBytes,
		int(limits.MaxEventBytes+1),
		int(limits.MaxTotalBytes+1),
	)
	return &SSEDecoder{
		reader: bufio.NewReaderSize(wireReader, readBufferBytes),
		limits: limits,
		state:  sseDecoderOpen,
	}, nil
}

// Next reads and validates one complete event. The strict subset contains one
// data field followed immediately by one empty delimiter line. Both LF and
// CRLF are accepted and retained exactly; comments and multi-field events are
// rejected. A blocked read still requires the stream owner to close reader
// when ctx is canceled.
func (d *SSEDecoder) Next(ctx context.Context) (ValidatedSSEEvent, error) {
	if ctx == nil {
		return ValidatedSSEEvent{}, errors.New("context must not be nil")
	}
	switch d.state {
	case sseDecoderDoneReturned:
		return ValidatedSSEEvent{}, errSSEVerifyEOFRequired
	case sseDecoderVerified:
		return ValidatedSSEEvent{}, io.EOF
	case sseDecoderFailed:
		return ValidatedSSEEvent{}, d.failure
	}
	if err := ctx.Err(); err != nil {
		return ValidatedSSEEvent{}, d.fail(err)
	}
	if d.events >= d.limits.MaxEvents {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonEventCountLimit))
	}

	raw := make([]byte, 0, min(int(d.limits.MaxEventBytes), sseReadBufferBytes))
	var eventBytes uint64
	first, err := d.readSSELine(ctx, &raw, &eventBytes)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = sseError(SSEReasonMissingDone)
		}
		return ValidatedSSEEvent{}, d.fail(err)
	}
	if !utf8.Valid(raw) {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonInvalidUTF8))
	}
	if first.invalidEnding {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonInvalidLineEnding))
	}
	if len(first.content) == 0 {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonEmptyEvent))
	}

	payload, validField := sseDataValue(first.content)
	if !validField {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonUnsupportedField))
	}

	second, err := d.readSSELine(ctx, &raw, &eventBytes)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = sseError(SSEReasonTruncatedEvent)
		}
		return ValidatedSSEEvent{}, d.fail(err)
	}
	if !utf8.Valid(raw) {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonInvalidUTF8))
	}
	if second.invalidEnding {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonInvalidLineEnding))
	}
	if len(second.content) != 0 {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonMultipleFields))
	}
	if len(payload) == 0 {
		return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonEmptyData))
	}

	d.events++
	body := make([]byte, len(raw))
	copy(body, raw)
	if bytes.Equal(payload, []byte("[DONE]")) {
		if d.chunks == 0 {
			return ValidatedSSEEvent{}, d.fail(sseError(SSEReasonDoneBeforeChunk))
		}
		d.state = sseDecoderDoneReturned
		return ValidatedSSEEvent{kind: SSEEventKindDone, body: body}, nil
	}
	if reason := validateSSEChunk(ctx, payload); reason != 0 {
		if err := ctx.Err(); err != nil {
			return ValidatedSSEEvent{}, d.fail(err)
		}
		return ValidatedSSEEvent{}, d.fail(sseError(reason))
	}
	if err := ctx.Err(); err != nil {
		return ValidatedSSEEvent{}, d.fail(err)
	}
	d.chunks++
	return ValidatedSSEEvent{kind: SSEEventKindChunk, body: body}, nil
}

// VerifyEOF proves that no wire bytes follow the returned terminal event. It
// is idempotent after success. Any trailing byte, including whitespace or an
// empty event, invalidates the stream.
func (d *SSEDecoder) VerifyEOF(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	switch d.state {
	case sseDecoderVerified:
		return nil
	case sseDecoderFailed:
		return d.failure
	case sseDecoderOpen:
		return d.fail(sseError(SSEReasonMissingDone))
	}
	if err := ctx.Err(); err != nil {
		return d.fail(err)
	}

	_, readErr := d.reader.ReadByte()
	if readErr == nil {
		if err := d.accountBytes(1, nil); err != nil {
			return d.fail(err)
		}
		if err := ctx.Err(); err != nil {
			return d.fail(err)
		}
		return d.fail(sseError(SSEReasonTrailingData))
	}
	if err := ctx.Err(); err != nil {
		return d.fail(err)
	}
	if !errors.Is(readErr, io.EOF) {
		return d.fail(sseError(SSEReasonBodyReadFailure))
	}
	d.state = sseDecoderVerified
	return nil
}

type sseLine struct {
	content       []byte
	invalidEnding bool
}

func (d *SSEDecoder) readSSELine(
	ctx context.Context,
	raw *[]byte,
	eventBytes *uint64,
) (sseLine, error) {
	start := len(*raw)
	for {
		if err := ctx.Err(); err != nil {
			return sseLine{}, err
		}
		fragment, readErr := d.reader.ReadSlice('\n')
		if err := d.accountBytes(uint64(len(fragment)), eventBytes); err != nil {
			return sseLine{}, err
		}
		if err := ctx.Err(); err != nil {
			return sseLine{}, err
		}
		*raw = appendSSEBytes(*raw, fragment, int(d.limits.MaxEventBytes))

		switch {
		case readErr == nil:
			end := len(*raw) - 1
			if end > start && (*raw)[end-1] == '\r' {
				end--
			}
			content := (*raw)[start:end]
			return sseLine{
				content:       content,
				invalidEnding: bytes.IndexByte(content, '\r') >= 0,
			}, nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF):
			if len(*raw) == start {
				return sseLine{}, io.EOF
			}
			partial := (*raw)[start:]
			if bytes.IndexByte(partial, '\r') >= 0 {
				return sseLine{}, sseError(SSEReasonInvalidLineEnding)
			}
			return sseLine{}, sseError(SSEReasonTruncatedEvent)
		default:
			return sseLine{}, sseError(SSEReasonBodyReadFailure)
		}
	}
}

// appendSSEBytes grows only up to the validated event ceiling. Go's ordinary
// append growth may otherwise reserve capacity above the configured bound even
// though the logical event length remains within it.
func appendSSEBytes(destination, source []byte, maximum int) []byte {
	needed := len(destination) + len(source)
	if needed > cap(destination) {
		capacity := max(cap(destination)*2, needed)
		capacity = min(capacity, maximum)
		expanded := make([]byte, len(destination), capacity)
		copy(expanded, destination)
		destination = expanded
	}
	return append(destination, source...)
}

func (d *SSEDecoder) accountBytes(count uint64, eventBytes *uint64) error {
	total, carry := bits.Add64(d.totalBytes, count, 0)
	if carry != 0 || total > d.limits.MaxTotalBytes {
		return sseError(SSEReasonTotalBytesLimit)
	}
	if eventBytes != nil {
		event, eventCarry := bits.Add64(*eventBytes, count, 0)
		if eventCarry != 0 || event > d.limits.MaxEventBytes {
			return sseError(SSEReasonEventBytesLimit)
		}
		*eventBytes = event
	}
	d.totalBytes = total
	return nil
}

func (d *SSEDecoder) fail(err error) error {
	d.state = sseDecoderFailed
	d.failure = err
	return err
}

func sseDataValue(line []byte) ([]byte, bool) {
	field, value, found := bytes.Cut(line, []byte{':'})
	if !found || !bytes.Equal(field, []byte("data")) {
		return nil, false
	}
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	return value, true
}

func validateSSEChunk(ctx context.Context, body []byte) SSEReason {
	if !utf8.Valid(body) {
		return SSEReasonInvalidUTF8
	}
	if !validUnicodeEscapes(ctx, body) {
		if ctx.Err() != nil {
			return 0
		}
		return SSEReasonInvalidUnicodeEscape
	}
	if reason := validateJSONDocument(ctx, body, maxJSONDepth); reason != 0 {
		if ctx.Err() != nil {
			return 0
		}
		return sseReasonFromJSON(reason)
	}
	return validateSSEChunkObject(ctx, body)
}

func sseReasonFromJSON(reason Reason) SSEReason {
	switch reason {
	case ReasonTrailingJSON:
		return SSEReasonTrailingJSON
	case ReasonDuplicateKey:
		return SSEReasonDuplicateKey
	case ReasonNestingLimit:
		return SSEReasonNestingLimit
	default:
		return SSEReasonMalformedJSON
	}
}

func validateSSEChunkObject(ctx context.Context, body []byte) SSEReason {
	decoder := newContextDecoder(ctx, body)
	opening, err := decoder.Token()
	if err != nil {
		return SSEReasonMalformedJSON
	}
	if opening != json.Delim('{') {
		return SSEReasonRootNotObject
	}

	objectSet := false
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		if keyErr != nil {
			return SSEReasonMalformedJSON
		}
		key, ok := keyToken.(string)
		if !ok {
			return SSEReasonMalformedJSON
		}
		if key == "object" {
			value, reason := readString(decoder)
			if reason == ReasonWrongType {
				return SSEReasonWrongObjectType
			}
			if reason != 0 {
				return SSEReasonMalformedJSON
			}
			if value != chatCompletionChunkObject {
				return SSEReasonUnsupportedObject
			}
			objectSet = true
			continue
		}
		if reason := scanJSONValue(decoder, 2, maxJSONDepth); reason != 0 {
			return sseReasonFromJSON(reason)
		}
	}

	closing, closeErr := decoder.Token()
	if closeErr != nil || closing != json.Delim('}') {
		return SSEReasonMalformedJSON
	}
	if !objectSet {
		return SSEReasonMissingObject
	}
	return 0
}
