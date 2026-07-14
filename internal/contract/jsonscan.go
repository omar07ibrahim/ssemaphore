package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
)

func validateJSONDocument(ctx context.Context, body []byte, maximumDepth int) Reason {
	decoder := newContextDecoder(ctx, body)

	if reason := scanJSONValue(decoder, 1, maximumDepth); reason != 0 {
		return reason
	}

	_, err := decoder.Token()
	switch err {
	case io.EOF:
		return 0
	case nil:
		return ReasonTrailingJSON
	default:
		return ReasonMalformedJSON
	}
}

func scanJSONValue(decoder *contextDecoder, depth, maximumDepth int) Reason {
	if depth > maximumDepth {
		return ReasonNestingLimit
	}

	token, err := decoder.Token()
	if err != nil {
		return ReasonMalformedJSON
	}

	delimiter, composite := token.(json.Delim)
	if !composite {
		return 0
	}

	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return ReasonMalformedJSON
			}
			key, ok := keyToken.(string)
			if !ok {
				return ReasonMalformedJSON
			}
			if _, duplicate := keys[key]; duplicate {
				return ReasonDuplicateKey
			}
			keys[key] = struct{}{}
			if reason := scanJSONValue(decoder, depth+1, maximumDepth); reason != 0 {
				return reason
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim('}') {
			return ReasonMalformedJSON
		}
		return 0
	case '[':
		for decoder.More() {
			if reason := scanJSONValue(decoder, depth+1, maximumDepth); reason != 0 {
				return reason
			}
		}
		closing, closeErr := decoder.Token()
		if closeErr != nil || closing != json.Delim(']') {
			return ReasonMalformedJSON
		}
		return 0
	default:
		return ReasonMalformedJSON
	}
}

func validUnicodeEscapes(ctx context.Context, body []byte) bool {
	inString := false
	for index := 0; index < len(body); index++ {
		if index&1023 == 0 && ctx.Err() != nil {
			return false
		}
		switch body[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(body) {
				return true
			}
			if body[index] != 'u' {
				continue
			}

			value, ok := decodeHexQuad(body, index+1)
			if !ok {
				return true
			}
			index += 4
			switch {
			case value >= 0xdc00 && value <= 0xdfff:
				return false
			case value >= 0xd800 && value <= 0xdbff:
				if index+6 >= len(body) || body[index+1] != '\\' || body[index+2] != 'u' {
					return false
				}
				low, lowOK := decodeHexQuad(body, index+3)
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 6
			}
		}
	}
	return true
}

type contextDecoder struct {
	ctx     context.Context
	decoder *json.Decoder
}

func newContextDecoder(ctx context.Context, body []byte) *contextDecoder {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	return &contextDecoder{ctx: ctx, decoder: decoder}
}

func (d *contextDecoder) Token() (json.Token, error) {
	if err := d.ctx.Err(); err != nil {
		return nil, err
	}
	token, err := d.decoder.Token()
	if err == nil {
		if contextErr := d.ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
	}
	return token, err
}

func (d *contextDecoder) More() bool {
	return d.ctx.Err() == nil && d.decoder.More()
}

func decodeHexQuad(body []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(body) {
		return 0, false
	}

	var value uint16
	for _, digit := range body[start : start+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
