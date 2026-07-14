package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

const maxPolicyBytes = 1 << 20

const maxPolicyDepth = 32

var errPolicyInvalid = errors.New("policy is invalid")

func decodePolicyJSON(data []byte, destination any) error {
	if destination == nil || len(data) == 0 || len(data) > maxPolicyBytes {
		return errPolicyInvalid
	}
	if !utf8.Valid(data) || !validPolicyUnicodeEscapes(data) {
		return errPolicyInvalid
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') || !scanPolicyObject(decoder, 1) {
		return errPolicyInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errPolicyInvalid
	}

	var shape any
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&shape); err != nil || !exactPolicyJSONFields(shape, reflect.TypeOf(destination)) {
		return errPolicyInvalid
	}

	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return errPolicyInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errPolicyInvalid
	}
	return nil
}

func exactPolicyJSONFields(value any, destinationType reflect.Type) bool {
	if destinationType == nil {
		return false
	}
	for destinationType.Kind() == reflect.Pointer {
		destinationType = destinationType.Elem()
	}
	if value == nil || destinationType.Kind() == reflect.Interface {
		return true
	}

	switch destinationType.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return true // The typed decoder reports the wrong JSON kind.
		}
		fields := make(map[string]reflect.Type, destinationType.NumField())
		for index := 0; index < destinationType.NumField(); index++ {
			field := destinationType.Field(index)
			if !field.IsExported() || field.Anonymous {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			fields[name] = field.Type
		}
		for name, member := range object {
			fieldType, exists := fields[name]
			if !exists || !exactPolicyJSONFields(member, fieldType) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if destinationType.Kind() == reflect.Slice && destinationType.Elem().Kind() == reflect.Uint8 {
			return true
		}
		array, ok := value.([]any)
		if !ok {
			return true // The typed decoder reports the wrong JSON kind.
		}
		for _, member := range array {
			if !exactPolicyJSONFields(member, destinationType.Elem()) {
				return false
			}
		}
		return true
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok || destinationType.Key().Kind() != reflect.String {
			return true
		}
		for _, member := range object {
			if !exactPolicyJSONFields(member, destinationType.Elem()) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func scanPolicyValue(decoder *json.Decoder, depth int) bool {
	if depth > maxPolicyDepth {
		return false
	}

	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return true
	}

	switch delimiter {
	case '{':
		return scanPolicyObject(decoder, depth)
	case '[':
		return scanPolicyArray(decoder, depth)
	default:
		return false
	}
}

func scanPolicyObject(decoder *json.Decoder, depth int) bool {
	if depth > maxPolicyDepth {
		return false
	}

	keys := make(map[string]struct{})
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := keyToken.(string)
		if !ok {
			return false
		}
		if _, duplicate := keys[key]; duplicate {
			return false
		}
		keys[key] = struct{}{}
		if !scanPolicyValue(decoder, depth+1) {
			return false
		}
	}
	closing, err := decoder.Token()
	return err == nil && closing == json.Delim('}')
}

func scanPolicyArray(decoder *json.Decoder, depth int) bool {
	if depth > maxPolicyDepth {
		return false
	}

	for decoder.More() {
		if !scanPolicyValue(decoder, depth+1) {
			return false
		}
	}
	closing, err := decoder.Token()
	return err == nil && closing == json.Delim(']')
}

func validPolicyUnicodeEscapes(data []byte) bool {
	inString := false
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(data) {
				return true
			}
			if data[index] != 'u' {
				continue
			}

			value, ok := decodePolicyHexQuad(data, index+1)
			if !ok {
				return true
			}
			index += 4
			switch {
			case value >= 0xdc00 && value <= 0xdfff:
				return false
			case value >= 0xd800 && value <= 0xdbff:
				if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
					return false
				}
				low, lowOK := decodePolicyHexQuad(data, index+3)
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 6
			}
		}
	}
	return true
}

func decodePolicyHexQuad(data []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(data) {
		return 0, false
	}

	var value uint16
	for _, digit := range data[start : start+4] {
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
