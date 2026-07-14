package app

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

type policyJSONFixture struct {
	Count   uint64             `json:"count"`
	Name    string             `json:"name"`
	Nested  policyJSONNested   `json:"nested"`
	Items   []policyJSONNested `json:"items"`
	Padding string             `json:"padding"`
}

type policyJSONNested struct {
	Enabled bool `json:"enabled"`
}

func TestDecodePolicyJSONAcceptsStrictObject(t *testing.T) {
	data := []byte(`{"count":7,"name":"safe","nested":{"enabled":true},"items":[{"enabled":false}]}`)
	var destination policyJSONFixture
	if err := decodePolicyJSON(data, &destination); err != nil {
		t.Fatalf("decodePolicyJSON() error = %v", err)
	}
	if destination.Count != 7 || destination.Name != "safe" || !destination.Nested.Enabled {
		t.Fatalf("destination = %+v", destination)
	}
	if len(destination.Items) != 1 || destination.Items[0].Enabled {
		t.Fatalf("destination.Items = %+v", destination.Items)
	}
}

func TestDecodePolicyJSONAcceptsMaximumSize(t *testing.T) {
	prefix := []byte(`{"padding":"`)
	suffix := []byte(`"}`)
	data := make([]byte, 0, maxPolicyBytes)
	data = append(data, prefix...)
	data = append(data, bytes.Repeat([]byte{'a'}, maxPolicyBytes-len(prefix)-len(suffix))...)
	data = append(data, suffix...)

	var destination policyJSONFixture
	if err := decodePolicyJSON(data, &destination); err != nil {
		t.Fatalf("decodePolicyJSON() error = %v", err)
	}
	if len(destination.Padding) != maxPolicyBytes-len(prefix)-len(suffix) {
		t.Fatalf("padding length = %d", len(destination.Padding))
	}
}

func TestDecodePolicyJSONRejectsInvalidDocuments(t *testing.T) {
	tooLarge := append(bytes.Repeat([]byte{' '}, maxPolicyBytes), ' ')
	invalidUTF8 := append([]byte(`{"name":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	deepAllowed := nestedPolicyJSON(maxPolicyDepth - 3)
	deepRejected := nestedPolicyJSON(maxPolicyDepth - 2)
	var typedNil *policyJSONFixture

	tests := []struct {
		name        string
		data        []byte
		destination any
	}{
		{name: "nil destination", data: []byte(`{}`), destination: nil},
		{name: "typed nil destination", data: []byte(`{}`), destination: typedNil},
		{name: "empty", data: nil, destination: &policyJSONFixture{}},
		{name: "too large", data: tooLarge, destination: &policyJSONFixture{}},
		{name: "whitespace", data: []byte(" \n\t"), destination: &policyJSONFixture{}},
		{name: "invalid utf8", data: invalidUTF8, destination: &policyJSONFixture{}},
		{name: "top level null", data: []byte(`null`), destination: &policyJSONFixture{}},
		{name: "top level array", data: []byte(`[]`), destination: &policyJSONFixture{}},
		{name: "top level scalar", data: []byte(`7`), destination: &policyJSONFixture{}},
		{name: "duplicate key", data: []byte(`{"count":1,"count":2}`), destination: &policyJSONFixture{}},
		{name: "escaped duplicate key", data: []byte(`{"name":"first","na\u006de":"second"}`), destination: &policyJSONFixture{}},
		{name: "nested duplicate key", data: []byte(`{"nested":{"enabled":true,"enabled":false}}`), destination: &policyJSONFixture{}},
		{name: "array nested duplicate key", data: []byte(`{"items":[{"enabled":true,"enabled":false}]}`), destination: &policyJSONFixture{}},
		{name: "unknown field", data: []byte(`{"unknown_canary_secret":"value_canary_secret"}`), destination: &policyJSONFixture{}},
		{name: "wrong case field", data: []byte(`{"Count":1}`), destination: &policyJSONFixture{}},
		{name: "case-folded semantic duplicate", data: []byte(`{"count":1,"Count":2}`), destination: &policyJSONFixture{}},
		{name: "wrong case nested field", data: []byte(`{"nested":{"Enabled":true}}`), destination: &policyJSONFixture{}},
		{name: "trailing object", data: []byte(`{} {}`), destination: &policyJSONFixture{}},
		{name: "trailing scalar", data: []byte(`{} 1`), destination: &policyJSONFixture{}},
		{name: "high surrogate alone", data: []byte(`{"name":"\uD800"}`), destination: &policyJSONFixture{}},
		{name: "low surrogate alone", data: []byte(`{"name":"\uDC00"}`), destination: &policyJSONFixture{}},
		{name: "high surrogate followed by scalar", data: []byte(`{"name":"\uD800a"}`), destination: &policyJSONFixture{}},
		{name: "high surrogate followed by high", data: []byte(`{"name":"\uD800\uD800"}`), destination: &policyJSONFixture{}},
		{name: "unpaired surrogate in key", data: []byte(`{"\uD800":"value"}`), destination: &policyJSONFixture{}},
		{name: "malformed unicode escape", data: []byte(`{"name":"\uZZZZ"}`), destination: &policyJSONFixture{}},
		{name: "depth beyond limit", data: deepRejected, destination: &map[string]any{}},
		{name: "fractional integer", data: []byte(`{"count":1.5}`), destination: &policyJSONFixture{}},
		{name: "integer overflow", data: []byte(`{"count":18446744073709551616}`), destination: &policyJSONFixture{}},
		{name: "malformed", data: []byte(`{"count":`), destination: &policyJSONFixture{}},
		{name: "malformed canary", data: []byte(`{"document_canary_secret":`), destination: &policyJSONFixture{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := decodePolicyJSON(test.data, test.destination)
			if !errors.Is(err, errPolicyInvalid) {
				t.Fatalf("decodePolicyJSON() error = %v", err)
			}
			if err.Error() != errPolicyInvalid.Error() {
				t.Fatalf("error text = %q", err.Error())
			}
			for _, canary := range []string{"unknown_canary_secret", "value_canary_secret", "document_canary_secret"} {
				if canary != "" && strings.Contains(err.Error(), canary) {
					t.Fatalf("error contains input canary")
				}
			}
		})
	}

	var accepted map[string]any
	if err := decodePolicyJSON(deepAllowed, &accepted); err != nil {
		t.Fatalf("depth at limit error = %v", err)
	}
}

func TestDecodePolicyJSONAcceptsUnicodeSurrogatePairAndEscapedSlash(t *testing.T) {
	data := []byte(`{"name":"\uD83D\uDE80 \\uD800"}`)
	var destination policyJSONFixture
	if err := decodePolicyJSON(data, &destination); err != nil {
		t.Fatalf("decodePolicyJSON() error = %v", err)
	}
	if destination.Name != "🚀 \\uD800" {
		t.Fatalf("name = %q", destination.Name)
	}
}

func nestedPolicyJSON(arrayCount int) []byte {
	var builder strings.Builder
	builder.WriteString(`{"items":`)
	for range arrayCount {
		builder.WriteByte('[')
	}
	builder.WriteString(`{"leaf":true}`)
	for range arrayCount {
		builder.WriteByte(']')
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}
