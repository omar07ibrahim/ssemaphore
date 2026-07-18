package contract

import (
	"context"
	"encoding/json"
	"math/bits"
	"strconv"
)

func (p *Parser) decodeRequest(ctx context.Context, body []byte) (Request, Reason) {
	decoder := newContextDecoder(ctx, body)

	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return Request{}, ReasonRootNotObject
	}

	request := Request{mode: RequestModeNonStreaming}
	var modelSet, messagesSet, completionSet bool
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		if keyErr != nil {
			return Request{}, ReasonMalformedJSON
		}
		key, ok := keyToken.(string)
		if !ok {
			return Request{}, ReasonMalformedJSON
		}

		switch key {
		case "model":
			model, reason := readString(decoder)
			if reason != 0 {
				return Request{}, reason
			}
			if model != p.publicModel {
				return Request{}, ReasonUnsupportedValue
			}
			request.model = model
			modelSet = true
		case "messages":
			messages, textBytes, reason := p.readMessages(decoder)
			if reason != 0 {
				return Request{}, reason
			}
			request.messages = messages
			request.messageTextBytes = textBytes
			messagesSet = true
		case "max_completion_tokens":
			value, reason := readUnsignedInteger(decoder)
			if reason != 0 {
				return Request{}, reason
			}
			if value == 0 || value > p.limits.MaxCompletionTokens {
				return Request{}, ReasonCompletionLimit
			}
			request.maxCompletionTokens = value
			completionSet = true
		case "stream":
			stream, reason := readBoolean(decoder)
			if reason != 0 {
				return Request{}, reason
			}
			if stream {
				request.mode = RequestModeStreaming
			}
		case "stream_options":
			return Request{}, ReasonUnsupportedValue
		case "n":
			value, reason := readUnsignedInteger(decoder)
			if reason != 0 {
				return Request{}, reason
			}
			if value != 1 {
				return Request{}, ReasonUnsupportedValue
			}
		default:
			return Request{}, ReasonUnknownField
		}
	}

	closing, closeErr := decoder.Token()
	if closeErr != nil || closing != json.Delim('}') {
		return Request{}, ReasonMalformedJSON
	}
	if !modelSet || !messagesSet || !completionSet {
		return Request{}, ReasonMissingField
	}
	return request, 0
}

func (p *Parser) readMessages(decoder *contextDecoder) ([]Message, uint64, Reason) {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('[') {
		return nil, 0, ReasonWrongType
	}

	messages := make([]Message, 0, min(int(p.limits.MaxMessageCount), 8))
	var textBytes uint64
	for decoder.More() {
		if uint64(len(messages)) >= p.limits.MaxMessageCount {
			return nil, 0, ReasonMessageCountLimit
		}

		message, reason := readMessage(decoder)
		if reason != 0 {
			return nil, 0, reason
		}
		updated, carry := bits.Add64(textBytes, uint64(len(message.Content)), 0)
		if carry != 0 || updated > p.limits.MaxMessageTextBytes {
			return nil, 0, ReasonMessageBytesLimit
		}
		textBytes = updated
		messages = append(messages, message)
	}

	closing, closeErr := decoder.Token()
	if closeErr != nil || closing != json.Delim(']') {
		return nil, 0, ReasonMalformedJSON
	}
	if len(messages) == 0 {
		return nil, 0, ReasonUnsupportedValue
	}
	return messages, textBytes, 0
}

func readMessage(decoder *contextDecoder) (Message, Reason) {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return Message{}, ReasonWrongType
	}

	message := Message{}
	var roleSet, contentSet bool
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		if keyErr != nil {
			return Message{}, ReasonMalformedJSON
		}
		key, ok := keyToken.(string)
		if !ok {
			return Message{}, ReasonMalformedJSON
		}

		switch key {
		case "role":
			roleText, reason := readString(decoder)
			if reason != 0 {
				return Message{}, reason
			}
			role, valid := parseRole(roleText)
			if !valid {
				return Message{}, ReasonUnsupportedValue
			}
			message.Role = role
			roleSet = true
		case "content":
			content, reason := readString(decoder)
			if reason != 0 {
				return Message{}, reason
			}
			message.Content = content
			contentSet = true
		default:
			return Message{}, ReasonUnknownField
		}
	}

	closing, closeErr := decoder.Token()
	if closeErr != nil || closing != json.Delim('}') {
		return Message{}, ReasonMalformedJSON
	}
	if !roleSet || !contentSet {
		return Message{}, ReasonMissingField
	}
	return message, 0
}

func readString(decoder *contextDecoder) (string, Reason) {
	token, err := decoder.Token()
	if err != nil {
		return "", ReasonMalformedJSON
	}
	value, ok := token.(string)
	if !ok {
		return "", ReasonWrongType
	}
	return value, 0
}

func readBoolean(decoder *contextDecoder) (bool, Reason) {
	token, err := decoder.Token()
	if err != nil {
		return false, ReasonMalformedJSON
	}
	value, ok := token.(bool)
	if !ok {
		return false, ReasonWrongType
	}
	return value, 0
}

func readUnsignedInteger(decoder *contextDecoder) (uint64, Reason) {
	token, err := decoder.Token()
	if err != nil {
		return 0, ReasonMalformedJSON
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, ReasonWrongType
	}
	text := number.String()
	if text == "" {
		return 0, ReasonWrongType
	}
	for index := range len(text) {
		if text[index] < '0' || text[index] > '9' {
			return 0, ReasonWrongType
		}
	}
	value, parseErr := strconv.ParseUint(text, 10, 64)
	if parseErr != nil {
		return 0, ReasonWrongType
	}
	return value, 0
}

func parseRole(value string) (Role, bool) {
	switch value {
	case "developer":
		return RoleDeveloper, true
	case "system":
		return RoleSystem, true
	case "user":
		return RoleUser, true
	case "assistant":
		return RoleAssistant, true
	default:
		return 0, false
	}
}
