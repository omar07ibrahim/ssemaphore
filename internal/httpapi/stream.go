package httpapi

import (
	"mime"
	"net/http"
	"strings"
)

// validStreamingUpstreamMetadata accepts only an unencoded successful SSE
// response. The handler relays none of the validated upstream headers.
func validStreamingUpstreamMetadata(response UpstreamResponse) bool {
	if response.StatusCode != http.StatusOK || response.Body == nil {
		return false
	}
	if len(response.Header.Values("Content-Encoding")) != 0 {
		return false
	}
	values := response.Header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	if err != nil || !strings.EqualFold(mediaType, "text/event-stream") {
		return false
	}
	for name, value := range parameters {
		if !strings.EqualFold(name, "charset") || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}
