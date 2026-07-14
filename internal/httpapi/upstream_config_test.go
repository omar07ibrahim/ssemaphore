package httpapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

const upstreamConfigTestToken = "upstream-config-token-._~+/=="

func TestNewHTTPUpstreamRejectsInvalidEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "empty", endpoint: ""},
		{name: "above byte maximum", endpoint: strings.Repeat("a", absoluteMaxUpstreamEndpointBytes+1)},
		{name: "relative", endpoint: chatCompletionsPath},
		{name: "unsupported scheme", endpoint: "ftp://api.example.com" + chatCompletionsPath},
		{name: "opaque URL", endpoint: "https:api.example.com" + chatCompletionsPath},
		{name: "empty host", endpoint: "https:///v1/chat/completions"},
		{name: "userinfo", endpoint: "https://user:password@api.example.com" + chatCompletionsPath},
		{name: "query", endpoint: "https://api.example.com" + chatCompletionsPath + "?version=1"},
		{name: "empty query", endpoint: "https://api.example.com" + chatCompletionsPath + "?"},
		{name: "fragment", endpoint: "https://api.example.com" + chatCompletionsPath + "#fragment"},
		{name: "raw path", endpoint: "https://api.example.com/v1/chat/%63ompletions"},
		{name: "wrong path", endpoint: "https://api.example.com/v1/responses"},
		{name: "trailing slash", endpoint: "https://api.example.com" + chatCompletionsPath + "/"},
		{name: "empty port", endpoint: "https://api.example.com:" + chatCompletionsPath},
		{name: "zero port", endpoint: "https://api.example.com:0" + chatCompletionsPath},
		{name: "port above maximum", endpoint: "https://api.example.com:65536" + chatCompletionsPath},
		{name: "non-numeric port", endpoint: "https://api.example.com:https" + chatCompletionsPath},
		{name: "hostname with underscore", endpoint: "https://api_internal.example.com" + chatCompletionsPath},
		{name: "hostname with trailing dot", endpoint: "https://api.example.com." + chatCompletionsPath},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := upstreamConfigTestBaseConfig()
			config.Endpoint = test.endpoint
			if upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken); err == nil {
				upstream.CloseIdleConnections()
				t.Fatal("NewHTTPUpstream() error = nil, want endpoint validation error")
			}
		})
	}
}

func TestNewHTTPUpstreamPlaintextRequiresNumericLoopback(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantOK   bool
	}{
		{name: "IPv4 loopback", endpoint: "http://127.0.0.1:8080" + chatCompletionsPath, wantOK: true},
		{name: "IPv4 loopback range", endpoint: "http://127.42.0.7" + chatCompletionsPath, wantOK: true},
		{name: "IPv6 loopback", endpoint: "http://[::1]:8080" + chatCompletionsPath, wantOK: true},
		{name: "localhost name", endpoint: "http://localhost:8080" + chatCompletionsPath},
		{name: "public hostname", endpoint: "http://api.example.com" + chatCompletionsPath},
		{name: "unspecified IPv4", endpoint: "http://0.0.0.0" + chatCompletionsPath},
		{name: "private IPv4", endpoint: "http://10.0.0.1" + chatCompletionsPath},
		{name: "unspecified IPv6", endpoint: "http://[::]" + chatCompletionsPath},
		{name: "public IPv6", endpoint: "http://[2001:db8::1]" + chatCompletionsPath},
		{name: "zoned IPv6", endpoint: "http://[fe80::1%25lo0]" + chatCompletionsPath},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := upstreamConfigTestBaseConfig()
			config.Endpoint = test.endpoint
			upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken)
			if test.wantOK {
				if err != nil {
					t.Fatalf("NewHTTPUpstream() error = %v", err)
				}
				upstream.CloseIdleConnections()
				return
			}
			if err == nil {
				upstream.CloseIdleConnections()
				t.Fatal("NewHTTPUpstream() error = nil, want plaintext endpoint rejection")
			}
		})
	}
}

func TestNewHTTPUpstreamAcceptsHTTPSDestinations(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		wantEndpoint string
	}{
		{
			name:         "DNS name",
			endpoint:     "HTTPS://api.example.com:8443" + chatCompletionsPath,
			wantEndpoint: "https://api.example.com:8443" + chatCompletionsPath,
		},
		{
			name:         "public IPv4",
			endpoint:     "https://192.0.2.1" + chatCompletionsPath,
			wantEndpoint: "https://192.0.2.1" + chatCompletionsPath,
		},
		{
			name:         "public IPv6",
			endpoint:     "https://[2001:db8::1]" + chatCompletionsPath,
			wantEndpoint: "https://[2001:db8::1]" + chatCompletionsPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := upstreamConfigTestBaseConfig()
			config.Endpoint = test.endpoint
			upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken)
			if err != nil {
				t.Fatalf("NewHTTPUpstream() error = %v", err)
			}
			defer upstream.CloseIdleConnections()
			if upstream.endpoint != test.wantEndpoint {
				t.Fatalf("endpoint = %q, want %q", upstream.endpoint, test.wantEndpoint)
			}
		})
	}
}

func TestNewHTTPUpstreamValidatesCredentialGrammarWithoutLeakingSecrets(t *testing.T) {
	valid := []string{
		"a",
		"token-._~+/==",
		strings.Repeat("a", absoluteMaxCredentialBytes),
	}
	for index, token := range valid {
		t.Run(fmt.Sprintf("valid_%d", index), func(t *testing.T) {
			upstream, err := NewHTTPUpstream(upstreamConfigTestBaseConfig(), token)
			if err != nil {
				t.Fatalf("NewHTTPUpstream() error = %v", err)
			}
			upstream.CloseIdleConnections()
		})
	}

	invalid := []string{
		"",
		"==",
		"a=b",
		"token with space",
		"token?query",
		"tøken",
		strings.Repeat("a", absoluteMaxCredentialBytes+1),
	}
	for index, token := range invalid {
		t.Run(fmt.Sprintf("invalid_%d", index), func(t *testing.T) {
			_, err := NewHTTPUpstream(upstreamConfigTestBaseConfig(), token)
			if err == nil {
				t.Fatal("NewHTTPUpstream() error = nil, want credential validation error")
			}
			if token != "" && strings.Contains(err.Error(), token) {
				t.Fatalf("NewHTTPUpstream() error leaked credential: %q", err)
			}
		})
	}
}

func TestHTTPUpstreamValuesRedactCredentialFromFormattingAndConfigJSON(t *testing.T) {
	const token = "credential-canary-._~+/=="
	config := upstreamConfigTestBaseConfig()
	upstream, err := NewHTTPUpstream(config, token)
	if err != nil {
		t.Fatalf("NewHTTPUpstream() error = %v", err)
	}
	defer upstream.CloseIdleConnections()

	formatted := []struct {
		name  string
		value string
		want  string
	}{
		{name: "config default", value: fmt.Sprintf("%v", config), want: "httpapi.HTTPUpstreamConfig{redacted}"},
		{name: "config fields", value: fmt.Sprintf("%+v", config), want: "httpapi.HTTPUpstreamConfig{redacted}"},
		{name: "config Go syntax", value: fmt.Sprintf("%#v", config), want: "httpapi.HTTPUpstreamConfig{redacted}"},
		{name: "upstream default", value: fmt.Sprintf("%v", upstream), want: "httpapi.HTTPUpstream{redacted}"},
		{name: "upstream fields", value: fmt.Sprintf("%+v", upstream), want: "httpapi.HTTPUpstream{redacted}"},
		{name: "upstream Go syntax", value: fmt.Sprintf("%#v", upstream), want: "httpapi.HTTPUpstream{redacted}"},
	}
	for _, item := range formatted {
		t.Run(item.name, func(t *testing.T) {
			if item.value != item.want {
				t.Fatalf("formatted value = %q, want %q", item.value, item.want)
			}
			if strings.Contains(item.value, token) {
				t.Fatalf("formatted value leaked credential: %q", item.value)
			}
		})
	}

	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal(config) error = %v", err)
	}
	if strings.Contains(string(encoded), token) || strings.Contains(string(encoded), "BearerToken") {
		t.Fatalf("JSON config exposed a credential field: %s", encoded)
	}
}

func TestNewHTTPUpstreamValidatesEveryTransportTimeoutBoundary(t *testing.T) {
	timeouts := []struct {
		name string
		set  func(*HTTPUpstreamConfig, time.Duration)
	}{
		{name: "connect", set: func(c *HTTPUpstreamConfig, value time.Duration) { c.ConnectTimeout = value }},
		{name: "TLS handshake", set: func(c *HTTPUpstreamConfig, value time.Duration) { c.TLSHandshakeTimeout = value }},
		{name: "response header", set: func(c *HTTPUpstreamConfig, value time.Duration) { c.ResponseHeaderTimeout = value }},
		{name: "idle connection", set: func(c *HTTPUpstreamConfig, value time.Duration) { c.IdleConnectionTimeout = value }},
	}
	boundaries := []struct {
		name   string
		value  time.Duration
		wantOK bool
	}{
		{name: "negative", value: -time.Nanosecond},
		{name: "zero", value: 0},
		{name: "minimum positive", value: time.Nanosecond, wantOK: true},
		{name: "exact maximum", value: absoluteMaxTransportTimeout, wantOK: true},
		{name: "above maximum", value: absoluteMaxTransportTimeout + time.Nanosecond},
	}

	for _, timeout := range timeouts {
		for _, boundary := range boundaries {
			t.Run(timeout.name+"/"+boundary.name, func(t *testing.T) {
				config := upstreamConfigTestBaseConfig()
				timeout.set(&config, boundary.value)
				upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken)
				if boundary.wantOK {
					if err != nil {
						t.Fatalf("NewHTTPUpstream() error = %v", err)
					}
					upstream.CloseIdleConnections()
					return
				}
				if err == nil {
					upstream.CloseIdleConnections()
					t.Fatalf("NewHTTPUpstream() error = nil for %s timeout %s", timeout.name, boundary.value)
				}
			})
		}
	}
}

func TestNewHTTPUpstreamValidatesHeaderAndConnectionBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*HTTPUpstreamConfig)
		wantOK bool
	}{
		{name: "negative header bytes", mutate: func(c *HTTPUpstreamConfig) { c.MaxResponseHeaderBytes = -1 }},
		{name: "zero header bytes", mutate: func(c *HTTPUpstreamConfig) { c.MaxResponseHeaderBytes = 0 }},
		{name: "one header byte", mutate: func(c *HTTPUpstreamConfig) { c.MaxResponseHeaderBytes = 1 }, wantOK: true},
		{name: "exact header maximum", mutate: func(c *HTTPUpstreamConfig) { c.MaxResponseHeaderBytes = absoluteMaxResponseHeaderBytes }, wantOK: true},
		{name: "above header maximum", mutate: func(c *HTTPUpstreamConfig) { c.MaxResponseHeaderBytes = absoluteMaxResponseHeaderBytes + 1 }},
		{name: "zero connections", mutate: func(c *HTTPUpstreamConfig) { c.MaxConnections = 0 }},
		{name: "one connection", mutate: func(c *HTTPUpstreamConfig) { c.MaxConnections = 1 }, wantOK: true},
		{name: "exact connection maximum", mutate: func(c *HTTPUpstreamConfig) { c.MaxConnections = absoluteMaxUpstreamConnections }, wantOK: true},
		{name: "above connection maximum", mutate: func(c *HTTPUpstreamConfig) { c.MaxConnections = absoluteMaxUpstreamConnections + 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := upstreamConfigTestBaseConfig()
			test.mutate(&config)
			upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken)
			if test.wantOK {
				if err != nil {
					t.Fatalf("NewHTTPUpstream() error = %v", err)
				}
				upstream.CloseIdleConnections()
				return
			}
			if err == nil {
				upstream.CloseIdleConnections()
				t.Fatal("NewHTTPUpstream() error = nil, want resource-bound validation error")
			}
		})
	}
}

func TestNewHTTPUpstreamConstructsHardenedBoundedTransport(t *testing.T) {
	config := HTTPUpstreamConfig{
		Endpoint:               "HTTPS://api.example.com:8443" + chatCompletionsPath,
		ConnectTimeout:         11 * time.Second,
		TLSHandshakeTimeout:    12 * time.Second,
		ResponseHeaderTimeout:  13 * time.Second,
		IdleConnectionTimeout:  14 * time.Second,
		MaxResponseHeaderBytes: 32 * 1024,
		MaxConnections:         17,
	}
	upstream, err := NewHTTPUpstream(config, upstreamConfigTestToken)
	if err != nil {
		t.Fatalf("NewHTTPUpstream() error = %v", err)
	}
	defer upstream.CloseIdleConnections()

	if upstream.endpoint != "https://api.example.com:8443"+chatCompletionsPath {
		t.Fatalf("endpoint = %q, want normalized immutable endpoint", upstream.endpoint)
	}
	if upstream.authorization != "Bearer "+upstreamConfigTestToken {
		t.Fatal("authorization was not constructed from the validated credential")
	}
	if upstream.client == nil || upstream.dialer == nil || upstream.transport == nil {
		t.Fatal("client, dialer, or transport is nil")
	}
	if upstream.client.Transport != upstream.transport {
		t.Fatal("client transport does not reference the owned transport")
	}
	if upstream.client.Timeout != 0 {
		t.Fatalf("client timeout = %s, want caller-owned context deadline", upstream.client.Timeout)
	}
	if upstream.client.Jar != nil {
		t.Fatal("client cookie jar is non-nil")
	}
	if upstream.client.CheckRedirect == nil {
		t.Fatal("client redirect policy is nil")
	}
	if upstream.dialer.Timeout != config.ConnectTimeout {
		t.Fatalf("connect timeout = %s, want %s", upstream.dialer.Timeout, config.ConnectTimeout)
	}
	if upstream.dialer.KeepAlive != 30*time.Second {
		t.Fatalf("TCP keep-alive = %s, want 30s", upstream.dialer.KeepAlive)
	}
	if upstream.dialer.FallbackDelay >= 0 {
		t.Fatalf("fast fallback delay = %s, want disabled to bound concurrent sockets", upstream.dialer.FallbackDelay)
	}
	request, requestErr := http.NewRequest(http.MethodGet, "https://redirect.example.com", nil)
	if requestErr != nil {
		t.Fatalf("http.NewRequest() error = %v", requestErr)
	}
	if redirectErr := upstream.client.CheckRedirect(request, nil); !errors.Is(redirectErr, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect() error = %v, want http.ErrUseLastResponse", redirectErr)
	}

	transport := upstream.transport
	if transport.Proxy != nil {
		t.Fatal("transport proxy policy is non-nil")
	}
	if transport.DialContext == nil {
		t.Fatal("transport dial context is nil")
	}
	if transport.Dial != nil || transport.DialTLS != nil || transport.DialTLSContext != nil {
		t.Fatal("transport contains an unexpected alternate dial hook")
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("transport TLS config is nil")
	}
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS minimum = %#x, want TLS 1.2", transport.TLSClientConfig.MinVersion)
	}
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS certificate verification is disabled")
	}
	if transport.TLSHandshakeTimeout != config.TLSHandshakeTimeout {
		t.Fatalf("TLS handshake timeout = %s, want %s", transport.TLSHandshakeTimeout, config.TLSHandshakeTimeout)
	}
	if !transport.DisableCompression {
		t.Fatal("transport compression is enabled")
	}
	connectionLimit := int(config.MaxConnections)
	if transport.MaxIdleConns != connectionLimit ||
		transport.MaxIdleConnsPerHost != connectionLimit ||
		transport.MaxConnsPerHost != connectionLimit {
		t.Fatalf(
			"connection limits = (%d, %d, %d), want %d",
			transport.MaxIdleConns,
			transport.MaxIdleConnsPerHost,
			transport.MaxConnsPerHost,
			connectionLimit,
		)
	}
	if transport.IdleConnTimeout != config.IdleConnectionTimeout {
		t.Fatalf("idle timeout = %s, want %s", transport.IdleConnTimeout, config.IdleConnectionTimeout)
	}
	if transport.ResponseHeaderTimeout != config.ResponseHeaderTimeout {
		t.Fatalf("response header timeout = %s, want %s", transport.ResponseHeaderTimeout, config.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != time.Second {
		t.Fatalf("expect-continue timeout = %s, want 1s", transport.ExpectContinueTimeout)
	}
	if transport.MaxResponseHeaderBytes != config.MaxResponseHeaderBytes {
		t.Fatalf("maximum response header bytes = %d, want %d", transport.MaxResponseHeaderBytes, config.MaxResponseHeaderBytes)
	}
	if transport.Protocols == nil {
		t.Fatal("transport protocols are nil")
	}
	if !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() || transport.Protocols.UnencryptedHTTP2() {
		t.Fatalf(
			"transport protocols = HTTP/1:%t HTTP/2:%t h2c:%t, want true, false, false",
			transport.Protocols.HTTP1(),
			transport.Protocols.HTTP2(),
			transport.Protocols.UnencryptedHTTP2(),
		)
	}
}

func TestClassifyUpstreamRequestErrorPrioritizesContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := classifyUpstreamRequestError(canceled, upstreamConfigTestTimeoutError("network timeout")); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled context classification = %v, want context.Canceled", got)
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if got := classifyUpstreamRequestError(expired, errors.New("network failure")); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("expired context classification = %v, want context.DeadlineExceeded", got)
	}
}

func TestClassifyUpstreamRequestErrorUsesTimeoutSentinel(t *testing.T) {
	wrapped := fmt.Errorf("outer transport wrapper: %w", upstreamConfigTestTimeoutError("dial timed out at a private address"))
	got := classifyUpstreamRequestError(context.Background(), wrapped)
	if got != ErrUpstreamTimeout {
		t.Fatalf("timeout classification = %v, want ErrUpstreamTimeout", got)
	}
}

func TestClassifyUpstreamRequestErrorReturnsStaticNonLeakingFailure(t *testing.T) {
	const canary = "credential-canary@private-upstream.example"
	got := classifyUpstreamRequestError(context.Background(), errors.New("dial failed for "+canary))
	if got != errUpstreamRequestFailed {
		t.Fatalf("failure classification = %v, want static errUpstreamRequestFailed", got)
	}
	if strings.Contains(got.Error(), canary) {
		t.Fatalf("failure classification leaked source error: %q", got)
	}
}

type upstreamConfigTestTimeoutError string

func (e upstreamConfigTestTimeoutError) Error() string { return string(e) }
func (upstreamConfigTestTimeoutError) Timeout() bool   { return true }

func upstreamConfigTestBaseConfig() HTTPUpstreamConfig {
	return HTTPUpstreamConfig{
		Endpoint:               "https://api.example.com" + chatCompletionsPath,
		ConnectTimeout:         time.Second,
		TLSHandshakeTimeout:    2 * time.Second,
		ResponseHeaderTimeout:  3 * time.Second,
		IdleConnectionTimeout:  4 * time.Second,
		MaxResponseHeaderBytes: 64 * 1024,
		MaxConnections:         8,
	}
}
