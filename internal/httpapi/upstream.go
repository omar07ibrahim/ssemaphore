package httpapi

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/omar07ibrahim/ssemaphore/internal/contract"
)

const (
	absoluteMaxUpstreamEndpointBytes = 4096
	absoluteMaxUpstreamConnections   = 4096
	absoluteMaxResponseHeaderBytes   = 1 << 20
	absoluteMaxTransportTimeout      = time.Hour

	upstreamUserAgent = "ssemaphore"
)

var (
	// ErrUpstreamTimeout is the only transport failure that the HTTP handler
	// distinguishes from an invalid or unavailable upstream. It carries no
	// network address, credential, or underlying error text.
	ErrUpstreamTimeout = errors.New("upstream transport timed out")

	errUpstreamDeadlineRequired = errors.New("upstream context must have a deadline")
	errUpstreamRequestInvalid   = errors.New("validated upstream request is invalid")
	errUpstreamRequestFailed    = errors.New("upstream HTTP request failed")
)

// HTTPUpstreamConfig defines one immutable Chat Completions destination and
// finite transport resource bounds. The bearer credential is deliberately a
// separate constructor argument so this policy value can be logged or encoded
// without containing a secret.
type HTTPUpstreamConfig struct {
	Endpoint string

	ConnectTimeout        time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	IdleConnectionTimeout time.Duration

	MaxResponseHeaderBytes int64
	MaxConnections         uint64
}

func (HTTPUpstreamConfig) String() string   { return "httpapi.HTTPUpstreamConfig{redacted}" }
func (HTTPUpstreamConfig) GoString() string { return "httpapi.HTTPUpstreamConfig{redacted}" }

// HTTPUpstream is a concurrent-safe, fixed-destination implementation of
// Upstream. The caller-owned context is the only total request timer;
// transport timeouts bound individual network phases.
type HTTPUpstream struct {
	endpoint      string
	authorization string
	client        *http.Client
	dialer        *net.Dialer
	transport     *http.Transport
}

var _ Upstream = (*HTTPUpstream)(nil)

// upstreamTransportContext preserves lifecycle signals while preventing
// caller-installed context values such as httptrace hooks from observing the
// operator credential or blocking the transport.
type upstreamTransportContext struct {
	context.Context
}

func (upstreamTransportContext) Value(any) any { return nil }

func (c upstreamTransportContext) AfterFunc(callback func()) func() bool {
	return context.AfterFunc(c.Context, callback)
}

func (HTTPUpstream) String() string   { return "httpapi.HTTPUpstream{redacted}" }
func (HTTPUpstream) GoString() string { return "httpapi.HTTPUpstream{redacted}" }

// NewHTTPUpstream validates all policy before constructing a reusable client.
// Construction performs no DNS lookup, dial, or other network operation.
func NewHTTPUpstream(config HTTPUpstreamConfig, bearerToken string) (*HTTPUpstream, error) {
	endpoint, err := validateUpstreamEndpoint(config.Endpoint)
	if err != nil {
		return nil, err
	}
	if !validBearerToken(bearerToken) {
		return nil, errors.New("upstream bearer credential is outside its safety bounds")
	}
	if err := validateTransportTimeout("connect", config.ConnectTimeout); err != nil {
		return nil, err
	}
	if err := validateTransportTimeout("TLS handshake", config.TLSHandshakeTimeout); err != nil {
		return nil, err
	}
	if err := validateTransportTimeout("response header", config.ResponseHeaderTimeout); err != nil {
		return nil, err
	}
	if err := validateTransportTimeout("idle connection", config.IdleConnectionTimeout); err != nil {
		return nil, err
	}
	if config.MaxResponseHeaderBytes <= 0 || config.MaxResponseHeaderBytes > absoluteMaxResponseHeaderBytes {
		return nil, errors.New("maximum upstream response header bytes is outside its safety bounds")
	}
	if config.MaxConnections == 0 || config.MaxConnections > absoluteMaxUpstreamConnections {
		return nil, errors.New("maximum upstream connections is outside its safety bounds")
	}

	dialer := &net.Dialer{
		Timeout:       config.ConnectTimeout,
		KeepAlive:     30 * time.Second,
		FallbackDelay: -1,
	}
	// Go's HTTP/2 transport may retry a request whose body has not yet been
	// read after selected connection errors. HTTP/1 plus a non-replayable body
	// keeps the v0 no-automatic-retry contract enforceable.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	connectionLimit := int(config.MaxConnections)
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:    config.TLSHandshakeTimeout,
		DisableCompression:     true,
		MaxIdleConns:           connectionLimit,
		MaxIdleConnsPerHost:    connectionLimit,
		MaxConnsPerHost:        connectionLimit,
		IdleConnTimeout:        config.IdleConnectionTimeout,
		ResponseHeaderTimeout:  config.ResponseHeaderTimeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: config.MaxResponseHeaderBytes,
		Protocols:              protocols,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &HTTPUpstream{
		endpoint:      endpoint,
		authorization: "Bearer " + bearerToken,
		client:        client,
		dialer:        dialer,
		transport:     transport,
	}, nil
}

// Complete sends one non-idempotent POST with no replay body or client-derived
// header. Response body ownership transfers to the Handler on success.
func (u *HTTPUpstream) Complete(ctx context.Context, request contract.Request) (UpstreamResponse, error) {
	if ctx == nil {
		return UpstreamResponse{}, errUpstreamDeadlineRequired
	}
	if err := ctx.Err(); err != nil {
		return UpstreamResponse{}, err
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		return UpstreamResponse{}, errUpstreamDeadlineRequired
	}
	if request.BodyBytes() == 0 {
		return UpstreamResponse{}, errUpstreamRequestInvalid
	}

	outboundContext := upstreamTransportContext{Context: ctx}
	outbound, err := http.NewRequestWithContext(outboundContext, http.MethodPost, u.endpoint, request.BodyReader())
	if err != nil {
		return UpstreamResponse{}, errUpstreamRequestInvalid
	}
	outbound.ContentLength = int64(request.BodyBytes())
	outbound.GetBody = nil
	outbound.Header = make(http.Header, 4)
	accept := "application/json"
	if request.Mode() == contract.RequestModeStreaming {
		accept = "text/event-stream"
	}
	outbound.Header.Set("Accept", accept)
	outbound.Header.Set("Authorization", u.authorization)
	outbound.Header.Set("Content-Type", "application/json")
	outbound.Header.Set("User-Agent", upstreamUserAgent)

	response, requestErr := u.client.Do(outbound)
	if requestErr != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return UpstreamResponse{}, classifyUpstreamRequestError(ctx, requestErr)
	}

	return UpstreamResponse{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       response.Body,
	}, nil
}

// CloseIdleConnections closes pooled keep-alive connections without affecting
// active requests. It is safe to call concurrently and during shutdown.
func (u *HTTPUpstream) CloseIdleConnections() {
	u.transport.CloseIdleConnections()
}

func validateUpstreamEndpoint(value string) (string, error) {
	if value == "" || len(value) > absoluteMaxUpstreamEndpointBytes {
		return "", errors.New("upstream endpoint is outside its safety bounds")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", errors.New("upstream endpoint is invalid")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("upstream endpoint scheme is unsupported")
	}
	if !parsed.IsAbs() || parsed.Opaque != "" || parsed.Host == "" || parsed.Hostname() == "" {
		return "", errors.New("upstream endpoint must be an absolute HTTP URL")
	}
	if !validUpstreamHostname(parsed.Hostname()) || strings.HasSuffix(parsed.Host, ":") {
		return "", errors.New("upstream endpoint authority is invalid")
	}
	if port := parsed.Port(); port != "" {
		portNumber, portErr := strconv.ParseUint(port, 10, 16)
		if portErr != nil || portNumber == 0 {
			return "", errors.New("upstream endpoint authority is invalid")
		}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", errors.New("upstream endpoint contains unsupported URL components")
	}
	if parsed.RawPath != "" || parsed.Path != chatCompletionsPath {
		return "", errors.New("upstream endpoint path must be the Chat Completions path")
	}
	if parsed.Scheme == "http" {
		address, parseErr := netip.ParseAddr(parsed.Hostname())
		if parseErr != nil || address.Zone() != "" || !address.IsLoopback() {
			return "", errors.New("plaintext upstream endpoint must use a numeric loopback address")
		}
	}
	return parsed.String(), nil
}

func validUpstreamHostname(hostname string) bool {
	if address, err := netip.ParseAddr(hostname); err == nil {
		return address.Zone() == ""
	}
	if hostname == "" || len(hostname) > 253 || strings.HasSuffix(hostname, ".") {
		return false
	}
	for _, label := range strings.Split(hostname, ".") {
		if label == "" || len(label) > 63 || !asciiAlphaNumeric(label[0]) || !asciiAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for index := 1; index < len(label)-1; index++ {
			if !asciiAlphaNumeric(label[index]) && label[index] != '-' {
				return false
			}
		}
	}
	return true
}

func asciiAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func validateTransportTimeout(name string, value time.Duration) error {
	if value <= 0 || value > absoluteMaxTransportTimeout {
		return errors.New(name + " timeout is outside its safety bounds")
	}
	return nil
}

func classifyUpstreamRequestError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return ErrUpstreamTimeout
	}
	return errUpstreamRequestFailed
}
