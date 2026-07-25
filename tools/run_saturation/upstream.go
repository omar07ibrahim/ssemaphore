package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	chatCompletionsPath = "/v1/chat/completions"
	upstreamCredential  = "synthetic-upstream-credential"

	bufferedCompletionBody = `{"object":"chat.completion","choices":[]}`
	streamChunkEvent       = "data: {\"object\":\"chat.completion.chunk\"}\n\n"
	streamDoneEvent        = "data: [DONE]\n\n"
)

var (
	errControlledUpstreamStart    = errors.New("controlled upstream could not start")
	errControlledUpstreamLine     = errors.New("controlled upstream received invalid HTTP framing")
	errControlledUpstreamHeaders  = errors.New("controlled upstream received invalid headers")
	errControlledUpstreamBody     = errors.New("controlled upstream received an invalid bounded body")
	errControlledUpstreamFixture  = errors.New("controlled upstream received an unknown fixture")
	errControlledUpstreamDispatch = errors.New("controlled upstream dispatch failed")
	errControlledUpstreamClose    = errors.New("controlled upstream did not close cleanly")
)

type upstreamDispatch struct {
	job          saturationJob
	arrived      time.Time
	releaseFirst chan struct{}
	releaseDone  chan struct{}
}

type controlledUpstream struct {
	host string
	jobs map[uint64]saturationJob

	listener   *net.TCPListener
	server     *http.Server
	dispatches chan upstreamDispatch
	issues     chan error
	serveDone  chan error
	stop       context.Context
	cancelStop context.CancelFunc

	mu         sync.Mutex
	dispatched map[uint64]struct{}
	issueOnce  sync.Once
}

func startControlledUpstream(workload seededWorkload) (*controlledUpstream, error) {
	listener, err := net.ListenTCP(
		"tcp4",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)},
	)
	if err != nil {
		return nil, errControlledUpstreamStart
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		address.Port <= 0 || address.Port > 65_535 {
		_ = listener.Close()
		return nil, errControlledUpstreamStart
	}

	stop, cancelStop := context.WithCancel(context.Background())
	upstream := &controlledUpstream{
		host:       net.JoinHostPort("127.0.0.1", strconv.Itoa(address.Port)),
		jobs:       workload.jobs,
		listener:   listener,
		dispatches: make(chan upstreamDispatch, 1),
		issues:     make(chan error, 1),
		serveDone:  make(chan error, 1),
		stop:       stop,
		cancelStop: cancelStop,
		dispatched: make(map[uint64]struct{}, ciTotalRequestCount),
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	upstream.server = &http.Server{
		Handler:           upstream,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       2 * time.Second,
		Protocols:         protocols,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	go func() {
		upstream.serveDone <- upstream.server.Serve(listener)
	}()
	return upstream, nil
}

func (u *controlledUpstream) endpoint() string {
	return "http://" + u.host + chatCompletionsPath
}

func (u *controlledUpstream) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	job, validationErr := u.validateRequest(request)
	if validationErr != nil {
		u.recordIssue(validationErr)
		writer.WriteHeader(http.StatusBadGateway)
		return
	}
	if !u.markDispatched(job.id) {
		u.recordIssue(errControlledUpstreamDispatch)
		writer.WriteHeader(http.StatusBadGateway)
		return
	}

	dispatch := upstreamDispatch{
		job:          job,
		arrived:      time.Now(),
		releaseFirst: make(chan struct{}),
	}
	if job.stream {
		dispatch.releaseDone = make(chan struct{})
	}
	select {
	case u.dispatches <- dispatch:
	case <-request.Context().Done():
		return
	case <-u.stop.Done():
		return
	}

	select {
	case <-dispatch.releaseFirst:
	case <-request.Context().Done():
		return
	case <-u.stop.Done():
		return
	}

	if !job.stream {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Content-Length", strconv.Itoa(len(bufferedCompletionBody)))
		writer.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(writer, bufferedCompletionBody); err != nil {
			u.recordIssue(errControlledUpstreamDispatch)
		}
		return
	}

	flusher, ok := writer.(http.Flusher)
	if !ok {
		u.recordIssue(errControlledUpstreamDispatch)
		writer.WriteHeader(http.StatusBadGateway)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(writer, streamChunkEvent); err != nil {
		u.recordIssue(errControlledUpstreamDispatch)
		return
	}
	flusher.Flush()

	select {
	case <-dispatch.releaseDone:
	case <-request.Context().Done():
		return
	case <-u.stop.Done():
		return
	}
	if _, err := io.WriteString(writer, streamDoneEvent); err != nil {
		u.recordIssue(errControlledUpstreamDispatch)
	}
}

type controlledRequest struct {
	Model               string              `json:"model"`
	Messages            []controlledMessage `json:"messages"`
	MaxCompletionTokens uint64              `json:"max_completion_tokens"`
	Stream              bool                `json:"stream"`
}

type controlledMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (u *controlledUpstream) validateRequest(
	request *http.Request,
) (saturationJob, error) {
	if request == nil || request.Method != http.MethodPost ||
		request.ProtoMajor != 1 || request.URL == nil ||
		request.URL.Path != chatCompletionsPath ||
		request.URL.RawPath != "" || request.URL.RawQuery != "" ||
		request.Host != u.host || request.ContentLength <= 0 ||
		len(request.TransferEncoding) != 0 {
		return saturationJob{}, errControlledUpstreamLine
	}
	if !exactUpstreamHeaders(request.Header, request.ContentLength) {
		return saturationJob{}, errControlledUpstreamHeaders
	}

	body, readErr := io.ReadAll(io.LimitReader(
		request.Body,
		int64(ciMaximumBodyBytes+1),
	))
	closeErr := request.Body.Close()
	if readErr != nil || closeErr != nil ||
		len(body) == 0 || len(body) > ciMaximumBodyBytes ||
		request.ContentLength != int64(len(body)) {
		return saturationJob{}, errControlledUpstreamBody
	}

	decoded := controlledRequest{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&decoded) != nil {
		return saturationJob{}, errControlledUpstreamBody
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return saturationJob{}, errControlledUpstreamBody
	}
	if decoded.Model != syntheticModel || len(decoded.Messages) != 1 ||
		decoded.Messages[0] != (controlledMessage{Role: "user", Content: ""}) {
		return saturationJob{}, errControlledUpstreamFixture
	}
	job, exists := u.jobs[decoded.MaxCompletionTokens]
	if !exists || job.id != decoded.MaxCompletionTokens ||
		job.stream != decoded.Stream || !bytes.Equal(body, job.body) ||
		(job.kind != jobControl && job.kind != jobService) {
		return saturationJob{}, errControlledUpstreamFixture
	}
	wantAccept := "application/json"
	if job.stream {
		wantAccept = "text/event-stream"
	}
	if request.Header.Get("Accept") != wantAccept {
		return saturationJob{}, errControlledUpstreamHeaders
	}
	return job, nil
}

func exactUpstreamHeaders(header http.Header, contentLength int64) bool {
	if len(header) != 5 || contentLength <= 0 {
		return false
	}
	expected := []struct {
		name         string
		value        string
		modeSpecific bool
	}{
		{name: "Accept", value: "application/json", modeSpecific: true},
		{name: "Authorization", value: "Bearer " + upstreamCredential},
		{name: "Content-Length", value: strconv.FormatInt(contentLength, 10)},
		{name: "Content-Type", value: "application/json"},
		{name: "User-Agent", value: "ssemaphore"},
	}
	for _, item := range expected {
		values := header.Values(item.name)
		if len(values) != 1 {
			return false
		}
		if !item.modeSpecific && values[0] != item.value {
			return false
		}
	}
	return len(header.Values("Accept-Encoding")) == 0
}

func (u *controlledUpstream) markDispatched(identifier uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, duplicate := u.dispatched[identifier]; duplicate {
		return false
	}
	u.dispatched[identifier] = struct{}{}
	return true
}

func (u *controlledUpstream) dispatchedCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.dispatched)
}

func (u *controlledUpstream) recordIssue(err error) {
	u.issueOnce.Do(func() {
		u.issues <- err
	})
}

func (u *controlledUpstream) shutdown(ctx context.Context) error {
	if ctx == nil {
		return errControlledUpstreamClose
	}
	u.cancelStop()
	shutdownErr := u.server.Shutdown(ctx)
	select {
	case serveErr := <-u.serveDone:
		if shutdownErr != nil ||
			serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return errControlledUpstreamClose
		}
		return nil
	case <-ctx.Done():
		return errControlledUpstreamClose
	}
}

func (u *controlledUpstream) abort(ctx context.Context) {
	u.cancelStop()
	_ = u.server.Close()
	select {
	case <-u.serveDone:
	case <-ctx.Done():
	}
}
