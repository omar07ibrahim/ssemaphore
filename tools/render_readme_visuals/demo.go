package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	loopbackDemoTenantEnvironment   = "SSEMAPHORE_README_DEMO_TENANT_TOKEN"
	loopbackDemoUpstreamEnvironment = "SSEMAPHORE_README_DEMO_UPSTREAM_TOKEN"
	loopbackDemoChatPath            = "/v1/chat/completions"

	loopbackDemoClientHeader      = "X-Ssemaphore-Demo-Client"
	loopbackDemoClientHeaderValue = "synthetic-client-only-metadata"
	loopbackDemoClientUserAgent   = "ssemaphore-readme-demo-client"
	loopbackDemoUnsafeHeader      = "X-Ssemaphore-Upstream-Unsafe"
	loopbackDemoAcceptBarrierPath = "/__ssemaphore_accept_barrier__"

	loopbackDemoBufferedRequest = `{"model":"portfolio-model","messages":[{"role":"user","content":"show the bounded buffered path"}],"max_completion_tokens":8}`
	loopbackDemoStreamRequest   = `{"model":"portfolio-model","messages":[{"role":"user","content":"show the bounded streaming path"}],"max_completion_tokens":8,"stream":true}`

	loopbackDemoBufferedResponse = `{"id":"chatcmpl-demo-buffered","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"bounded relay"}}]}`
	loopbackDemoChunkOne         = "data: {\"id\":\"chatcmpl-demo-stream\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"bounded \"}}]}\n\n"
	loopbackDemoChunkTwo         = "data: {\"id\":\"chatcmpl-demo-stream\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"stream\"},\"finish_reason\":\"stop\"}]}\n\n"
	loopbackDemoDone             = "data: [DONE]\n\n"

	loopbackDemoValidateOutput = "gateway policy is valid\n"
	loopbackDemoTotalTimeout   = 40 * time.Second
)

type demoEvidence struct {
	GoVersion                          string
	OperatingSystem                    string
	ValidateStdout                     string
	ValidatePortStayedReserved         bool
	ValidateUpstreamCalls              int
	ValidateUpstreamConnections        int
	BufferedStatus                     int
	BufferedProtocolMajor              int
	BufferedBodyExact                  bool
	BufferedSafeHeaders                bool
	StreamStatus                       int
	StreamProtocolMajor                int
	StreamEvents                       []string
	FirstChunkBeforeRelease            bool
	StreamSafeHeaders                  bool
	TenantCredentialAbsentUpstream     bool
	UpstreamCredentialAbsentDownstream bool
	UpstreamHeadersStripped            bool
	ClientHeadersAbsentUpstream        bool
	UpstreamHeaderAllowlistExact       bool
	SeparateUpstreamCredential         bool
	InflightStreamCompleted            bool
	ServeOutputEmpty                   bool
	ShutdownExitCode                   int
	ListenerReleased                   bool
	UpstreamCalls                      int
}

type loopbackDemoUpstreamCheck struct {
	call                   int
	requestExact           bool
	headerAllowlistExact   bool
	tenantCredentialAbsent bool
	clientHeadersAbsent    bool
	separateCredential     bool
}

type loopbackDemoUpstream struct {
	host            string
	tenantToken     string
	upstreamToken   string
	calls           atomic.Int32
	acceptBarrier   chan loopbackDemoAcceptBarrier
	checks          chan loopbackDemoUpstreamCheck
	chunkOneFlushed chan struct{}
	streamResult    chan bool
	releaseStream   chan struct{}
	releaseOnce     sync.Once
	released        atomic.Bool
}

type loopbackDemoAcceptedConnection struct {
	*net.TCPConn
	sequence int64
}

type loopbackDemoCountingListener struct {
	*net.TCPListener
	accepted atomic.Int64
}

type loopbackDemoConnectionSequenceKey struct{}

type loopbackDemoAcceptBarrier struct {
	sequence int64
	exact    bool
}

type loopbackDemoProcessWait struct {
	done chan struct{}
	err  error
}

type loopbackDemoBufferedResult struct {
	status                   int
	protocolMajor            int
	bodyExact                bool
	safeHeaders              bool
	upstreamCredentialAbsent bool
	upstreamHeadersStripped  bool
}

type loopbackDemoStreamResult struct {
	status                   int
	protocolMajor            int
	events                   []string
	firstChunkBeforeRelease  bool
	safeHeaders              bool
	upstreamCredentialAbsent bool
	upstreamHeadersStripped  bool
	inflightCompleted        bool
}

func runLoopbackDemo(ctx context.Context, root string) (evidence demoEvidence, resultErr error) {
	if ctx == nil {
		return demoEvidence{}, errors.New("loopback demo requires a context")
	}
	demoContext, cancelDemo := context.WithTimeout(ctx, loopbackDemoTotalTimeout)
	defer cancelDemo()

	root, err := filepath.Abs(root)
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not resolve the repository")
	}

	privateDirectory, err := os.MkdirTemp("", "ssemaphore-readme-demo-")
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not create private storage")
	}
	if err := os.Chmod(privateDirectory, 0o700); err != nil {
		_ = os.RemoveAll(privateDirectory)
		return demoEvidence{}, errors.New("loopback demo could not secure private storage")
	}

	var (
		gatewayReservation *net.TCPListener
		upstreamListener   *net.TCPListener
		countingListener   *loopbackDemoCountingListener
		upstreamServer     *http.Server
		upstreamWait       chan error
		upstream           *loopbackDemoUpstream
		serveCommand       *exec.Cmd
		serveWait          *loopbackDemoProcessWait
		clientTransport    *http.Transport
	)
	defer func() {
		cleanupFailed := false
		if upstream != nil {
			upstream.release()
		}
		if clientTransport != nil {
			clientTransport.CloseIdleConnections()
		}
		if serveWait != nil && !serveWait.finished() {
			if serveCommand == nil || serveCommand.Process == nil {
				cleanupFailed = true
			} else {
				_ = serveCommand.Process.Kill()
				if !serveWait.wait(3 * time.Second) {
					cleanupFailed = true
				}
			}
		}
		if gatewayReservation != nil {
			if closeErr := gatewayReservation.Close(); closeErr != nil &&
				!errors.Is(closeErr, net.ErrClosed) {
				cleanupFailed = true
			}
		}
		if upstreamServer != nil {
			shutdownContext, cancelShutdown := context.WithTimeout(
				context.Background(),
				3*time.Second,
			)
			shutdownErr := upstreamServer.Shutdown(shutdownContext)
			cancelShutdown()
			if shutdownErr != nil {
				if closeErr := upstreamServer.Close(); closeErr != nil &&
					!errors.Is(closeErr, net.ErrClosed) {
					cleanupFailed = true
				}
			}
		} else if upstreamListener != nil {
			if closeErr := upstreamListener.Close(); closeErr != nil &&
				!errors.Is(closeErr, net.ErrClosed) {
				cleanupFailed = true
			}
		}
		if upstreamWait != nil {
			select {
			case serveErr := <-upstreamWait:
				if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
					cleanupFailed = true
				}
			case <-time.After(3 * time.Second):
				cleanupFailed = true
			}
		}
		if removeErr := os.RemoveAll(privateDirectory); removeErr != nil {
			cleanupFailed = true
		}
		if cleanupFailed && resultErr == nil {
			evidence = demoEvidence{}
			resultErr = errors.New("loopback demo cleanup did not finish cleanly")
		}
	}()

	binaryPath := filepath.Join(privateDirectory, "ssemaphore")
	if err := loopbackDemoBuildBinary(demoContext, root, binaryPath); err != nil {
		return demoEvidence{}, err
	}

	tenantToken, err := loopbackDemoRandomToken()
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not create synthetic credentials")
	}
	upstreamToken, err := loopbackDemoRandomToken()
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not create synthetic credentials")
	}
	for upstreamToken == tenantToken {
		upstreamToken, err = loopbackDemoRandomToken()
		if err != nil {
			return demoEvidence{}, errors.New("loopback demo could not create synthetic credentials")
		}
	}

	upstreamListener, err = net.ListenTCP("tcp4", &net.TCPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not reserve an upstream listener")
	}
	upstreamAddress, ok := loopbackDemoTCPAddress(upstreamListener)
	if !ok {
		return demoEvidence{}, errors.New("loopback demo received an invalid upstream listener")
	}

	upstream = &loopbackDemoUpstream{
		host:            net.JoinHostPort("127.0.0.1", strconv.Itoa(upstreamAddress.Port)),
		tenantToken:     tenantToken,
		upstreamToken:   upstreamToken,
		acceptBarrier:   make(chan loopbackDemoAcceptBarrier, 1),
		checks:          make(chan loopbackDemoUpstreamCheck, 4),
		chunkOneFlushed: make(chan struct{}, 1),
		streamResult:    make(chan bool, 1),
		releaseStream:   make(chan struct{}),
	}
	countingListener = &loopbackDemoCountingListener{
		TCPListener: upstreamListener,
	}
	upstreamProtocols := new(http.Protocols)
	upstreamProtocols.SetHTTP1(true)
	upstreamServer = &http.Server{
		Handler:           upstream,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       5 * time.Second,
		Protocols:         upstreamProtocols,
		ErrorLog:          log.New(io.Discard, "", 0),
		ConnContext:       loopbackDemoConnectionContext,
	}
	upstreamWait = make(chan error, 1)
	go func() {
		upstreamWait <- upstreamServer.Serve(countingListener)
	}()

	gatewayReservation, err = net.ListenTCP("tcp4", &net.TCPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo could not reserve a gateway listener")
	}
	gatewayAddress, ok := loopbackDemoTCPAddress(gatewayReservation)
	if !ok {
		return demoEvidence{}, errors.New("loopback demo received an invalid gateway listener")
	}
	gatewayHost := net.JoinHostPort("127.0.0.1", strconv.Itoa(gatewayAddress.Port))

	policyPath := filepath.Join(privateDirectory, "policy.json")
	upstreamEndpoint := "http://" + upstream.host + loopbackDemoChatPath
	if err := loopbackDemoWritePolicy(
		root,
		policyPath,
		gatewayAddress.Port,
		upstreamEndpoint,
	); err != nil {
		return demoEvidence{}, err
	}

	childEnvironment := loopbackDemoChildEnvironment(tenantToken, upstreamToken)
	validateContext, cancelValidate := context.WithTimeout(demoContext, 10*time.Second)
	validateCommand := exec.CommandContext(
		validateContext,
		binaryPath,
		"validate",
		"--config",
		policyPath,
	)
	validateCommand.Dir = root
	validateCommand.Env = childEnvironment
	var validateStdout bytes.Buffer
	var validateStderr bytes.Buffer
	validateCommand.Stdout = &validateStdout
	validateCommand.Stderr = &validateStderr
	validateRunErr := validateCommand.Run()
	cancelValidate()
	if validateRunErr != nil || validateCommand.ProcessState == nil ||
		validateCommand.ProcessState.ExitCode() != 0 {
		return demoEvidence{}, errors.New("loopback demo policy validation failed")
	}
	if validateStdout.String() != loopbackDemoValidateOutput || validateStderr.Len() != 0 {
		return demoEvidence{}, errors.New("loopback demo policy validation output was not exact")
	}

	evidence.GoVersion = runtime.Version()
	evidence.OperatingSystem = runtime.GOOS
	evidence.ValidateStdout = validateStdout.String()
	validateConnections, err := loopbackDemoAcceptedBeforeBarrier(
		demoContext,
		upstream.host,
		countingListener,
		upstream.acceptBarrier,
	)
	if err != nil {
		return demoEvidence{}, err
	}
	evidence.ValidateUpstreamConnections = validateConnections
	evidence.ValidateUpstreamCalls = int(upstream.calls.Load())
	if evidence.ValidateUpstreamCalls != 0 ||
		evidence.ValidateUpstreamConnections != 0 {
		return demoEvidence{}, errors.New("loopback demo validation contacted the upstream")
	}
	competingListener, competingErr := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: gatewayAddress.Port,
	})
	if competingErr == nil {
		_ = competingListener.Close()
		return demoEvidence{}, errors.New("loopback demo validation released the reserved listener")
	}
	if !errors.Is(competingErr, syscall.EADDRINUSE) {
		return demoEvidence{}, errors.New("loopback demo could not prove listener ownership")
	}
	evidence.ValidatePortStayedReserved = true

	if closeErr := gatewayReservation.Close(); closeErr != nil {
		return demoEvidence{}, errors.New("loopback demo could not release the gateway reservation")
	}
	gatewayReservation = nil

	serveCommand = exec.CommandContext(
		demoContext,
		binaryPath,
		"serve",
		"--config",
		policyPath,
	)
	serveCommand.Dir = root
	serveCommand.Env = childEnvironment
	var serveStdout bytes.Buffer
	var serveStderr bytes.Buffer
	serveCommand.Stdout = &serveStdout
	serveCommand.Stderr = &serveStderr
	if err := serveCommand.Start(); err != nil {
		return demoEvidence{}, errors.New("loopback demo could not start the gateway")
	}
	serveWait = loopbackDemoWaitForCommand(serveCommand)

	if err := loopbackDemoWaitForListener(demoContext, gatewayHost, serveWait); err != nil {
		return demoEvidence{}, err
	}

	clientProtocols := new(http.Protocols)
	clientProtocols.SetHTTP1(true)
	clientTransport = &http.Transport{
		Proxy:              nil,
		DisableCompression: true,
		Protocols:          clientProtocols,
	}
	client := &http.Client{Transport: clientTransport}
	gatewayEndpoint := "http://" + gatewayHost + loopbackDemoChatPath

	buffered, err := loopbackDemoBufferedExchange(
		demoContext,
		client,
		gatewayEndpoint,
		tenantToken,
		upstreamToken,
	)
	if err != nil {
		return demoEvidence{}, err
	}
	evidence.BufferedStatus = buffered.status
	evidence.BufferedProtocolMajor = buffered.protocolMajor
	evidence.BufferedBodyExact = buffered.bodyExact
	evidence.BufferedSafeHeaders = buffered.safeHeaders

	bufferedCheck, err := loopbackDemoAwaitCheck(demoContext, upstream.checks, 1)
	if err != nil {
		return demoEvidence{}, err
	}
	switch {
	case !bufferedCheck.requestExact:
		return demoEvidence{}, errors.New("loopback demo buffered upstream request framing was not exact")
	case !bufferedCheck.headerAllowlistExact:
		return demoEvidence{}, errors.New("loopback demo buffered upstream request allowlist was not exact")
	case !bufferedCheck.tenantCredentialAbsent:
		return demoEvidence{}, errors.New("loopback demo buffered upstream request crossed a tenant credential boundary")
	case !bufferedCheck.clientHeadersAbsent:
		return demoEvidence{}, errors.New("loopback demo buffered upstream request retained client metadata")
	case !bufferedCheck.separateCredential:
		return demoEvidence{}, errors.New("loopback demo buffered upstream request did not use a separate credential")
	}
	if upstream.calls.Load() != 1 {
		return demoEvidence{}, errors.New("loopback demo buffered exchange made an unexpected upstream attempt")
	}

	streamed, err := loopbackDemoStreamingExchange(
		demoContext,
		client,
		gatewayEndpoint,
		gatewayHost,
		tenantToken,
		upstreamToken,
		upstream,
		serveCommand,
		serveWait,
	)
	if err != nil {
		return demoEvidence{}, err
	}
	evidence.StreamStatus = streamed.status
	evidence.StreamProtocolMajor = streamed.protocolMajor
	evidence.StreamEvents = streamed.events
	evidence.FirstChunkBeforeRelease = streamed.firstChunkBeforeRelease
	evidence.StreamSafeHeaders = streamed.safeHeaders
	evidence.InflightStreamCompleted = streamed.inflightCompleted

	streamCheck, err := loopbackDemoAwaitCheck(demoContext, upstream.checks, 2)
	if err != nil {
		return demoEvidence{}, err
	}
	switch {
	case !streamCheck.requestExact:
		return demoEvidence{}, errors.New("loopback demo streaming upstream request framing was not exact")
	case !streamCheck.headerAllowlistExact:
		return demoEvidence{}, errors.New("loopback demo streaming upstream request allowlist was not exact")
	case !streamCheck.tenantCredentialAbsent:
		return demoEvidence{}, errors.New("loopback demo streaming upstream request crossed a tenant credential boundary")
	case !streamCheck.clientHeadersAbsent:
		return demoEvidence{}, errors.New("loopback demo streaming upstream request retained client metadata")
	case !streamCheck.separateCredential:
		return demoEvidence{}, errors.New("loopback demo streaming upstream request did not use a separate credential")
	}

	evidence.TenantCredentialAbsentUpstream =
		bufferedCheck.tenantCredentialAbsent && streamCheck.tenantCredentialAbsent
	evidence.ClientHeadersAbsentUpstream =
		bufferedCheck.clientHeadersAbsent && streamCheck.clientHeadersAbsent
	evidence.UpstreamHeaderAllowlistExact =
		bufferedCheck.headerAllowlistExact && streamCheck.headerAllowlistExact
	evidence.SeparateUpstreamCredential =
		bufferedCheck.separateCredential && streamCheck.separateCredential
	evidence.UpstreamCredentialAbsentDownstream =
		buffered.upstreamCredentialAbsent && streamed.upstreamCredentialAbsent
	evidence.UpstreamHeadersStripped =
		buffered.upstreamHeadersStripped && streamed.upstreamHeadersStripped

	if !serveWait.finished() {
		if !serveWait.wait(10 * time.Second) {
			return demoEvidence{}, errors.New("loopback demo gateway did not finish shutdown")
		}
	}
	evidence.ShutdownExitCode = -1
	if serveCommand.ProcessState != nil {
		evidence.ShutdownExitCode = serveCommand.ProcessState.ExitCode()
	}
	if serveWait.err != nil || evidence.ShutdownExitCode != 0 {
		return demoEvidence{}, errors.New("loopback demo gateway did not exit cleanly")
	}
	evidence.ServeOutputEmpty = serveStdout.Len() == 0 && serveStderr.Len() == 0
	if !evidence.ServeOutputEmpty {
		return demoEvidence{}, errors.New("loopback demo gateway produced unexpected process output")
	}

	evidence.UpstreamCalls = int(upstream.calls.Load())
	if evidence.UpstreamCalls != 2 {
		return demoEvidence{}, errors.New("loopback demo made an unexpected number of upstream attempts")
	}

	clientTransport.CloseIdleConnections()
	rebound, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: gatewayAddress.Port,
	})
	if err != nil {
		return demoEvidence{}, errors.New("loopback demo gateway listener was not released")
	}
	evidence.ListenerReleased = true
	if closeErr := rebound.Close(); closeErr != nil {
		return demoEvidence{}, errors.New("loopback demo could not close its listener proof")
	}

	if !evidence.BufferedBodyExact || !evidence.BufferedSafeHeaders ||
		!evidence.StreamSafeHeaders || !evidence.FirstChunkBeforeRelease ||
		!evidence.InflightStreamCompleted ||
		!evidence.TenantCredentialAbsentUpstream ||
		!evidence.UpstreamCredentialAbsentDownstream ||
		!evidence.UpstreamHeadersStripped ||
		!evidence.ClientHeadersAbsentUpstream ||
		!evidence.UpstreamHeaderAllowlistExact ||
		!evidence.SeparateUpstreamCredential {
		return demoEvidence{}, errors.New("loopback demo did not prove every relay boundary")
	}

	return evidence, nil
}

func loopbackDemoBuildBinary(ctx context.Context, root, destination string) error {
	buildContext, cancelBuild := context.WithTimeout(ctx, 25*time.Second)
	defer cancelBuild()
	command := exec.CommandContext(
		buildContext,
		filepath.Join(runtime.GOROOT(), "bin", "go"),
		"build",
		"-trimpath",
		"-o",
		destination,
		"./cmd/ssemaphore",
	)
	command.Dir = root
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil || command.ProcessState == nil ||
		command.ProcessState.ExitCode() != 0 {
		return errors.New("loopback demo could not build the gateway binary")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		return errors.New("loopback demo gateway build produced unexpected output")
	}
	info, err := os.Stat(destination)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o100 == 0 {
		return errors.New("loopback demo gateway binary was not executable")
	}
	return nil
}

func loopbackDemoRandomToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "synthetic-" + hex.EncodeToString(raw), nil
}

func loopbackDemoWritePolicy(
	root string,
	destination string,
	gatewayPort int,
	upstreamEndpoint string,
) error {
	repository, err := openPinnedVisualDirectory(root, false)
	if err != nil {
		return errors.New("loopback demo could not open the repository")
	}
	payload, _, readErr := readBoundedVisualFile(
		repository,
		"configs/policy.example.json",
		visualMaxSourceFileBytes,
	)
	closeErr := repository.Close()
	if readErr != nil || closeErr != nil {
		return errors.New("loopback demo could not read the policy example")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	document := make(map[string]any)
	decodeErr := decoder.Decode(&document)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || !errors.Is(trailingErr, io.EOF) {
		return errors.New("loopback demo policy example was not one JSON document")
	}

	listener, ok := document["listener"].(map[string]any)
	if !ok {
		return errors.New("loopback demo policy example has no listener object")
	}
	listener["host"] = "127.0.0.1"
	listener["port"] = json.Number(strconv.Itoa(gatewayPort))

	configuredUpstream, ok := document["upstream"].(map[string]any)
	if !ok {
		return errors.New("loopback demo policy example has no upstream object")
	}
	configuredUpstream["endpoint"] = upstreamEndpoint
	configuredUpstream["bearer_token_env"] = loopbackDemoUpstreamEnvironment

	admission, ok := document["admission"].(map[string]any)
	if !ok {
		return errors.New("loopback demo policy example has no admission object")
	}
	tenants, ok := admission["tenants"].([]any)
	if !ok {
		return errors.New("loopback demo policy example has no tenant list")
	}
	var tenantOne map[string]any
	for _, candidate := range tenants {
		tenant, candidateOK := candidate.(map[string]any)
		if !candidateOK {
			continue
		}
		id, idOK := tenant["id"].(json.Number)
		if idOK && id.String() == "1" {
			tenantOne = tenant
			break
		}
	}
	if tenantOne == nil {
		return errors.New("loopback demo policy example has no tenant one")
	}
	tenantOne["bearer_token_envs"] = []any{loopbackDemoTenantEnvironment}
	admission["tenants"] = []any{tenantOne}

	if !filepath.IsAbs(destination) {
		return errors.New("loopback demo policy destination was not absolute")
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("loopback demo could not create its private policy")
	}
	writeOK := true
	if err := file.Chmod(0o600); err != nil {
		writeOK = false
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if writeOK {
		if err := encoder.Encode(document); err != nil {
			writeOK = false
		}
	}
	if writeOK {
		if err := file.Sync(); err != nil {
			writeOK = false
		}
	}
	if err := file.Close(); err != nil {
		writeOK = false
	}
	if !writeOK {
		return errors.New("loopback demo could not write its private policy")
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode() != 0o600 {
		return errors.New("loopback demo private policy mode was not exact")
	}
	return nil
}

func loopbackDemoChildEnvironment(tenantToken, upstreamToken string) []string {
	current := os.Environ()
	environment := make([]string, 0, len(current)+2)
	for _, entry := range current {
		name, _, _ := strings.Cut(entry, "=")
		if name == loopbackDemoTenantEnvironment ||
			name == loopbackDemoUpstreamEnvironment {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(
		environment,
		loopbackDemoTenantEnvironment+"="+tenantToken,
		loopbackDemoUpstreamEnvironment+"="+upstreamToken,
	)
	return environment
}

func loopbackDemoTCPAddress(listener *net.TCPListener) (*net.TCPAddr, bool) {
	if listener == nil {
		return nil, false
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) ||
		address.Zone != "" || address.Port <= 0 || address.Port > 65_535 {
		return nil, false
	}
	return address, true
}

func (listener *loopbackDemoCountingListener) Accept() (net.Conn, error) {
	if listener == nil || listener.TCPListener == nil {
		return nil, net.ErrClosed
	}
	connection, err := listener.TCPListener.AcceptTCP()
	if err != nil {
		return nil, err
	}
	sequence := listener.accepted.Add(1)
	return &loopbackDemoAcceptedConnection{
		TCPConn:  connection,
		sequence: sequence,
	}, nil
}

func loopbackDemoConnectionContext(ctx context.Context, connection net.Conn) context.Context {
	accepted, ok := connection.(*loopbackDemoAcceptedConnection)
	if !ok || accepted.sequence <= 0 {
		return ctx
	}
	return context.WithValue(
		ctx,
		loopbackDemoConnectionSequenceKey{},
		accepted.sequence,
	)
}

func loopbackDemoAcceptedBeforeBarrier(
	ctx context.Context,
	address string,
	listener *loopbackDemoCountingListener,
	barriers <-chan loopbackDemoAcceptBarrier,
) (connections int, resultErr error) {
	if ctx == nil || listener == nil || listener.TCPListener == nil ||
		barriers == nil {
		return 0, errors.New("loopback demo accept barrier was not configured")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return 0, errors.New("loopback demo accept barrier address was invalid")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		return 0, errors.New("loopback demo accept barrier was not numeric loopback")
	}

	barrierContext, cancelBarrier := context.WithTimeout(ctx, 3*time.Second)
	defer cancelBarrier()
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(barrierContext, "tcp4", address)
	if err != nil {
		return 0, errors.New("loopback demo could not connect its accept barrier")
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) && resultErr == nil {
			connections = 0
			resultErr = errors.New("loopback demo could not close its accept barrier")
		}
	}()
	if deadline, ok := barrierContext.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return 0, errors.New("loopback demo could not bound its accept barrier")
		}
	}

	request := "GET " + loopbackDemoAcceptBarrierPath +
		" HTTP/1.1\r\nHost: " + address +
		"\r\nConnection: close\r\n\r\n"
	written, err := io.WriteString(connection, request)
	if err != nil || written != len(request) {
		return 0, errors.New("loopback demo could not write its accept barrier")
	}
	response, err := http.ReadResponse(
		bufio.NewReader(connection),
		&http.Request{Method: http.MethodGet},
	)
	if err != nil {
		return 0, errors.New("loopback demo could not read its accept barrier")
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) != 0 ||
		response.StatusCode != http.StatusNoContent ||
		response.ProtoMajor != 1 ||
		response.ContentLength != 0 ||
		len(response.TransferEncoding) != 0 {
		return 0, errors.New("loopback demo accept barrier response was not exact")
	}

	var barrier loopbackDemoAcceptBarrier
	select {
	case barrier = <-barriers:
	case <-barrierContext.Done():
		return 0, errors.New("loopback demo accept barrier was not observed")
	}
	accepted := listener.accepted.Load()
	if !barrier.exact || barrier.sequence <= 0 ||
		accepted != barrier.sequence {
		return 0, errors.New("loopback demo accept barrier ordering was not exact")
	}
	return int(barrier.sequence - 1), nil
}

func loopbackDemoWaitForCommand(command *exec.Cmd) *loopbackDemoProcessWait {
	wait := &loopbackDemoProcessWait{done: make(chan struct{})}
	go func() {
		wait.err = command.Wait()
		close(wait.done)
	}()
	return wait
}

func (wait *loopbackDemoProcessWait) finished() bool {
	if wait == nil {
		return false
	}
	select {
	case <-wait.done:
		return true
	default:
		return false
	}
}

func (wait *loopbackDemoProcessWait) wait(timeout time.Duration) bool {
	if wait == nil {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-wait.done:
		return true
	case <-timer.C:
		return false
	}
}

func loopbackDemoWaitForListener(
	ctx context.Context,
	address string,
	process *loopbackDemoProcessWait,
) error {
	waitContext, cancelWait := context.WithTimeout(ctx, 4*time.Second)
	defer cancelWait()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	dialer := net.Dialer{Timeout: 100 * time.Millisecond}
	for {
		if process.finished() {
			return errors.New("loopback demo gateway exited before listener readiness")
		}
		connection, err := dialer.DialContext(waitContext, "tcp4", address)
		if err == nil {
			if closeErr := connection.Close(); closeErr != nil {
				return errors.New("loopback demo could not close its readiness probe")
			}
			return nil
		}
		select {
		case <-waitContext.Done():
			return errors.New("loopback demo gateway listener did not become ready")
		case <-ticker.C:
		}
	}
}

func loopbackDemoWaitForListenerClosure(
	ctx context.Context,
	address string,
	process *loopbackDemoProcessWait,
) error {
	waitContext, cancelWait := context.WithTimeout(ctx, 4*time.Second)
	defer cancelWait()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	dialer := net.Dialer{Timeout: 100 * time.Millisecond}
	for {
		if process.finished() {
			return errors.New("loopback demo gateway exited before draining its stream")
		}
		connection, err := dialer.DialContext(waitContext, "tcp4", address)
		if err != nil {
			if process.finished() {
				return errors.New("loopback demo gateway exited before draining its stream")
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				return nil
			}
		}
		if err == nil {
			if closeErr := connection.Close(); closeErr != nil {
				return errors.New("loopback demo could not close its drain probe")
			}
		}
		select {
		case <-waitContext.Done():
			return errors.New("loopback demo gateway did not enter drain")
		case <-ticker.C:
		}
	}
}

func loopbackDemoNewRequest(
	ctx context.Context,
	endpoint string,
	body string,
	tenantToken string,
) (*http.Request, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		strings.NewReader(body),
	)
	if err != nil {
		return nil, errors.New("loopback demo could not build a client request")
	}
	request.Header.Set("Authorization", "Bearer "+tenantToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(loopbackDemoClientHeader, loopbackDemoClientHeaderValue)
	request.Header.Set("User-Agent", loopbackDemoClientUserAgent)
	return request, nil
}

func loopbackDemoBufferedExchange(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	tenantToken string,
	upstreamToken string,
) (loopbackDemoBufferedResult, error) {
	requestContext, cancelRequest := context.WithTimeout(ctx, 8*time.Second)
	defer cancelRequest()
	request, err := loopbackDemoNewRequest(
		requestContext,
		endpoint,
		loopbackDemoBufferedRequest,
		tenantToken,
	)
	if err != nil {
		return loopbackDemoBufferedResult{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered request failed")
	}
	body, readErr := io.ReadAll(io.LimitReader(
		response.Body,
		int64(len(loopbackDemoBufferedResponse)+1),
	))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response did not close cleanly")
	}
	result := loopbackDemoBufferedResult{
		status:        response.StatusCode,
		protocolMajor: response.ProtoMajor,
		bodyExact:     bytes.Equal(body, []byte(loopbackDemoBufferedResponse)),
		safeHeaders: loopbackDemoSafeBufferedHeaders(
			response.Header,
			response.ContentLength,
			len(loopbackDemoBufferedResponse),
		),
		upstreamCredentialAbsent: !loopbackDemoHeaderContains(
			response.Header,
			upstreamToken,
		) && !bytes.Contains(body, []byte(upstreamToken)),
		upstreamHeadersStripped: len(
			response.Header.Values(loopbackDemoUnsafeHeader),
		) == 0,
	}
	switch {
	case result.status != http.StatusOK || result.protocolMajor != 1:
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response status was not exact")
	case !result.bodyExact:
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response body was not exact")
	case !result.safeHeaders:
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response safety metadata was not exact")
	case !result.upstreamCredentialAbsent:
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response crossed a credential boundary")
	case !result.upstreamHeadersStripped:
		return loopbackDemoBufferedResult{}, errors.New("loopback demo buffered response retained upstream metadata")
	}
	return result, nil
}

func loopbackDemoStreamingExchange(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	gatewayAddress string,
	tenantToken string,
	upstreamToken string,
	upstream *loopbackDemoUpstream,
	process *exec.Cmd,
	processWait *loopbackDemoProcessWait,
) (loopbackDemoStreamResult, error) {
	requestContext, cancelRequest := context.WithTimeout(ctx, 15*time.Second)
	defer cancelRequest()
	request, err := loopbackDemoNewRequest(
		requestContext,
		endpoint,
		loopbackDemoStreamRequest,
		tenantToken,
	)
	if err != nil {
		return loopbackDemoStreamResult{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return loopbackDemoStreamResult{}, errors.New("loopback demo streaming request failed")
	}
	bodyClosed := false
	defer func() {
		if !bodyClosed {
			_ = response.Body.Close()
		}
	}()

	result := loopbackDemoStreamResult{
		status:        response.StatusCode,
		protocolMajor: response.ProtoMajor,
		safeHeaders: loopbackDemoSafeStreamHeaders(
			response.Header,
			response.ContentLength,
			response.TransferEncoding,
		),
		upstreamCredentialAbsent: !loopbackDemoHeaderContains(
			response.Header,
			upstreamToken,
		),
		upstreamHeadersStripped: len(
			response.Header.Values(loopbackDemoUnsafeHeader),
		) == 0,
	}
	if result.status != http.StatusOK || result.protocolMajor != 1 ||
		!result.safeHeaders || !result.upstreamCredentialAbsent ||
		!result.upstreamHeadersStripped {
		return loopbackDemoStreamResult{}, errors.New("loopback demo streaming response metadata was not exact")
	}

	first := make([]byte, len(loopbackDemoChunkOne))
	if _, err := io.ReadFull(response.Body, first); err != nil ||
		!bytes.Equal(first, []byte(loopbackDemoChunkOne)) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo did not receive the exact first stream event")
	}
	if bytes.Contains(first, []byte(upstreamToken)) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo first stream event crossed a credential boundary")
	}
	select {
	case <-upstream.chunkOneFlushed:
	case <-requestContext.Done():
		return loopbackDemoStreamResult{}, errors.New("loopback demo upstream did not flush its first event")
	}
	if upstream.released.Load() {
		return loopbackDemoStreamResult{}, errors.New("loopback demo upstream released before the first event arrived")
	}
	result.firstChunkBeforeRelease = true

	if process == nil || process.Process == nil || processWait.finished() {
		return loopbackDemoStreamResult{}, errors.New("loopback demo gateway was unavailable for shutdown")
	}
	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		return loopbackDemoStreamResult{}, errors.New("loopback demo could not signal the gateway")
	}
	if err := loopbackDemoWaitForListenerClosure(
		requestContext,
		gatewayAddress,
		processWait,
	); err != nil {
		return loopbackDemoStreamResult{}, err
	}
	upstream.release()

	second := make([]byte, len(loopbackDemoChunkTwo))
	if _, err := io.ReadFull(response.Body, second); err != nil ||
		!bytes.Equal(second, []byte(loopbackDemoChunkTwo)) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo did not receive the exact second stream event")
	}
	done := make([]byte, len(loopbackDemoDone))
	if _, err := io.ReadFull(response.Body, done); err != nil ||
		!bytes.Equal(done, []byte(loopbackDemoDone)) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo did not receive the exact terminal stream event")
	}
	trailing := make([]byte, 1)
	n, readErr := response.Body.Read(trailing)
	if n != 0 || !errors.Is(readErr, io.EOF) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo stream did not end at a clean boundary")
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		return loopbackDemoStreamResult{}, errors.New("loopback demo stream body did not close cleanly")
	}
	bodyClosed = true
	if bytes.Contains(second, []byte(upstreamToken)) ||
		bytes.Contains(done, []byte(upstreamToken)) {
		return loopbackDemoStreamResult{}, errors.New("loopback demo stream crossed a credential boundary")
	}
	select {
	case succeeded := <-upstream.streamResult:
		if !succeeded {
			return loopbackDemoStreamResult{}, errors.New("loopback demo upstream stream did not finish cleanly")
		}
	case <-requestContext.Done():
		return loopbackDemoStreamResult{}, errors.New("loopback demo upstream stream did not finish")
	}

	result.events = []string{"chunk-1", "chunk-2", "[DONE]", "clean-eof"}
	result.inflightCompleted = true
	return result, nil
}

func loopbackDemoAwaitCheck(
	ctx context.Context,
	checks <-chan loopbackDemoUpstreamCheck,
	wantCall int,
) (loopbackDemoUpstreamCheck, error) {
	waitContext, cancelWait := context.WithTimeout(ctx, 3*time.Second)
	defer cancelWait()
	select {
	case check := <-checks:
		if check.call != wantCall {
			return loopbackDemoUpstreamCheck{}, errors.New("loopback demo observed an unexpected upstream call order")
		}
		return check, nil
	case <-waitContext.Done():
		return loopbackDemoUpstreamCheck{}, errors.New("loopback demo did not observe the upstream request")
	}
}

func (upstream *loopbackDemoUpstream) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	if upstream.serveAcceptBarrier(writer, request) {
		return
	}
	call := int(upstream.calls.Add(1))
	expectedBody := ""
	expectedAccept := ""
	switch call {
	case 1:
		expectedBody = loopbackDemoBufferedRequest
		expectedAccept = "application/json"
	case 2:
		expectedBody = loopbackDemoStreamRequest
		expectedAccept = "text/event-stream"
	}

	body, readErr := io.ReadAll(io.LimitReader(
		request.Body,
		int64(len(expectedBody)+1),
	))
	closeErr := request.Body.Close()
	tenantAbsent := !loopbackDemoHeaderContains(request.Header, upstream.tenantToken) &&
		!bytes.Contains(body, []byte(upstream.tenantToken))
	clientHeadersAbsent :=
		len(request.Header.Values(loopbackDemoClientHeader)) == 0 &&
			!loopbackDemoHeaderContains(request.Header, loopbackDemoClientHeaderValue) &&
			!loopbackDemoHeaderContains(request.Header, loopbackDemoClientUserAgent)
	headerExact := loopbackDemoExactUpstreamHeaders(
		request.Header,
		expectedAccept,
		upstream.upstreamToken,
		len(expectedBody),
	)
	requestExact := expectedBody != "" &&
		readErr == nil &&
		closeErr == nil &&
		request.Method == http.MethodPost &&
		request.URL != nil &&
		request.URL.Path == loopbackDemoChatPath &&
		request.URL.RawQuery == "" &&
		!request.URL.ForceQuery &&
		request.ProtoMajor == 1 &&
		request.Host == upstream.host &&
		request.ContentLength == int64(len(expectedBody)) &&
		len(request.TransferEncoding) == 0 &&
		bytes.Equal(body, []byte(expectedBody))
	separateCredential := upstream.tenantToken != upstream.upstreamToken &&
		request.Header.Get("Authorization") == "Bearer "+upstream.upstreamToken &&
		tenantAbsent
	check := loopbackDemoUpstreamCheck{
		call:                   call,
		requestExact:           requestExact,
		headerAllowlistExact:   headerExact,
		tenantCredentialAbsent: tenantAbsent,
		clientHeadersAbsent:    clientHeadersAbsent,
		separateCredential:     separateCredential,
	}
	select {
	case upstream.checks <- check:
	default:
	}
	if !requestExact || !headerExact || !tenantAbsent ||
		!clientHeadersAbsent || !separateCredential {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set(loopbackDemoUnsafeHeader, "Bearer "+upstream.upstreamToken)
	switch call {
	case 1:
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, loopbackDemoBufferedResponse)
	case 2:
		upstream.serveStream(writer, request)
	default:
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func (upstream *loopbackDemoUpstream) serveAcceptBarrier(
	writer http.ResponseWriter,
	request *http.Request,
) bool {
	if request == nil || request.URL == nil ||
		request.URL.Path != loopbackDemoAcceptBarrierPath {
		return false
	}
	body, readErr := io.ReadAll(io.LimitReader(request.Body, 1))
	closeErr := request.Body.Close()
	sequence, sequenceOK := request.Context().Value(
		loopbackDemoConnectionSequenceKey{},
	).(int64)
	exact := sequenceOK &&
		sequence > 0 &&
		readErr == nil &&
		closeErr == nil &&
		len(body) == 0 &&
		request.Method == http.MethodGet &&
		request.URL.RawQuery == "" &&
		!request.URL.ForceQuery &&
		request.ProtoMajor == 1 &&
		request.Host == upstream.host &&
		request.ContentLength == 0 &&
		len(request.TransferEncoding) == 0 &&
		request.Close &&
		request.Header.Get("Authorization") == "" &&
		!loopbackDemoHeaderContains(request.Header, upstream.tenantToken) &&
		!loopbackDemoHeaderContains(request.Header, upstream.upstreamToken)
	select {
	case upstream.acceptBarrier <- loopbackDemoAcceptBarrier{
		sequence: sequence,
		exact:    exact,
	}:
	default:
		exact = false
	}
	if !exact {
		writer.WriteHeader(http.StatusBadRequest)
		return true
	}
	writer.WriteHeader(http.StatusNoContent)
	return true
}

func (upstream *loopbackDemoUpstream) serveStream(
	writer http.ResponseWriter,
	request *http.Request,
) {
	succeeded := false
	defer func() {
		select {
		case upstream.streamResult <- succeeded:
		default:
		}
	}()
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(writer, loopbackDemoChunkOne); err != nil {
		return
	}
	if err := http.NewResponseController(writer).Flush(); err != nil {
		return
	}
	select {
	case upstream.chunkOneFlushed <- struct{}{}:
	default:
	}
	select {
	case <-upstream.releaseStream:
	case <-request.Context().Done():
		return
	}
	if _, err := io.WriteString(writer, loopbackDemoChunkTwo); err != nil {
		return
	}
	if err := http.NewResponseController(writer).Flush(); err != nil {
		return
	}
	if _, err := io.WriteString(writer, loopbackDemoDone); err != nil {
		return
	}
	if err := http.NewResponseController(writer).Flush(); err != nil {
		return
	}
	succeeded = true
}

func (upstream *loopbackDemoUpstream) release() {
	upstream.releaseOnce.Do(func() {
		upstream.released.Store(true)
		close(upstream.releaseStream)
	})
}

func loopbackDemoExactUpstreamHeaders(
	header http.Header,
	accept string,
	upstreamToken string,
	contentLength int,
) bool {
	if !loopbackDemoSingleHeader(
		header,
		"Content-Length",
		strconv.Itoa(contentLength),
	) {
		return false
	}
	contentHeaders := header.Clone()
	contentHeaders.Del("Content-Length")
	want := http.Header{
		"Accept":        []string{accept},
		"Authorization": []string{"Bearer " + upstreamToken},
		"Content-Type":  []string{"application/json"},
		"User-Agent":    []string{"ssemaphore"},
	}
	return reflect.DeepEqual(contentHeaders, want)
}

func loopbackDemoSafeBufferedHeaders(
	header http.Header,
	contentLength int64,
	wantLength int,
) bool {
	allowed := map[string]struct{}{
		"Cache-Control":          {},
		"Content-Length":         {},
		"Content-Type":           {},
		"Date":                   {},
		"X-Content-Type-Options": {},
		"X-Request-Id":           {},
	}
	return loopbackDemoHeaderNamesExact(header, allowed) &&
		loopbackDemoSingleHeader(header, "Cache-Control", "no-store") &&
		loopbackDemoSingleHeader(header, "Content-Length", strconv.Itoa(wantLength)) &&
		loopbackDemoSingleHeader(header, "Content-Type", "application/json") &&
		loopbackDemoSingleNonemptyHeader(header, "Date") &&
		loopbackDemoSingleHeader(header, "X-Content-Type-Options", "nosniff") &&
		loopbackDemoValidRequestIDHeader(header) &&
		contentLength == int64(wantLength)
}

func loopbackDemoSafeStreamHeaders(
	header http.Header,
	contentLength int64,
	transferEncoding []string,
) bool {
	allowed := map[string]struct{}{
		"Cache-Control":          {},
		"Content-Type":           {},
		"Date":                   {},
		"X-Content-Type-Options": {},
		"X-Request-Id":           {},
	}
	return loopbackDemoHeaderNamesExact(header, allowed) &&
		loopbackDemoSingleHeader(header, "Cache-Control", "no-store") &&
		loopbackDemoSingleHeader(header, "Content-Type", "text/event-stream") &&
		loopbackDemoSingleNonemptyHeader(header, "Date") &&
		loopbackDemoSingleHeader(header, "X-Content-Type-Options", "nosniff") &&
		loopbackDemoValidRequestIDHeader(header) &&
		contentLength == -1 &&
		len(transferEncoding) == 1 &&
		transferEncoding[0] == "chunked"
}

func loopbackDemoHeaderNamesExact(
	header http.Header,
	allowed map[string]struct{},
) bool {
	if len(header) != len(allowed) {
		return false
	}
	for name := range header {
		if _, ok := allowed[http.CanonicalHeaderKey(name)]; !ok {
			return false
		}
	}
	return true
}

func loopbackDemoSingleHeader(header http.Header, name, value string) bool {
	values := header.Values(name)
	return len(values) == 1 && values[0] == value
}

func loopbackDemoSingleNonemptyHeader(header http.Header, name string) bool {
	values := header.Values(name)
	return len(values) == 1 && values[0] != ""
}

func loopbackDemoValidRequestIDHeader(header http.Header) bool {
	values := header.Values("X-Request-Id")
	if len(values) != 1 || len(values[0]) != 32 {
		return false
	}
	decoded := make([]byte, 16)
	n, err := hex.Decode(decoded, []byte(values[0]))
	return err == nil && n == len(decoded)
}

func loopbackDemoHeaderContains(header http.Header, value string) bool {
	if value == "" {
		return false
	}
	for name, values := range header {
		if strings.Contains(name, value) {
			return true
		}
		for _, candidate := range values {
			if strings.Contains(candidate, value) {
				return true
			}
		}
	}
	return false
}
