package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestParseCommandAcceptsOnlyExactGrammar(t *testing.T) {
	const path = "/private/config-canary.json"
	tests := []struct {
		name  string
		args  []string
		mode  commandMode
		path  string
		help  bool
		valid bool
	}{
		{name: "help flag", args: []string{"--help"}, help: true, valid: true},
		{name: "help command", args: []string{"help"}, help: true, valid: true},
		{name: "validate", args: []string{"validate", "--config", path}, mode: commandValidate, path: path, valid: true},
		{name: "serve", args: []string{"serve", "--config", path}, mode: commandServe, path: path, valid: true},
		{name: "nil"},
		{name: "empty argument", args: []string{""}},
		{name: "short help", args: []string{"-h"}},
		{name: "help with operand", args: []string{"help", path}},
		{name: "missing path", args: []string{"serve", "--config"}},
		{name: "empty path", args: []string{"serve", "--config", ""}},
		{name: "wrong flag", args: []string{"serve", "--policy", path}},
		{name: "wrong order", args: []string{"--config", path, "serve"}},
		{name: "unknown command", args: []string{"CONFIG_CANARY_COMMAND", "--config", path}},
		{name: "case mismatch", args: []string{"Serve", "--config", path}},
		{name: "extra argument", args: []string{"serve", "--config", path, "ARGUMENT_CANARY"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mode, gotPath, help, valid := parseCommand(test.args)
			if mode != test.mode || gotPath != test.path || help != test.help || valid != test.valid {
				t.Fatalf(
					"parseCommand() = (%v, %q, %v, %v), want (%v, %q, %v, %v)",
					mode,
					gotPath,
					help,
					valid,
					test.mode,
					test.path,
					test.help,
					test.valid,
				)
			}
		})
	}
}

func TestRunCommandRejectsMalformedCLIWithoutEchoingArguments(t *testing.T) {
	const canary = "MALFORMED_CLI_PRIVATE_CANARY"
	tests := [][]string{
		nil,
		{},
		{canary},
		{"serve", "--config", "/private/" + canary, canary},
		{canary, "--config", "/private/policy.json"},
		{"validate", canary, "/private/policy.json"},
	}

	for index, args := range tests {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runCommand(args, &stdout, &stderr, commandDependencies{})
			if code != exitUsage {
				t.Fatalf("runCommand() exit = %d, want %d", code, exitUsage)
			}
			if stdout.String() != "" || stderr.String() != usageText {
				t.Fatalf("runCommand() output = (%q, %q), want (empty, exact usage)", stdout.String(), stderr.String())
			}
			commandTestAssertNoCanary(t, canary, stdout.String(), stderr.String())
		})
	}
}

func TestRunCommandValidateConsumesSecretsWithoutSubscribingOrListening(t *testing.T) {
	const canary = "VALIDATE_PRIVATE_CANARY"
	path := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))
	source := commandTestSecrets(canary)
	var listenCalls atomic.Int32
	var subscribeCalls atomic.Int32
	dependencies := commandDependencies{
		secrets: source,
		listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
			listenCalls.Add(1)
			return nil, errors.New(canary)
		},
		subscribe: func() (<-chan os.Signal, func(), error) {
			subscribeCalls.Add(1)
			return nil, nil, errors.New(canary)
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runCommand([]string{"validate", "--config", path}, &stdout, &stderr, dependencies)

	if code != exitSuccess {
		t.Fatalf("runCommand(validate) exit = %d, want %d", code, exitSuccess)
	}
	if stdout.String() != validationSucceededMessage || stderr.String() != "" {
		t.Fatalf("runCommand(validate) output = (%q, %q)", stdout.String(), stderr.String())
	}
	if listenCalls.Load() != 0 || subscribeCalls.Load() != 0 {
		t.Fatalf("validate dependency calls = (listen %d, subscribe %d), want zero", listenCalls.Load(), subscribeCalls.Load())
	}
	commandTestAssertSecretsConsumed(t, source)
	commandTestAssertNoCanary(t, canary, path, stdout.String(), stderr.String())
}

func TestRunCommandConfigurationFailuresAreStatic(t *testing.T) {
	const canary = "CONFIGURATION_FAILURE_PRIVATE_CANARY"
	validPath := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))
	invalidPolicyPath := writePolicyFixture(t, []byte(`{"UNKNOWN_`+canary+`":"`+canary+`"}`))
	missingPath := "/private/" + canary + "/policy.json"

	tests := []struct {
		name    string
		path    string
		source  *fakeSecretSource
		message string
	}{
		{
			name:    "unreadable file",
			path:    missingPath,
			source:  commandTestSecrets(canary),
			message: configurationRejectedMessage,
		},
		{
			name:    "rejected policy",
			path:    invalidPolicyPath,
			source:  commandTestSecrets(canary),
			message: configurationRejectedMessage,
		},
		{
			name: "missing secret",
			path: validPath,
			source: &fakeSecretSource{values: map[string]string{
				"SSEMAPHORE_TENANT_1_PRIMARY":      "tenant-one-" + canary,
				"SSEMAPHORE_TENANT_2_PRIMARY":      "tenant-two-" + canary,
				"SSEMAPHORE_UPSTREAM_BEARER_TOKEN": "upstream-" + canary,
			}},
			message: validationFailedMessage,
		},
		{
			name: "invalid secret",
			path: validPath,
			source: &fakeSecretSource{values: map[string]string{
				"SSEMAPHORE_TENANT_1_PRIMARY":      "invalid token " + canary,
				"SSEMAPHORE_TENANT_1_ROTATION":     "tenant-rotation-" + canary,
				"SSEMAPHORE_TENANT_2_PRIMARY":      "tenant-two-" + canary,
				"SSEMAPHORE_UPSTREAM_BEARER_TOKEN": "upstream-" + canary,
			}},
			message: validationFailedMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var listenCalls atomic.Int32
			var subscribeCalls atomic.Int32
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runCommand(
				[]string{"serve", "--config", test.path},
				&stdout,
				&stderr,
				commandDependencies{
					secrets: test.source,
					listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
						listenCalls.Add(1)
						return nil, errors.New(canary)
					},
					subscribe: func() (<-chan os.Signal, func(), error) {
						subscribeCalls.Add(1)
						return nil, nil, errors.New(canary)
					},
				},
			)
			if code != exitUsage {
				t.Fatalf("runCommand() exit = %d, want %d", code, exitUsage)
			}
			if stdout.String() != "" || stderr.String() != test.message {
				t.Fatalf("runCommand() output = (%q, %q), want (empty, %q)", stdout.String(), stderr.String(), test.message)
			}
			if listenCalls.Load() != 0 || subscribeCalls.Load() != 0 {
				t.Fatalf("failed configuration reached runtime dependencies: (listen %d, subscribe %d)", listenCalls.Load(), subscribeCalls.Load())
			}
			commandTestAssertNoCanary(t, canary, stdout.String(), stderr.String())
		})
	}
}

func TestRunCommandSubscriptionFailureStopsAndReturnsAfterPreparedCleanup(t *testing.T) {
	const canary = "SUBSCRIPTION_FAILURE_PRIVATE_CANARY"
	path := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))
	source := commandTestSecrets(canary)
	var stopCalls atomic.Int32
	var listenCalls atomic.Int32

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runCommand(
		[]string{"serve", "--config", path},
		&stdout,
		&stderr,
		commandDependencies{
			secrets: source,
			listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
				listenCalls.Add(1)
				return nil, errors.New(canary)
			},
			subscribe: func() (<-chan os.Signal, func(), error) {
				return make(chan os.Signal), func() { stopCalls.Add(1) }, errors.New(canary)
			},
		},
	)

	if code != exitRuntime {
		t.Fatalf("runCommand() exit = %d, want %d", code, exitRuntime)
	}
	if stdout.String() != "" || stderr.String() != startupFailedMessage {
		t.Fatalf("runCommand() output = (%q, %q)", stdout.String(), stderr.String())
	}
	if stopCalls.Load() != 1 || listenCalls.Load() != 0 {
		t.Fatalf("subscription failure calls = (stop %d, listen %d), want (1, 0)", stopCalls.Load(), listenCalls.Load())
	}
	commandTestAssertSecretsConsumed(t, source)
	commandTestAssertNoCanary(t, canary, path, stdout.String(), stderr.String())
}

func TestRunCommandListenerFailureStopsExactlyOnceAndIsStatic(t *testing.T) {
	const canary = "LISTENER_FAILURE_PRIVATE_CANARY"
	document := canonicalPolicyDocument()
	document.Listener.Port = 43_219
	path := writePolicyFixture(t, marshalPolicyDocument(t, document))
	source := commandTestSecrets(canary)
	var stopCalls atomic.Int32
	var listenCalls atomic.Int32
	events := make(chan os.Signal)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runCommand(
		[]string{"serve", "--config", path},
		&stdout,
		&stderr,
		commandDependencies{
			secrets: source,
			listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
				listenCalls.Add(1)
				return nil, errors.New(canary)
			},
			subscribe: func() (<-chan os.Signal, func(), error) {
				return events, func() { stopCalls.Add(1) }, nil
			},
		},
	)

	if code != exitRuntime {
		t.Fatalf("runCommand() exit = %d, want %d", code, exitRuntime)
	}
	if stdout.String() != "" || stderr.String() != startupFailedMessage {
		t.Fatalf("runCommand() output = (%q, %q)", stdout.String(), stderr.String())
	}
	if stopCalls.Load() != 1 || listenCalls.Load() != 1 {
		t.Fatalf("listener failure calls = (stop %d, listen %d), want (1, 1)", stopCalls.Load(), listenCalls.Load())
	}
	commandTestAssertSecretsConsumed(t, source)
	commandTestAssertNoCanary(t, canary, path, stdout.String(), stderr.String())
}

func TestRunCommandServeTerminationCleansRuntimeAndReleasesPort(t *testing.T) {
	commandTestRunServeLifecycle(t, true)
}

func TestRunCommandClosedSignalChannelReportsRuntimeFailureAfterCleanup(t *testing.T) {
	commandTestRunServeLifecycle(t, false)
}

func TestRunCommandRejectsNilDependenciesBeforeRuntime(t *testing.T) {
	const canary = "NIL_DEPENDENCY_PRIVATE_CANARY"
	path := writePolicyFixture(t, marshalPolicyDocument(t, canonicalPolicyDocument()))

	t.Run("nil secret source", func(t *testing.T) {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runCommand(
			[]string{"serve", "--config", path},
			&stdout,
			&stderr,
			commandDependencies{
				listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
					panic("listener reached with nil secret source")
				},
				subscribe: func() (<-chan os.Signal, func(), error) {
					panic("subscription reached with nil secret source")
				},
			},
		)
		if code != exitUsage || stdout.String() != "" || stderr.String() != validationFailedMessage {
			t.Fatalf("nil secret source result = (%d, %q, %q)", code, stdout.String(), stderr.String())
		}
	})

	tests := []struct {
		name         string
		dependencies func(*fakeSecretSource, *atomic.Int32) commandDependencies
	}{
		{
			name: "nil listener",
			dependencies: func(source *fakeSecretSource, runtimeCalls *atomic.Int32) commandDependencies {
				return commandDependencies{
					secrets: source,
					subscribe: func() (<-chan os.Signal, func(), error) {
						runtimeCalls.Add(1)
						return nil, nil, errors.New(canary)
					},
				}
			},
		},
		{
			name: "nil subscription",
			dependencies: func(source *fakeSecretSource, runtimeCalls *atomic.Int32) commandDependencies {
				return commandDependencies{
					secrets: source,
					listenTCP: func(string, *net.TCPAddr) (*net.TCPListener, error) {
						runtimeCalls.Add(1)
						return nil, errors.New(canary)
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := commandTestSecrets(canary)
			var runtimeCalls atomic.Int32
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runCommand(
				[]string{"serve", "--config", path},
				&stdout,
				&stderr,
				test.dependencies(source, &runtimeCalls),
			)
			if code != exitRuntime || stdout.String() != "" || stderr.String() != startupFailedMessage {
				t.Fatalf("nil dependency result = (%d, %q, %q)", code, stdout.String(), stderr.String())
			}
			if runtimeCalls.Load() != 0 {
				t.Fatalf("runtime dependency calls = %d, want zero", runtimeCalls.Load())
			}
			commandTestAssertSecretsConsumed(t, source)
			commandTestAssertNoCanary(t, canary, path, stdout.String(), stderr.String())
		})
	}
}

func TestRunCommandWriterFailuresDoNotChangeExitSemantics(t *testing.T) {
	const canary = "WRITER_FAILURE_PRIVATE_CANARY"
	tests := []struct {
		name   string
		args   []string
		stdout *commandTestFailingWriter
		stderr *commandTestFailingWriter
		want   int
	}{
		{
			name:   "help write error",
			args:   []string{"--help"},
			stdout: &commandTestFailingWriter{err: errors.New(canary)},
			want:   exitSuccess,
		},
		{
			name:   "usage write error",
			args:   []string{canary},
			stderr: &commandTestFailingWriter{err: errors.New(canary)},
			want:   exitUsage,
		},
		{
			name:   "help writer panic",
			args:   []string{"help"},
			stdout: &commandTestFailingWriter{panicValue: canary},
			want:   exitSuccess,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code := runCommand(test.args, test.stdout, test.stderr, commandDependencies{})
			if code != test.want {
				t.Fatalf("runCommand() exit = %d, want %d", code, test.want)
			}
			writer := test.stdout
			if writer == nil {
				writer = test.stderr
			}
			if writer.writes.Load() != 1 {
				t.Fatalf("writer calls = %d, want 1", writer.writes.Load())
			}
		})
	}

	if code := runCommand([]string{"--help"}, nil, nil, commandDependencies{}); code != exitSuccess {
		t.Fatalf("runCommand(help, nil writers) exit = %d, want %d", code, exitSuccess)
	}
	if code := runCommand([]string{canary}, nil, nil, commandDependencies{}); code != exitUsage {
		t.Fatalf("runCommand(invalid, nil writers) exit = %d, want %d", code, exitUsage)
	}
}

func TestMainHelpHasNoEnvironmentOrSignalSideEffects(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Main([]string{"--help"}, &stdout, &stderr)
	if code != exitSuccess || stdout.String() != usageText || stderr.String() != "" {
		t.Fatalf("Main(--help) = (%d, %q, %q)", code, stdout.String(), stderr.String())
	}
}

func commandTestRunServeLifecycle(t *testing.T, cleanSignal bool) {
	t.Helper()
	const canary = "SERVE_LIFECYCLE_PRIVATE_CANARY"
	reservation, port := commandTestReserveLoopbackListener(t)
	document := canonicalPolicyDocument()
	document.Listener.Host = "127.0.0.1"
	document.Listener.Port = uint64(port)
	path := writePolicyFixture(t, marshalPolicyDocument(t, document))
	source := commandTestSecrets(canary)

	events := make(chan os.Signal, 1)
	if cleanSignal {
		events <- syscall.SIGTERM
	} else {
		close(events)
	}
	var stopCalls atomic.Int32
	var listenCalls atomic.Int32
	var requestedNetwork string
	var requestedAddress *net.TCPAddr
	var ownedListener *net.TCPListener
	listen := func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
		listenCalls.Add(1)
		requestedNetwork = network
		if address != nil {
			requestedAddress = &net.TCPAddr{
				IP:   append(net.IP(nil), address.IP...),
				Port: address.Port,
				Zone: address.Zone,
			}
		}
		ownedListener = reservation
		return reservation, nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	result := make(chan int, 1)
	go func() {
		result <- runCommand(
			[]string{"serve", "--config", path},
			&stdout,
			&stderr,
			commandDependencies{
				secrets:   source,
				listenTCP: listen,
				subscribe: func() (<-chan os.Signal, func(), error) {
					return events, func() { stopCalls.Add(1) }, nil
				},
			},
		)
	}()

	watchdog, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var code int
	select {
	case code = <-result:
	case <-watchdog.Done():
		t.Fatal("runCommand(serve) did not complete within the watchdog")
	}

	wantCode := exitRuntime
	wantStderr := runtimeFailedMessage
	if cleanSignal {
		wantCode = exitSuccess
		wantStderr = ""
	}
	if code != wantCode || stdout.String() != "" || stderr.String() != wantStderr {
		t.Fatalf("runCommand(serve) = (%d, %q, %q), want (%d, empty, %q)", code, stdout.String(), stderr.String(), wantCode, wantStderr)
	}
	if listenCalls.Load() != 1 || stopCalls.Load() != 1 {
		t.Fatalf("serve lifecycle calls = (listen %d, stop %d), want (1, 1)", listenCalls.Load(), stopCalls.Load())
	}
	if requestedNetwork != "tcp4" || requestedAddress == nil ||
		!requestedAddress.IP.Equal(net.IPv4(127, 0, 0, 1)) || requestedAddress.Port != int(port) || requestedAddress.Zone != "" {
		t.Fatalf("listener request = (%q, %#v), want exact reserved IPv4 loopback", requestedNetwork, requestedAddress)
	}
	if ownedListener == nil {
		t.Fatal("serve lifecycle did not create its loopback listener")
	}
	if err := ownedListener.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("owned listener remains open after command return: SetDeadline error = %v", err)
	}
	commandTestAssertSecretsConsumed(t, source)
	commandTestAssertNoCanary(
		t,
		canary,
		stdout.String(),
		stderr.String(),
		path,
		strconv.Itoa(int(port)),
	)

	rebound, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: int(port),
	})
	if err != nil {
		t.Fatalf("released loopback port could not be rebound: %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatalf("close rebound loopback listener: %v", err)
	}
}

func commandTestReserveLoopbackListener(t *testing.T) (*net.TCPListener, uint16) {
	t.Helper()
	reservation, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	address, ok := reservation.Addr().(*net.TCPAddr)
	if !ok || address == nil || address.Port <= 0 || address.Port > 65_535 {
		_ = reservation.Close()
		t.Fatalf("reserved address = %#v, want bounded TCP port", reservation.Addr())
	}
	t.Cleanup(func() {
		_ = reservation.Close()
	})
	return reservation, uint16(address.Port)
}

func commandTestSecrets(canary string) *fakeSecretSource {
	return &fakeSecretSource{values: map[string]string{
		"SSEMAPHORE_TENANT_1_PRIMARY":      "tenant-one-" + canary,
		"SSEMAPHORE_TENANT_1_ROTATION":     "tenant-rotation-" + canary,
		"SSEMAPHORE_TENANT_2_PRIMARY":      "tenant-two-" + canary,
		"SSEMAPHORE_UPSTREAM_BEARER_TOKEN": "upstream-" + canary,
	}}
}

func commandTestAssertSecretsConsumed(t *testing.T, source *fakeSecretSource) {
	t.Helper()
	want := []string{
		"lookup:SSEMAPHORE_TENANT_1_PRIMARY",
		"unset:SSEMAPHORE_TENANT_1_PRIMARY",
		"lookup:SSEMAPHORE_TENANT_1_ROTATION",
		"unset:SSEMAPHORE_TENANT_1_ROTATION",
		"lookup:SSEMAPHORE_TENANT_2_PRIMARY",
		"unset:SSEMAPHORE_TENANT_2_PRIMARY",
		"lookup:SSEMAPHORE_UPSTREAM_BEARER_TOKEN",
		"unset:SSEMAPHORE_UPSTREAM_BEARER_TOKEN",
	}
	if fmt.Sprint(source.events) != fmt.Sprint(want) {
		t.Fatalf("secret source events = %v, want exact full consumption %v", source.events, want)
	}
}

func commandTestAssertNoCanary(t *testing.T, canary string, values ...string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(value, canary) {
			t.Fatalf("command output disclosed canary %q: %q", canary, value)
		}
	}
}

type commandTestFailingWriter struct {
	err        error
	panicValue any
	writes     atomic.Int32
}

func (writer *commandTestFailingWriter) Write([]byte) (int, error) {
	writer.writes.Add(1)
	if writer.panicValue != nil {
		panic(writer.panicValue)
	}
	return 0, writer.err
}
