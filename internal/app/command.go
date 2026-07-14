package app

import (
	"io"
	"net"
	"os"
)

const (
	usageText = `Usage:
  ssemaphore validate --config /absolute/path/policy.json
  ssemaphore serve --config /absolute/path/policy.json
`
	configurationRejectedMessage = "configuration rejected\n"
	validationFailedMessage      = "gateway validation failed\n"
	startupFailedMessage         = "gateway startup failed\n"
	runtimeFailedMessage         = "gateway runtime failed\n"
	validationSucceededMessage   = "gateway policy is valid\n"
)

const (
	exitSuccess = 0
	exitRuntime = 1
	exitUsage   = 2
)

type commandMode uint8

const (
	commandValidate commandMode = iota + 1
	commandServe
)

type commandDependencies struct {
	secrets   secretSource
	listenTCP listenTCPFunc
	subscribe func() (<-chan os.Signal, func(), error)
}

// Main executes one strict command invocation and returns a process exit code.
// It never exits the process itself, so every owned resource is cleaned before
// cmd/ssemaphore calls os.Exit.
func Main(args []string, stdout, stderr io.Writer) int {
	return runCommand(args, stdout, stderr, commandDependencies{
		secrets:   systemSecretSource{},
		listenTCP: net.ListenTCP,
		subscribe: subscribeTerminationSignals,
	})
}

func runCommand(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	dependencies commandDependencies,
) int {
	mode, path, help, valid := parseCommand(args)
	if help {
		writeStatic(stdout, usageText)
		return exitSuccess
	}
	if !valid {
		writeStatic(stderr, usageText)
		return exitUsage
	}

	policy, err := loadPolicy(path)
	if err != nil {
		writeStatic(stderr, configurationRejectedMessage)
		return exitUsage
	}
	prepared, err := prepareGateway(policy, dependencies.secrets)
	if err != nil {
		writeStatic(stderr, validationFailedMessage)
		return exitUsage
	}

	if mode == commandValidate {
		if err := prepared.close(); err != nil {
			writeStatic(stderr, runtimeFailedMessage)
			return exitRuntime
		}
		writeStatic(stdout, validationSucceededMessage)
		return exitSuccess
	}

	if dependencies.listenTCP == nil || dependencies.subscribe == nil {
		_ = prepared.close()
		writeStatic(stderr, startupFailedMessage)
		return exitRuntime
	}
	events, stop, err := dependencies.subscribe()
	if err != nil || events == nil || stop == nil {
		if stop != nil {
			stop()
		}
		_ = prepared.close()
		writeStatic(stderr, startupFailedMessage)
		return exitRuntime
	}

	runtime, err := prepared.start(dependencies.listenTCP)
	if err != nil {
		stop()
		writeStatic(stderr, startupFailedMessage)
		return exitRuntime
	}
	if supervise(runtime, events, stop, prepared.shutdownWait) != outcomeCleanShutdown {
		writeStatic(stderr, runtimeFailedMessage)
		return exitRuntime
	}
	return exitSuccess
}

func parseCommand(args []string) (commandMode, string, bool, bool) {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "help") {
		return 0, "", true, true
	}
	if len(args) != 3 || args[1] != "--config" || args[2] == "" {
		return 0, "", false, false
	}
	switch args[0] {
	case "validate":
		return commandValidate, args[2], false, true
	case "serve":
		return commandServe, args[2], false, true
	default:
		return 0, "", false, false
	}
}

func writeStatic(destination io.Writer, message string) {
	if destination == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	_, _ = io.WriteString(destination, message)
}
