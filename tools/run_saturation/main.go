package main

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	saturationUsage = `Usage:
  go run ./tools/run_saturation --profile=ci --seed=<unsigned-integer>
`
	saturationFailure = "saturation harness failed\n"
)

type commandOptions struct {
	profile string
	seed    uint64
}

func main() {
	os.Exit(runCommand(os.Args[1:], os.Stdout, os.Stderr))
}

func runCommand(args []string, stdout, stderr io.Writer) int {
	options, help, valid := parseArguments(args)
	if help {
		writeStatic(stdout, saturationUsage)
		return 0
	}
	if !valid {
		writeStatic(stderr, saturationUsage)
		return 2
	}
	profile, err := profileByName(options.profile)
	if err != nil {
		writeStatic(stderr, saturationFailure)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), profile.executionTimeout)
	report, err := runSaturationHarness(ctx, profile, options.seed)
	cancel()
	if err != nil {
		writeStatic(stderr, saturationFailure)
		return 1
	}
	encoded, err := marshalReport(report)
	if err != nil || !writeExact(stdout, encoded) {
		writeStatic(stderr, saturationFailure)
		return 1
	}
	return 0
}

func parseArguments(args []string) (commandOptions, bool, bool) {
	if len(args) == 1 && args[0] == "--help" {
		return commandOptions{}, true, true
	}
	if len(args) != 2 {
		return commandOptions{}, false, false
	}

	options := commandOptions{}
	profileSeen := false
	seedSeen := false
	for _, argument := range args {
		switch {
		case strings.HasPrefix(argument, "--profile="):
			value := strings.TrimPrefix(argument, "--profile=")
			if profileSeen || value != ciProfileName {
				return commandOptions{}, false, false
			}
			options.profile = value
			profileSeen = true
		case strings.HasPrefix(argument, "--seed="):
			value := strings.TrimPrefix(argument, "--seed=")
			if seedSeen || !unsignedDecimal(value) {
				return commandOptions{}, false, false
			}
			seed, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return commandOptions{}, false, false
			}
			options.seed = seed
			seedSeen = true
		default:
			return commandOptions{}, false, false
		}
	}
	return options, false, profileSeen && seedSeen
}

func unsignedDecimal(value string) bool {
	if value == "" || len(value) > 20 {
		return false
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func writeStatic(destination io.Writer, text string) {
	if destination == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	_, _ = io.WriteString(destination, text)
}

func writeExact(destination io.Writer, data []byte) bool {
	if destination == nil {
		return false
	}
	for len(data) > 0 {
		written, err := destination.Write(data)
		if err != nil || written <= 0 || written > len(data) {
			return false
		}
		data = data[written:]
	}
	return true
}
