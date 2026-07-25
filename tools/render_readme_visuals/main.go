package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	visualOutputDirectory = "docs/visuals/generated"
	visualManifestName    = "manifest.sha256.json"
	visualRunTimeout      = 45 * time.Second
)

func main() {
	os.Exit(runVisualCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runVisualCLI(args []string, stdout, stderr io.Writer) int {
	check, ok := parseVisualArgs(args)
	if !ok {
		_, _ = io.WriteString(stderr, "usage: go run ./tools/render_readme_visuals [--check]\n")
		return 2
	}

	root, err := visualRepositoryRoot()
	if err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: repository check failed\n")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), visualRunTimeout)
	defer cancel()
	evidence, err := runLoopbackDemo(ctx, root)
	if err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: loopback verification failed\n")
		return 1
	}
	if err := validateDemoEvidence(evidence); err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: evidence contract failed\n")
		return 1
	}

	artifacts, err := buildVisualArtifacts(root, evidence)
	if err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: artifact build failed\n")
		return 1
	}
	output := filepath.Join(root, filepath.FromSlash(visualOutputDirectory))

	if check {
		stale, err := checkVisualArtifacts(output, artifacts)
		if err != nil {
			_, _ = io.WriteString(stderr, "render_readme_visuals: artifact check failed\n")
			return 1
		}
		if len(stale) != 0 {
			_, _ = fmt.Fprintf(
				stderr,
				"stale README visuals: %s\n",
				visualStaleSummary(stale),
			)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "README visuals are current: %s\n", visualOutputDirectory)
		return 0
	}

	current, err := checkVisualArtifacts(output, artifacts)
	if err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: artifact preflight failed\n")
		return 1
	}
	for _, name := range current {
		if strings.HasPrefix(name, "unexpected:") {
			_, _ = io.WriteString(stderr, "unexpected README visual entry detected\n")
			return 1
		}
	}
	if err := writeVisualArtifacts(output, artifacts); err != nil {
		_, _ = io.WriteString(stderr, "render_readme_visuals: artifact write failed\n")
		return 1
	}
	stale, err := checkVisualArtifacts(output, artifacts)
	if err != nil || len(stale) != 0 {
		_, _ = io.WriteString(stderr, "render_readme_visuals: written artifact check failed\n")
		return 1
	}

	_, _ = fmt.Fprintf(
		stdout,
		"wrote %d README artifacts to %s\n",
		len(artifacts),
		visualOutputDirectory,
	)
	return 0
}

func parseVisualArgs(args []string) (bool, bool) {
	switch {
	case len(args) == 0:
		return false, true
	case len(args) == 1 && args[0] == "--check":
		return true, true
	default:
		return false, false
	}
}

func visualRepositoryRoot() (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("caller unavailable")
	}
	root, err := filepath.Abs(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	if err != nil {
		return "", errors.New("resolve repository root")
	}
	repository, err := openPinnedVisualDirectory(root, false)
	if err != nil {
		return "", errors.New("open repository root")
	}
	module, _, readErr := readBoundedVisualFile(
		repository,
		"go.mod",
		visualMaxSourceFileBytes,
	)
	closeErr := repository.Close()
	if readErr != nil || closeErr != nil || !strings.HasPrefix(
		string(module),
		"module github.com/omar07ibrahim/ssemaphore\n",
	) {
		return "", errors.New("unexpected repository root")
	}
	return root, nil
}

func checkVisualArtifacts(output string, artifacts map[string][]byte) ([]string, error) {
	if err := validateVisualArtifactSet(artifacts); err != nil {
		return nil, err
	}
	directory, err := openPinnedVisualDirectory(output, false)
	if errors.Is(err, os.ErrNotExist) {
		return sortedArtifactNames(artifacts), nil
	}
	if err != nil {
		return nil, errors.New("open artifact directory")
	}
	stale, checkErr := checkVisualArtifactsInRoot(directory, artifacts)
	closeErr := directory.Close()
	if checkErr != nil {
		return nil, checkErr
	}
	if closeErr != nil {
		return nil, errors.New("close artifact directory")
	}
	return stale, nil
}

func writeVisualArtifacts(output string, artifacts map[string][]byte) error {
	if err := validateVisualArtifactSet(artifacts); err != nil {
		return err
	}
	directory, err := openPinnedVisualDirectory(output, true)
	if err != nil {
		return errors.New("create artifact directory")
	}
	if err := validateVisualOutputInventory(directory, artifacts); err != nil {
		_ = directory.Close()
		return err
	}
	writeErr := writeVisualArtifactBundle(directory, artifacts, atomicWriteVisual)
	closeErr := directory.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return errors.New("close artifact directory")
	}
	return nil
}

func visualWriteOrder(artifacts map[string][]byte) []string {
	names := make([]string, 0, len(artifacts))
	for name := range artifacts {
		if name != visualManifestName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if _, ok := artifacts[visualManifestName]; ok {
		names = append(names, visualManifestName)
	}
	return names
}

func sortedArtifactNames(artifacts map[string][]byte) []string {
	names := make([]string, 0, len(artifacts))
	for name := range artifacts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func visualStaleSummary(stale []string) string {
	safe := make([]string, 0, len(stale))
	unexpected := false
	for _, name := range stale {
		if strings.HasPrefix(name, "unexpected:") {
			unexpected = true
			continue
		}
		safe = append(safe, name)
	}
	if unexpected {
		safe = append(safe, "unexpected entry")
	}
	return strings.Join(safe, ", ")
}
