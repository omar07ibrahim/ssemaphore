package main

import (
	"bytes"
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
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(
		string(module),
		"module github.com/omar07ibrahim/ssemaphore\n",
	) {
		return "", errors.New("unexpected repository root")
	}
	return root, nil
}

func checkVisualArtifacts(output string, artifacts map[string][]byte) ([]string, error) {
	stale := make([]string, 0)
	for _, name := range sortedArtifactNames(artifacts) {
		current, err := os.ReadFile(filepath.Join(output, name))
		switch {
		case errors.Is(err, os.ErrNotExist):
			stale = append(stale, name)
		case err != nil:
			return nil, errors.New("read managed artifact")
		case !bytes.Equal(current, artifacts[name]):
			stale = append(stale, name)
		}
	}

	entries, err := os.ReadDir(output)
	if errors.Is(err, os.ErrNotExist) {
		return stale, nil
	}
	if err != nil {
		return nil, errors.New("read artifact directory")
	}
	for _, entry := range entries {
		if _, managed := artifacts[entry.Name()]; !managed {
			stale = append(stale, "unexpected:"+entry.Name())
		}
	}
	sort.Strings(stale)
	return stale, nil
}

func writeVisualArtifacts(output string, artifacts map[string][]byte) error {
	if _, ok := artifacts[visualManifestName]; !ok {
		return errors.New("artifact set has no manifest")
	}
	if err := os.MkdirAll(output, 0o755); err != nil {
		return errors.New("create artifact directory")
	}
	for _, name := range visualWriteOrder(artifacts) {
		if err := atomicWriteVisual(filepath.Join(output, name), artifacts[name]); err != nil {
			return err
		}
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

func atomicWriteVisual(path string, payload []byte) error {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*")
	if err != nil {
		return errors.New("create temporary artifact")
	}
	temporary := file.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(temporary)
		}
	}()

	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return errors.New("set artifact mode")
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return errors.New("write artifact")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync artifact")
	}
	if err := file.Close(); err != nil {
		return errors.New("close artifact")
	}
	if err := os.Rename(temporary, path); err != nil {
		return errors.New("replace artifact")
	}
	remove = false

	directoryHandle, err := os.Open(directory)
	if err != nil {
		return errors.New("open artifact directory")
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return errors.New("sync artifact directory")
	}
	return nil
}
