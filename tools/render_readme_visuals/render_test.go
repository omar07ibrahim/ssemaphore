package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateDemoEvidenceRequiresEveryPublishedInvariant(t *testing.T) {
	evidence := completeDemoEvidence()
	if err := validateDemoEvidence(evidence); err != nil {
		t.Fatalf("validateDemoEvidence(valid) error = %v", err)
	}

	tests := map[string]func(*demoEvidence){
		"toolchain":             func(value *demoEvidence) { value.GoVersion = "go0.0.0" },
		"operating system":      func(value *demoEvidence) { value.OperatingSystem = "not-linux" },
		"validate output":       func(value *demoEvidence) { value.ValidateStdout = "accepted\n" },
		"validate listener":     func(value *demoEvidence) { value.ValidatePortStayedReserved = false },
		"validate upstream":     func(value *demoEvidence) { value.ValidateUpstreamCalls = 1 },
		"buffered body":         func(value *demoEvidence) { value.BufferedBodyExact = false },
		"buffered headers":      func(value *demoEvidence) { value.BufferedSafeHeaders = false },
		"buffered status":       func(value *demoEvidence) { value.BufferedStatus = 500 },
		"stream order":          func(value *demoEvidence) { value.StreamEvents[1] = "[DONE]" },
		"early event":           func(value *demoEvidence) { value.FirstChunkBeforeRelease = false },
		"stream headers":        func(value *demoEvidence) { value.StreamSafeHeaders = false },
		"stream status":         func(value *demoEvidence) { value.StreamStatus = 500 },
		"tenant isolation":      func(value *demoEvidence) { value.TenantCredentialAbsentUpstream = false },
		"upstream isolation":    func(value *demoEvidence) { value.UpstreamCredentialAbsentDownstream = false },
		"client headers":        func(value *demoEvidence) { value.ClientHeadersAbsentUpstream = false },
		"upstream headers":      func(value *demoEvidence) { value.UpstreamHeadersStripped = false },
		"header allowlist":      func(value *demoEvidence) { value.UpstreamHeaderAllowlistExact = false },
		"separate credential":   func(value *demoEvidence) { value.SeparateUpstreamCredential = false },
		"inflight completion":   func(value *demoEvidence) { value.InflightStreamCompleted = false },
		"process output":        func(value *demoEvidence) { value.ServeOutputEmpty = false },
		"process exit":          func(value *demoEvidence) { value.ShutdownExitCode = 1 },
		"listener release":      func(value *demoEvidence) { value.ListenerReleased = false },
		"upstream call count":   func(value *demoEvidence) { value.UpstreamCalls = 3 },
		"buffered HTTP version": func(value *demoEvidence) { value.BufferedProtocolMajor = 2 },
		"stream HTTP version":   func(value *demoEvidence) { value.StreamProtocolMajor = 2 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := completeDemoEvidence()
			candidate.StreamEvents = append([]string(nil), candidate.StreamEvents...)
			mutate(&candidate)
			if err := validateDemoEvidence(candidate); err == nil {
				t.Fatal("validateDemoEvidence() accepted a false published invariant")
			}
		})
	}
}

func TestVisualArtifactsAreDeterministicAccessibleAndSelfContained(t *testing.T) {
	root, err := visualRepositoryRoot()
	if err != nil {
		t.Fatalf("visualRepositoryRoot() error = %v", err)
	}
	first, err := buildVisualArtifacts(root, completeDemoEvidence())
	if err != nil {
		t.Fatalf("buildVisualArtifacts(first) error = %v", err)
	}
	second, err := buildVisualArtifacts(root, completeDemoEvidence())
	if err != nil {
		t.Fatalf("buildVisualArtifacts(second) error = %v", err)
	}
	if len(first) != 7 || len(second) != len(first) {
		t.Fatalf("artifact counts = %d/%d, want 7/7", len(first), len(second))
	}
	for name, payload := range first {
		if string(second[name]) != string(payload) {
			t.Fatalf("artifact %q is not byte-deterministic", name)
		}
		if err := ensureVisualPublishable(string(payload)); err != nil {
			t.Fatalf("artifact %q publishability error = %v", name, err)
		}
	}

	for _, name := range []string{
		visualTerminalName,
		visualSequenceName,
		visualArchitectureName,
		visualSetupName,
	} {
		payload := string(first[name])
		decoder := xml.NewDecoder(strings.NewReader(payload))
		for {
			if _, err := decoder.Token(); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("parse %s: %v", name, err)
			}
		}
		for _, forbidden := range []string{
			"<script",
			"<foreignObject",
			"<image",
			`href="http`,
			`href='http`,
		} {
			if strings.Contains(payload, forbidden) {
				t.Fatalf("%s contains forbidden external or executable content %q", name, forbidden)
			}
		}
		if !strings.Contains(payload, "<title") || !strings.Contains(payload, "<desc") {
			t.Fatalf("%s is missing accessible title/description", name)
		}
		if strings.Contains(payload, `font-family="Deja Sans`) {
			t.Fatalf("%s contains a misspelled font fallback", name)
		}
	}

	architecture := string(first[visualArchitectureName])
	for _, required := range []string{
		"PREDISPATCH COUNT SLOTS",
		"count + exact body bytes + estimated work",
		"count + work only (NO BYTES)",
		"FUTURE - NOT IMPLEMENTED",
	} {
		if !strings.Contains(architecture, required) {
			t.Fatalf("%s is missing exact boundary %q", visualArchitectureName, required)
		}
	}
	setup := string(first[visualSetupName])
	for _, required := range []string{
		"Go 1.26.5",
		"mode 0700",
		"exact mode 0600",
		"gateway policy is valid",
		"Replace the example upstream endpoint before serving.",
	} {
		if !strings.Contains(setup, required) {
			t.Fatalf("%s is missing workflow fact %q", visualSetupName, required)
		}
	}

	var manifest visualManifest
	if err := json.Unmarshal(first[visualManifestName], &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.Schema != "ssemaphore.readme-visuals.v1" {
		t.Fatalf("manifest schema = %q", manifest.Schema)
	}
	if len(manifest.Provenance.Engine.Files) == 0 ||
		len(manifest.Provenance.Generator.Files) == 0 {
		t.Fatal("manifest source lists are empty")
	}
	for name, metadata := range manifest.Artifacts {
		if metadata.Bytes != len(first[name]) || metadata.SHA256 != visualSHA256(first[name]) {
			t.Fatalf("manifest metadata for %q does not match exact artifact bytes", name)
		}
	}
}

func TestArtifactCheckRejectsUnexpectedEntriesAndManifestWritesLast(t *testing.T) {
	output := t.TempDir()
	artifacts := map[string][]byte{
		"b.svg":                  []byte("<svg/>\n"),
		"a.txt":                  []byte("evidence\n"),
		visualManifestName:       []byte("{}\n"),
		"loopback-evidence.json": []byte("{}\n"),
	}
	if err := writeVisualArtifacts(output, artifacts); err != nil {
		t.Fatalf("writeVisualArtifacts() error = %v", err)
	}
	if stale, err := checkVisualArtifacts(output, artifacts); err != nil || len(stale) != 0 {
		t.Fatalf("fresh check = (%v, %v), want no stale entries", stale, err)
	}
	if err := os.WriteFile(filepath.Join(output, "obsolete.svg"), []byte("<svg/>\n"), 0o644); err != nil {
		t.Fatalf("write stale fixture: %v", err)
	}
	stale, err := checkVisualArtifacts(output, artifacts)
	if err != nil {
		t.Fatalf("checkVisualArtifacts() error = %v", err)
	}
	if strings.Join(stale, ",") != "unexpected:obsolete.svg" {
		t.Fatalf("stale entries = %v, want unexpected obsolete SVG", stale)
	}
	if summary := visualStaleSummary([]string{
		"loopback-terminal.svg",
		"unexpected:private-token-name",
	}); summary != "loopback-terminal.svg, unexpected entry" {
		t.Fatalf("safe stale summary = %q", summary)
	}
	order := visualWriteOrder(artifacts)
	if order[len(order)-1] != visualManifestName {
		t.Fatalf("write order = %v, want manifest last", order)
	}
	if err := writeVisualArtifacts(
		t.TempDir(),
		map[string][]byte{"result.svg": []byte("<svg/>\n")},
	); err == nil {
		t.Fatal("writeVisualArtifacts() accepted a set without its manifest")
	}
	for name := range artifacts {
		info, err := os.Stat(filepath.Join(output, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Mode() != 0o644 {
			t.Fatalf("artifact %s mode = %v, want 0644 regular file", name, info.Mode())
		}
	}
}

func TestPublishabilityGateRejectsHostDetailsAndCredentialForms(t *testing.T) {
	tests := []string{
		"contact: person@example.com\n",
		"artifact: /home/user/private/report.json\n",
		"artifact: C:\\Users\\person\\private\\report.json\n",
		"credential " + "github_" + "pat_" + strings.Repeat("s", 32) + "\n",
		"credential " + "sk-" + strings.Repeat("s", 37) + "\n",
		"credential " + "xoxb-" + strings.Repeat("s", 37) + "\n",
		"authorization: Bearer synthetic-but-private-value\n",
		"non-ascii: \u015e\n",
	}
	for _, text := range tests {
		if err := ensureVisualPublishable(text); err == nil {
			t.Fatalf("ensureVisualPublishable(%q) accepted private content", text)
		}
	}
	for _, text := range []string{
		`<svg xmlns="http://www.w3.org/2000/svg"></svg>` + "\n",
		"documentation: https://example.invalid/reference\n",
	} {
		if err := ensureVisualPublishable(text); err != nil {
			t.Fatalf("ensureVisualPublishable(%q) rejected a public URL: %v", text, err)
		}
	}
}

func TestCommittedVisualsMatchFreshRealLoopbackRun(t *testing.T) {
	if testing.Short() {
		t.Skip("real binary loopback evidence is a release test")
	}
	root, err := visualRepositoryRoot()
	if err != nil {
		t.Fatalf("visualRepositoryRoot() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	evidence, err := runLoopbackDemo(ctx, root)
	if err != nil {
		t.Fatalf("runLoopbackDemo() failed")
	}
	if err := validateDemoEvidence(evidence); err != nil {
		t.Fatalf("validateDemoEvidence(real) error = %v", err)
	}
	artifacts, err := buildVisualArtifacts(root, evidence)
	if err != nil {
		t.Fatalf("buildVisualArtifacts(real) error = %v", err)
	}
	stale, err := checkVisualArtifacts(
		filepath.Join(root, filepath.FromSlash(visualOutputDirectory)),
		artifacts,
	)
	if err != nil {
		t.Fatalf("checkVisualArtifacts(real) error = %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("committed README visuals are stale: %v", stale)
	}
}

func completeDemoEvidence() demoEvidence {
	return demoEvidence{
		GoVersion:                          visualExpectedGo,
		OperatingSystem:                    visualExpectedOS,
		ValidateStdout:                     "gateway policy is valid\n",
		ValidatePortStayedReserved:         true,
		ValidateUpstreamCalls:              0,
		BufferedStatus:                     200,
		BufferedProtocolMajor:              1,
		BufferedBodyExact:                  true,
		BufferedSafeHeaders:                true,
		StreamStatus:                       200,
		StreamProtocolMajor:                1,
		StreamEvents:                       []string{"chunk-1", "chunk-2", "[DONE]", "clean-eof"},
		FirstChunkBeforeRelease:            true,
		StreamSafeHeaders:                  true,
		TenantCredentialAbsentUpstream:     true,
		UpstreamCredentialAbsentDownstream: true,
		UpstreamHeadersStripped:            true,
		ClientHeadersAbsentUpstream:        true,
		UpstreamHeaderAllowlistExact:       true,
		SeparateUpstreamCredential:         true,
		InflightStreamCompleted:            true,
		ServeOutputEmpty:                   true,
		ShutdownExitCode:                   0,
		ListenerReleased:                   true,
		UpstreamCalls:                      2,
	}
}
