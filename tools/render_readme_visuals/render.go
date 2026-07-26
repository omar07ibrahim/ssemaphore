package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	visualEvidenceJSONName = "loopback-evidence.json"
	visualSummaryName      = "loopback-evidence.txt"
	visualTerminalName     = "loopback-terminal.svg"
	visualSequenceName     = "stream-sequence.svg"
	visualArchitectureName = "architecture.svg"
	visualSetupName        = "setup-workflow.svg"
	visualExpectedGo       = "go1.26.5"
	visualExpectedOS       = "linux"
)

var (
	visualEmailPattern = regexp.MustCompile(
		`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`,
	)
	visualHostPathPattern = regexp.MustCompile(
		`(?:^|[\s"'=:,(])(?:` +
			`/(?:home|Users|srv|workspace|workspaces)/[A-Za-z0-9._~\-/]+|` +
			`[A-Za-z]:[\\/])`,
	)
	visualSecretPattern = regexp.MustCompile(
		`(?i)-----BEGIN [A-Z ]*PRIVATE KEY-----|` +
			`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|` +
			`\bgh[pousr]_[A-Za-z0-9]{20,}\b|` +
			`\bgithub_pat_[A-Za-z0-9_]{20,}\b|` +
			`\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b|` +
			`\bxox[baprs]-[A-Za-z0-9-]{10,}\b|` +
			`\b(?:api[_-]?key|authorization|bearer|password|passwd|secret|token)` +
			`\s*[:=]\s*\S+`,
	)
)

type visualEvidenceDocument struct {
	Schema    string                  `json:"schema"`
	Scope     string                  `json:"scope"`
	Toolchain string                  `json:"toolchain"`
	Platform  string                  `json:"platform"`
	Validate  visualValidateEvidence  `json:"validate"`
	Buffered  visualBufferedEvidence  `json:"buffered"`
	Streaming visualStreamingEvidence `json:"streaming"`
	Isolation visualIsolationEvidence `json:"isolation"`
	Shutdown  visualShutdownEvidence  `json:"shutdown"`
}

type visualValidateEvidence struct {
	Accepted                 bool `json:"accepted"`
	ListenerRemainedReserved bool `json:"listener_remained_reserved"`
	UpstreamConnections      int  `json:"upstream_connections"`
	UpstreamHTTPRequests     int  `json:"upstream_http_requests"`
}

type visualBufferedEvidence struct {
	Status           int  `json:"status"`
	ProtocolMajor    int  `json:"protocol_major"`
	UpstreamAttempts int  `json:"upstream_attempts"`
	BodyExact        bool `json:"body_exact"`
	SafeHeaders      bool `json:"safe_response_headers"`
}

type visualStreamingEvidence struct {
	Status                          int      `json:"status"`
	ProtocolMajor                   int      `json:"protocol_major"`
	UpstreamAttempts                int      `json:"upstream_attempts"`
	FirstEventBeforeUpstreamRelease bool     `json:"first_event_before_upstream_release"`
	EventOrder                      []string `json:"event_order"`
	CleanEOF                        bool     `json:"clean_eof"`
	SafeHeaders                     bool     `json:"safe_response_headers"`
}

type visualIsolationEvidence struct {
	UpstreamHeaderAllowlistExact bool `json:"upstream_header_allowlist_exact"`
	SeparateUpstreamCredential   bool `json:"separate_upstream_credential"`
	TenantCredentialForwarded    bool `json:"tenant_credential_forwarded"`
	ClientHeadersForwarded       bool `json:"client_headers_forwarded"`
	UpstreamHeadersRelayed       bool `json:"upstream_headers_relayed"`
	UpstreamCredentialRelayed    bool `json:"upstream_credential_relayed"`
}

type visualShutdownEvidence struct {
	Signal                  string `json:"signal"`
	InflightStreamCompleted bool   `json:"inflight_stream_completed"`
	ExitCode                int    `json:"exit_code"`
	ProcessOutputEmpty      bool   `json:"process_output_empty"`
	ListenerReleased        bool   `json:"listener_released"`
}

type visualManifest struct {
	Schema     string                            `json:"schema"`
	Runs       visualManifestRuns                `json:"runs"`
	Provenance visualManifestProvenance          `json:"provenance"`
	Artifacts  map[string]visualArtifactMetadata `json:"artifacts"`
}

type visualManifestRuns struct {
	Loopback   visualManifestRun           `json:"loopback"`
	Saturation visualManifestSaturationRun `json:"saturation"`
}

type visualManifestRun struct {
	Command   string `json:"command"`
	Scope     string `json:"scope"`
	Toolchain string `json:"toolchain"`
	Platform  string `json:"platform"`
}

type visualManifestSaturationRun struct {
	Command                   string `json:"command"`
	ReproduceCommand          string `json:"reproduce_command"`
	Scope                     string `json:"scope"`
	Toolchain                 string `json:"toolchain"`
	Platform                  string `json:"platform"`
	Architecture              string `json:"architecture"`
	Seed                      uint64 `json:"seed"`
	DiagnosticTimingsIncluded bool   `json:"diagnostic_timings_included"`
	Performance               bool   `json:"performance_claim"`
}

type visualManifestProvenance struct {
	Engine            visualSourceDigest `json:"engine"`
	Generator         visualSourceDigest `json:"generator"`
	SaturationHarness visualSourceDigest `json:"saturation_harness"`
	Policy            visualFileDigest   `json:"policy_example"`
	Scope             string             `json:"scope"`
}

type visualSourceDigest struct {
	Files  []string `json:"files"`
	SHA256 string   `json:"sha256"`
}

type visualFileDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type visualArtifactMetadata struct {
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func validateDemoEvidence(evidence demoEvidence) error {
	expectedEvents := []string{"chunk-1", "chunk-2", "[DONE]", "clean-eof"}
	switch {
	case evidence.GoVersion != visualExpectedGo:
		return errors.New("unexpected Go toolchain")
	case evidence.OperatingSystem != visualExpectedOS:
		return errors.New("unexpected operating system")
	case evidence.ValidateStdout != "gateway policy is valid\n":
		return errors.New("unexpected validate stdout")
	case !evidence.ValidatePortStayedReserved ||
		evidence.ValidateUpstreamConnections != 0 ||
		evidence.ValidateUpstreamCalls != 0:
		return errors.New("validate crossed a runtime boundary")
	case evidence.BufferedStatus != 200 || evidence.BufferedProtocolMajor != 1:
		return errors.New("unexpected buffered response")
	case !evidence.BufferedBodyExact || !evidence.BufferedSafeHeaders:
		return errors.New("buffered response evidence failed")
	case evidence.StreamStatus != 200 || evidence.StreamProtocolMajor != 1:
		return errors.New("unexpected streaming response")
	case !equalStrings(evidence.StreamEvents, expectedEvents):
		return errors.New("unexpected streaming event order")
	case !evidence.FirstChunkBeforeRelease || !evidence.StreamSafeHeaders:
		return errors.New("streaming boundary evidence failed")
	case !evidence.TenantCredentialAbsentUpstream:
		return errors.New("tenant credential isolation failed")
	case !evidence.UpstreamCredentialAbsentDownstream:
		return errors.New("upstream credential isolation failed")
	case !evidence.UpstreamHeadersStripped || !evidence.ClientHeadersAbsentUpstream:
		return errors.New("header isolation failed")
	case !evidence.UpstreamHeaderAllowlistExact || !evidence.SeparateUpstreamCredential:
		return errors.New("upstream construction evidence failed")
	case !evidence.InflightStreamCompleted:
		return errors.New("in-flight stream did not complete")
	case !evidence.ServeOutputEmpty || evidence.ShutdownExitCode != 0:
		return errors.New("serve process did not terminate cleanly")
	case !evidence.ListenerReleased || evidence.UpstreamCalls != 2:
		return errors.New("terminal resource evidence failed")
	default:
		return nil
	}
}

func buildVisualArtifacts(
	root string,
	evidence demoEvidence,
	saturation saturationVisualEvidence,
) (map[string][]byte, error) {
	document := evidenceDocument(evidence)
	evidenceJSON, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, errors.New("marshal evidence")
	}
	evidenceJSON = append(evidenceJSON, '\n')

	summary := []byte(renderEvidenceSummary(evidence))
	artifacts := map[string][]byte{
		visualEvidenceJSONName: evidenceJSON,
		visualSummaryName:      summary,
		visualTerminalName:     []byte(renderTerminalSVG(string(summary))),
		visualSequenceName:     []byte(renderSequenceSVG(evidence)),
		visualArchitectureName: []byte(renderArchitectureSVG()),
		visualSetupName:        []byte(renderSetupWorkflowSVG()),
	}
	saturationArtifacts, err := buildSaturationVisualArtifacts(saturation)
	if err != nil {
		return nil, err
	}
	for name, payload := range saturationArtifacts {
		if _, exists := artifacts[name]; exists {
			return nil, errors.New("duplicate visual artifact name")
		}
		artifacts[name] = payload
	}
	for name, payload := range artifacts {
		if err := ensureVisualPublishable(string(payload)); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}

	engine, err := digestVisualSources(
		root,
		[]string{
			".go-version",
			"go.mod",
			"configs/policy.example.json",
			"docs/running.md",
		},
		[]string{"cmd", "internal"},
		false,
	)
	if err != nil {
		return nil, err
	}
	saturationHarness, err := digestVisualSources(
		root,
		nil,
		[]string{"tools/run_saturation"},
		false,
	)
	if err != nil {
		return nil, err
	}
	generator, err := digestVisualSources(
		root,
		nil,
		[]string{"tools/render_readme_visuals"},
		false,
	)
	if err != nil {
		return nil, err
	}
	policyPath := "configs/policy.example.json"
	repository, err := openPinnedVisualDirectory(root, false)
	if err != nil {
		return nil, errors.New("open repository for policy digest")
	}
	policyBytes, _, readErr := readBoundedVisualFile(
		repository,
		policyPath,
		visualMaxSourceFileBytes,
	)
	closeErr := repository.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.New("read policy example")
	}

	manifest := visualManifest{
		Schema: "ssemaphore.readme-visuals.v2",
		Runs: visualManifestRuns{
			Loopback: visualManifestRun{
				Command:   "GOTOOLCHAIN=go1.26.5 go run ./tools/render_readme_visuals",
				Scope:     document.Scope,
				Toolchain: evidence.GoVersion,
				Platform:  evidence.OperatingSystem,
			},
			Saturation: visualManifestSaturationRun{
				Command: "GOTOOLCHAIN=go1.26.5 go run ./tools/render_readme_visuals",
				ReproduceCommand: "GOTOOLCHAIN=go1.26.5 go run ./tools/run_saturation " +
					"--profile=ci --seed=20260725",
				Scope:                     saturation.Scope,
				Toolchain:                 saturation.Toolchain,
				Platform:                  saturation.Platform,
				Architecture:              saturation.Architecture,
				Seed:                      saturation.Projection.Seed,
				DiagnosticTimingsIncluded: saturation.DiagnosticTimingsIncluded,
				Performance:               saturation.PerformanceClaim,
			},
		},
		Provenance: visualManifestProvenance{
			Engine:            engine,
			Generator:         generator,
			SaturationHarness: saturationHarness,
			Policy: visualFileDigest{
				Path:   policyPath,
				SHA256: visualSHA256(policyBytes),
			},
			Scope: "source and artifact digests support deterministic review; " +
				"they are not signatures or attestations",
		},
		Artifacts: make(map[string]visualArtifactMetadata, len(artifacts)),
	}
	for name, payload := range artifacts {
		manifest.Artifacts[name] = visualArtifactMetadata{
			Bytes:  len(payload),
			SHA256: visualSHA256(payload),
		}
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, errors.New("marshal visual manifest")
	}
	manifestBytes = append(manifestBytes, '\n')
	if err := ensureVisualPublishable(string(manifestBytes)); err != nil {
		return nil, fmt.Errorf("%s: %w", visualManifestName, err)
	}
	artifacts[visualManifestName] = manifestBytes
	return artifacts, nil
}

func evidenceDocument(evidence demoEvidence) visualEvidenceDocument {
	eventOrder := append([]string(nil), evidence.StreamEvents...)
	cleanEOF := len(eventOrder) > 0 && eventOrder[len(eventOrder)-1] == "clean-eof"
	if cleanEOF {
		eventOrder = eventOrder[:len(eventOrder)-1]
	}

	return visualEvidenceDocument{
		Schema:    "ssemaphore.loopback-evidence.v1",
		Scope:     "one Linux gateway instance, one tenant, two synthetic numeric-loopback requests",
		Toolchain: evidence.GoVersion,
		Platform:  evidence.OperatingSystem,
		Validate: visualValidateEvidence{
			Accepted:                 true,
			ListenerRemainedReserved: evidence.ValidatePortStayedReserved,
			UpstreamConnections:      evidence.ValidateUpstreamConnections,
			UpstreamHTTPRequests:     evidence.ValidateUpstreamCalls,
		},
		Buffered: visualBufferedEvidence{
			Status:           evidence.BufferedStatus,
			ProtocolMajor:    evidence.BufferedProtocolMajor,
			UpstreamAttempts: 1,
			BodyExact:        evidence.BufferedBodyExact,
			SafeHeaders:      evidence.BufferedSafeHeaders,
		},
		Streaming: visualStreamingEvidence{
			Status:                          evidence.StreamStatus,
			ProtocolMajor:                   evidence.StreamProtocolMajor,
			UpstreamAttempts:                1,
			FirstEventBeforeUpstreamRelease: evidence.FirstChunkBeforeRelease,
			EventOrder:                      eventOrder,
			CleanEOF:                        cleanEOF,
			SafeHeaders:                     evidence.StreamSafeHeaders,
		},
		Isolation: visualIsolationEvidence{
			UpstreamHeaderAllowlistExact: evidence.UpstreamHeaderAllowlistExact,
			SeparateUpstreamCredential:   evidence.SeparateUpstreamCredential,
			TenantCredentialForwarded:    !evidence.TenantCredentialAbsentUpstream,
			ClientHeadersForwarded:       !evidence.ClientHeadersAbsentUpstream,
			UpstreamHeadersRelayed:       !evidence.UpstreamHeadersStripped,
			UpstreamCredentialRelayed:    !evidence.UpstreamCredentialAbsentDownstream,
		},
		Shutdown: visualShutdownEvidence{
			Signal:                  "SIGTERM",
			InflightStreamCompleted: evidence.InflightStreamCompleted,
			ExitCode:                evidence.ShutdownExitCode,
			ProcessOutputEmpty:      evidence.ServeOutputEmpty,
			ListenerReleased:        evidence.ListenerReleased,
		},
	}
}

func renderEvidenceSummary(evidence demoEvidence) string {
	return strings.Join(
		[]string{
			"verified run: GOTOOLCHAIN=go1.26.5 go run ./tools/render_readme_visuals",
			"validate: " + strings.TrimSuffix(evidence.ValidateStdout, "\n"),
			fmt.Sprintf(
				"validate boundary: port reserved; upstream connections %d; HTTP requests %d",
				evidence.ValidateUpstreamConnections,
				evidence.ValidateUpstreamCalls,
			),
			fmt.Sprintf(
				"buffered relay: HTTP/%d %d; exact validated body; safe headers",
				evidence.BufferedProtocolMajor,
				evidence.BufferedStatus,
			),
			"stream relay: " + strings.Join(evidence.StreamEvents, " -> "),
			"stream ordering: first chunk arrived before upstream release",
			"credential boundary: tenant absent upstream; upstream absent downstream",
			"header boundary: client and upstream private headers stripped",
			fmt.Sprintf(
				"shutdown: SIGTERM -> exit %d; in-flight stream completed; listener released",
				evidence.ShutdownExitCode,
			),
			"",
		},
		"\n",
	)
}

func renderTerminalSVG(summary string) string {
	lines := strings.Split(strings.TrimSuffix(summary, "\n"), "\n")
	var body strings.Builder
	body.WriteString(`  <rect width="1200" height="390" rx="17" fill="#0d1117"/>` + "\n")
	body.WriteString(`  <circle cx="31" cy="29" r="7" fill="#ff5f56"/>` + "\n")
	body.WriteString(`  <circle cx="55" cy="29" r="7" fill="#ffbd2e"/>` + "\n")
	body.WriteString(`  <circle cx="79" cy="29" r="7" fill="#27c93f"/>` + "\n")
	body.WriteString(
		`  <text x="600" y="36" fill="#8b949e" text-anchor="middle" ` +
			`font-family="DejaVu Sans Mono, ui-monospace, SFMono-Regular, Consolas, monospace" ` +
			`font-size="15">verified numeric-loopback summary</text>` + "\n",
	)
	body.WriteString(`  <line x1="0" y1="54" x2="1200" y2="54" stroke="#30363d"/>` + "\n")
	for index, line := range lines {
		colour := "#7ee787"
		if index != 0 {
			colour = "#e6edf3"
		}
		_, _ = fmt.Fprintf(
			&body,
			"  <text x=\"54\" y=\"%d\" fill=\"%s\" "+
				"font-family=\"DejaVu Sans Mono, ui-monospace, SFMono-Regular, "+
				"Consolas, monospace\" font-size=\"16\">%s</text>\n",
			90+index*33,
			colour,
			html.EscapeString(line),
		)
	}
	return visualSVGDocument(
		1200,
		390,
		"SSEmaphore verified loopback summary",
		"Content-free evidence summary derived from real validate, buffered relay, streaming relay, credential isolation, and SIGTERM shutdown checks.",
		body.String(),
	)
}

func renderSequenceSVG(evidence demoEvidence) string {
	eventOrder := html.EscapeString(strings.Join(evidence.StreamEvents, " -> "))
	body := fmt.Sprintf(`  <defs>
    <marker id="arrow-blue" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#0969da"/>
    </marker>
    <marker id="arrow-purple" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#8250df"/>
    </marker>
  </defs>
  <rect width="1200" height="650" rx="18" fill="#f6f8fa"/>
  <text x="42" y="50" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="27" font-weight="700">Channel-driven proof of early SSE delivery</text>
  <text x="42" y="79" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="16">A real gateway process relays the first validated event before the controlled upstream is allowed to continue.</text>

  <rect x="55" y="108" width="260" height="54" rx="11" fill="#ffffff" stroke="#54aeff" stroke-width="2"/>
  <text x="185" y="141" fill="#0550ae" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="16" font-weight="700">demo client / coordinator</text>
  <rect x="470" y="108" width="260" height="54" rx="11" fill="#ffffff" stroke="#8250df" stroke-width="2"/>
  <text x="600" y="141" fill="#6639ba" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="16" font-weight="700">real SSEmaphore process</text>
  <rect x="885" y="108" width="260" height="54" rx="11" fill="#ffffff" stroke="#54aeff" stroke-width="2"/>
  <text x="1015" y="141" fill="#0550ae" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="16" font-weight="700">controlled HTTP/1 upstream</text>

  <line x1="185" y1="162" x2="185" y2="516" stroke="#8c959f" stroke-dasharray="6 7"/>
  <line x1="600" y1="162" x2="600" y2="516" stroke="#8c959f" stroke-dasharray="6 7"/>
  <line x1="1015" y1="162" x2="1015" y2="516" stroke="#8c959f" stroke-dasharray="6 7"/>

  <path d="M 185 196 L 590 196" fill="none" stroke="#0969da" stroke-width="3" marker-end="url(#arrow-blue)"/>
  <text x="388" y="186" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">POST stream=true with tenant bearer</text>
  <path d="M 610 234 L 1005 234" fill="none" stroke="#8250df" stroke-width="3" marker-end="url(#arrow-purple)"/>
  <text x="808" y="224" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">fixed HTTP/1 POST with separate bearer</text>
  <path d="M 1005 272 L 610 272" fill="none" stroke="#0969da" stroke-width="3" marker-end="url(#arrow-blue)"/>
  <text x="808" y="262" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">chunk-1 + upstream flush, then wait</text>
  <rect x="488" y="287" width="224" height="48" rx="8" fill="#fbefff" stroke="#bf8fef"/>
  <text x="600" y="307" fill="#6639ba" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" font-weight="700">validate complete event</text>
  <text x="600" y="326" fill="#6639ba" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="12">write exact bytes + flush</text>
  <path d="M 590 354 L 195 354" fill="none" stroke="#8250df" stroke-width="3" marker-end="url(#arrow-purple)"/>
  <text x="388" y="344" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">client reads complete chunk-1</text>
  <rect x="55" y="372" width="260" height="58" rx="9" fill="#dafbe1" stroke="#1a7f37" stroke-width="2"/>
  <text x="185" y="396" fill="#1a7f37" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" font-weight="700">proof gate satisfied</text>
  <text x="185" y="417" fill="#57606a" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="12">chunk observed before release</text>
  <path d="M 195 448 L 590 448" fill="none" stroke="#0969da" stroke-width="3" marker-end="url(#arrow-blue)"/>
  <text x="388" y="441" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13">SIGTERM, then release upstream</text>
  <path d="M 1005 478 L 610 478" fill="none" stroke="#0969da" stroke-width="3" marker-end="url(#arrow-blue)"/>
  <text x="808" y="468" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13">chunk-2 + [DONE] + clean EOF</text>
  <path d="M 590 508 L 195 508" fill="none" stroke="#8250df" stroke-width="3" marker-end="url(#arrow-purple)"/>
  <text x="388" y="498" fill="#24292f" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13">exact remainder; handled drain exits 0</text>

  <rect x="40" y="540" width="1120" height="82" rx="12" fill="#ffffff" stroke="#d0d7de"/>
  <text x="61" y="567" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="14" font-weight="700">Observed order: %s</text>
  <text x="61" y="592" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="13">The channel handshake proves logical delivery before release; it does not claim TCP packet boundaries or zero physical read-ahead.</text>
  <text x="61" y="612" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="13">Scope: one synthetic loopback run; no fairness, overload, public-edge TLS, GPU, telemetry, or real-model claim.</text>
`, eventOrder)
	return visualSVGDocument(
		1200,
		650,
		"SSEmaphore streaming order proof",
		"Sequence showing a real gateway deliver chunk one before the demo releases the controlled upstream, then complete during handled SIGTERM drain.",
		body,
	)
}

func visualSVGDocument(width, height int, title, description, body string) string {
	return fmt.Sprintf(
		"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" "+
			"viewBox=\"0 0 %d %d\" role=\"img\" aria-labelledby=\"title desc\">\n"+
			"  <title id=\"title\">%s</title>\n"+
			"  <desc id=\"desc\">%s</desc>\n%s</svg>\n",
		width,
		height,
		width,
		height,
		html.EscapeString(title),
		html.EscapeString(description),
		body,
	)
}

func ensureVisualPublishable(text string) error {
	if !utf8.ValidString(text) {
		return errors.New("visual evidence is not valid UTF-8")
	}
	for _, character := range text {
		if character > 127 {
			return errors.New("visual evidence must be ASCII")
		}
		if character < 32 && character != '\n' && character != '\t' {
			return errors.New("visual evidence contains a control character")
		}
	}
	switch {
	case visualEmailPattern.MatchString(text):
		return errors.New("visual evidence contains an email address")
	case visualHostPathPattern.MatchString(text):
		return errors.New("visual evidence contains a host path")
	case visualSecretPattern.MatchString(text):
		return errors.New("visual evidence contains a possible credential")
	default:
		return nil
	}
}

func digestVisualSources(
	root string,
	files []string,
	directories []string,
	includeTests bool,
) (visualSourceDigest, error) {
	repository, err := openPinnedVisualDirectory(root, false)
	if err != nil {
		return visualSourceDigest{}, errors.New("open source root")
	}
	result, digestErr := digestVisualSourcesInRoot(
		repository,
		files,
		directories,
		includeTests,
	)
	closeErr := repository.Close()
	if digestErr != nil {
		return visualSourceDigest{}, digestErr
	}
	if closeErr != nil {
		return visualSourceDigest{}, errors.New("close source root")
	}
	return result, nil
}

func digestVisualSourcesInRoot(
	root *os.Root,
	files []string,
	directories []string,
	includeTests bool,
) (visualSourceDigest, error) {
	if len(files) > visualMaxSourceEntries ||
		len(directories) > visualMaxSourceEntries ||
		len(files) > visualMaxSourceEntries-len(directories) {
		return visualSourceDigest{}, errors.New("source request exceeds entry bound")
	}
	selection := visualSourceSelection{
		files: make(map[string]struct{}),
	}
	for _, name := range files {
		canonical, err := canonicalVisualRelativePath(name, false)
		if err != nil {
			return visualSourceDigest{}, errors.New("invalid source file path")
		}
		if _, exists := selection.files[canonical]; !exists &&
			len(selection.files) == visualMaxSourceEntries {
			return visualSourceDigest{}, errors.New("source set exceeds entry bound")
		}
		selection.files[canonical] = struct{}{}
	}
	for _, directory := range directories {
		canonical, err := canonicalVisualRelativePath(directory, true)
		if err != nil {
			return visualSourceDigest{}, errors.New("invalid source directory path")
		}
		if err := collectVisualSourceDirectory(
			root,
			canonical,
			includeTests,
			0,
			&selection,
		); err != nil {
			return visualSourceDigest{}, err
		}
	}
	if len(selection.files) > visualMaxSourceEntries {
		return visualSourceDigest{}, errors.New("source set exceeds entry bound")
	}

	names := make([]string, 0, len(selection.files))
	for name := range selection.files {
		names = append(names, name)
	}
	sort.Strings(names)
	digest := sha256.New()
	var totalBytes int64
	for _, name := range names {
		payload, _, err := readBoundedVisualFile(
			root,
			name,
			visualMaxSourceFileBytes,
		)
		if err != nil {
			return visualSourceDigest{}, errors.New("read source input")
		}
		if int64(len(payload)) > visualMaxSourceTotalBytes-totalBytes {
			return visualSourceDigest{}, errors.New("source set exceeds byte bound")
		}
		totalBytes += int64(len(payload))
		_, _ = digest.Write([]byte(name))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(payload)
		_, _ = digest.Write([]byte{0})
	}
	return visualSourceDigest{
		Files:  names,
		SHA256: hex.EncodeToString(digest.Sum(nil)),
	}, nil
}

func visualSHA256(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
