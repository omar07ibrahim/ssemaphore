package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strings"
)

const (
	visualSaturationEvidenceName = "saturation-evidence.json"
	visualSaturationSummaryName  = "saturation-evidence.txt"
	visualSaturationTerminalName = "saturation-terminal.svg"
	visualSaturationOutcomesName = "saturation-outcomes.svg"
	visualSaturationDispatchName = "saturation-dispatch.svg"
)

func buildSaturationVisualArtifacts(
	evidence saturationVisualEvidence,
) (map[string][]byte, error) {
	if err := validateSaturationVisualEvidence(evidence); err != nil {
		return nil, err
	}
	evidenceJSON, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return nil, errors.New("marshal saturation evidence")
	}
	evidenceJSON = append(evidenceJSON, '\n')
	summary := renderSaturationSummary(evidence)
	artifacts := map[string][]byte{
		visualSaturationEvidenceName: evidenceJSON,
		visualSaturationSummaryName:  []byte(summary),
		visualSaturationTerminalName: []byte(renderSaturationTerminalSVG(summary)),
		visualSaturationOutcomesName: []byte(renderSaturationOutcomesSVG(evidence)),
		visualSaturationDispatchName: []byte(renderSaturationDispatchSVG(evidence)),
	}
	for name, payload := range artifacts {
		if err := ensureVisualPublishable(string(payload)); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	return artifacts, nil
}

func renderSaturationSummary(evidence saturationVisualEvidence) string {
	projection := evidence.Projection
	return strings.Join(
		[]string{
			"reproduce: GOTOOLCHAIN=go1.26.5 go run ./tools/run_saturation --profile=ci --seed=20260725",
			"scope: " + evidence.Scope,
			fmt.Sprintf(
				"accounting: %d jobs = %d service + %d control + %d global probe; reconciled",
				projection.Accounting.TotalJobs,
				projection.Accounting.ServiceSubmissions,
				projection.Accounting.ControlSubmissions,
				projection.Accounting.GlobalProbeSubmissions,
			),
			fmt.Sprintf(
				"service: %d admitted -> %d completed + %d canceled + %d queue deadline; %d rejected",
				projection.ServiceTotals.Admitted,
				projection.ServiceTotals.Completed,
				projection.ServiceTotals.Canceled,
				projection.ServiceTotals.DeadlineExceeded,
				projection.ServiceTotals.Rejected,
			),
			fmt.Sprintf(
				"tenant capacity: %d x HTTP %d; upstream requests %d",
				projection.CapacityProbes.Tenant.Rejected,
				projection.CapacityProbes.Tenant.StatusCode,
				projection.CapacityProbes.Tenant.UpstreamRequests,
			),
			fmt.Sprintf(
				"global capacity: %d x HTTP %d; upstream requests %d",
				projection.CapacityProbes.Global.Rejected,
				projection.CapacityProbes.Global.StatusCode,
				projection.CapacityProbes.Global.UpstreamRequests,
			),
			fmt.Sprintf(
				"dispatch: %d observed == %d independent WDRR oracle",
				len(projection.Service.Observed),
				len(projection.Service.Expected),
			),
			fmt.Sprintf(
				"configured bounds: %d ms execution; <= %d ms later cleanup; separate contexts",
				projection.Timeouts.ExecutionContextMS,
				projection.Timeouts.MaximumCleanupMS,
			),
			"categorical sha256: " + evidence.Categorical.Digest,
			"performance: false; measured diagnostic intervals excluded; no fairness, service-share, latency, or RSS claim",
			"",
		},
		"\n",
	)
}

func renderSaturationTerminalSVG(summary string) string {
	lines := strings.Split(strings.TrimSuffix(summary, "\n"), "\n")
	var body strings.Builder
	body.WriteString(`  <rect width="1400" height="470" rx="18" fill="#0d1117"/>` + "\n")
	body.WriteString(`  <circle cx="31" cy="29" r="7" fill="#ff5f56"/>` + "\n")
	body.WriteString(`  <circle cx="55" cy="29" r="7" fill="#ffbd2e"/>` + "\n")
	body.WriteString(`  <circle cx="79" cy="29" r="7" fill="#27c93f"/>` + "\n")
	body.WriteString(
		`  <text x="700" y="36" fill="#8b949e" text-anchor="middle" ` +
			`font-family="DejaVu Sans Mono, ui-monospace, SFMono-Regular, Consolas, monospace" ` +
			`font-size="15">verified saturation projection without measured diagnostic intervals</text>` + "\n",
	)
	body.WriteString(`  <line x1="0" y1="54" x2="1400" y2="54" stroke="#30363d"/>` + "\n")
	for index, line := range lines {
		colour := "#e6edf3"
		if index == 0 {
			colour = "#7ee787"
		}
		if index == len(lines)-1 {
			colour = "#d2a8ff"
		}
		_, _ = fmt.Fprintf(
			&body,
			"  <text x=\"48\" y=\"%d\" fill=\"%s\" "+
				"font-family=\"DejaVu Sans Mono, ui-monospace, SFMono-Regular, "+
				"Consolas, monospace\" font-size=\"14\">%s</text>\n",
			86+index*36,
			colour,
			html.EscapeString(line),
		)
	}
	return visualSVGDocument(
		1400,
		470,
		"SSEmaphore bounded saturation evidence",
		"A terminal summary derived from one real fixed-seed production-path saturation run. It reports categorical accounting, capacity probes, oracle agreement, and explicit non-performance boundaries.",
		body.String(),
	)
}

func renderSaturationOutcomesSVG(evidence saturationVisualEvidence) string {
	projection := evidence.Projection
	outcomes := projection.ServiceTotals
	segmentWidth := 40
	completedWidth := int(outcomes.Completed) * segmentWidth
	canceledWidth := int(outcomes.Canceled) * segmentWidth
	deadlineWidth := int(outcomes.DeadlineExceeded) * segmentWidth
	rejectedWidth := int(outcomes.Rejected) * segmentWidth
	tenantA := projection.Tenants[0]
	tenantB := projection.Tenants[1]
	body := fmt.Sprintf(`  <rect width="1200" height="650" rx="18" fill="#f6f8fa"/>
  <text x="54" y="55" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="28" font-weight="700">Exact request-count outcomes from one bounded run</text>
  <text x="54" y="84" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="16">26 service submissions plus one control and one dedicated global-capacity probe; fixed seed 20260725.</text>

  <text x="80" y="137" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="17" font-weight="700">service submissions = %d</text>
  <rect x="80" y="158" width="%d" height="58" rx="9" fill="#2da44e"/>
  <rect x="%d" y="158" width="%d" height="58" fill="#bf8700"/>
  <rect x="%d" y="158" width="%d" height="58" fill="#8250df"/>
  <rect x="%d" y="158" width="%d" height="58" rx="9" fill="#cf222e"/>
  <text x="%d" y="194" text-anchor="middle" fill="#0d1117" font-family="DejaVu Sans, Arial, sans-serif" font-size="15" font-weight="700">%d completed</text>
  <text x="%d" y="194" text-anchor="middle" fill="#0d1117" font-family="DejaVu Sans, Arial, sans-serif" font-size="14" font-weight="700">%d cancel</text>
  <text x="%d" y="194" text-anchor="middle" fill="#ffffff" font-family="DejaVu Sans, Arial, sans-serif" font-size="14" font-weight="700">%d deadline</text>
  <text x="%d" y="194" text-anchor="middle" fill="#ffffff" font-family="DejaVu Sans, Arial, sans-serif" font-size="14" font-weight="700">%d reject</text>

  <rect x="54" y="270" width="336" height="244" rx="16" fill="#ffffff" stroke="#0969da" stroke-width="2"/>
  <text x="82" y="310" fill="#0550ae" font-family="DejaVu Sans, Arial, sans-serif" font-size="20" font-weight="700">%s - weight %d</text>
  <text x="82" y="348" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">submitted             %d</text>
  <text x="82" y="378" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">admitted              %d</text>
  <text x="82" y="408" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">complete/cancel/dead  %d / %d / %d</text>
  <text x="82" y="438" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">rejected              %d</text>
  <text x="82" y="468" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">dispatches/work units %d / %d</text>

  <rect x="432" y="270" width="336" height="244" rx="16" fill="#ffffff" stroke="#8250df" stroke-width="2"/>
  <text x="460" y="310" fill="#6639ba" font-family="DejaVu Sans, Arial, sans-serif" font-size="20" font-weight="700">%s - weight %d</text>
  <text x="460" y="348" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">submitted             %d</text>
  <text x="460" y="378" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">admitted              %d</text>
  <text x="460" y="408" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">complete/cancel/dead  %d / %d / %d</text>
  <text x="460" y="438" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">rejected              %d</text>
  <text x="460" y="468" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">dispatches/work units %d / %d</text>

  <rect x="810" y="270" width="336" height="244" rx="16" fill="#ffffff" stroke="#57606a" stroke-width="2"/>
  <text x="838" y="310" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="20" font-weight="700">Boundary probes and control</text>
  <text x="838" y="350" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">tenant capacity  %d x %d</text>
  <text x="838" y="380" fill="#57606a" font-family="DejaVu Sans Mono, monospace" font-size="15">upstream requests      %d</text>
  <text x="838" y="420" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">global capacity  %d x %d</text>
  <text x="838" y="450" fill="#57606a" font-family="DejaVu Sans Mono, monospace" font-size="15">upstream requests      %d</text>
  <text x="838" y="490" fill="#24292f" font-family="DejaVu Sans Mono, monospace" font-size="16">control complete/upstream %d / %d</text>

  <rect x="54" y="550" width="1092" height="58" rx="12" fill="#fff8c5" stroke="#d4a72c"/>
  <text x="600" y="585" text-anchor="middle" fill="#633c01" font-family="DejaVu Sans, Arial, sans-serif" font-size="16">Request-count accounting only - not throughput, latency, RSS, fairness score, or service-share evidence.</text>
`,
		outcomes.Submitted,
		completedWidth,
		80+completedWidth,
		canceledWidth,
		80+completedWidth+canceledWidth,
		deadlineWidth,
		80+completedWidth+canceledWidth+deadlineWidth,
		rejectedWidth,
		80+completedWidth/2,
		outcomes.Completed,
		80+completedWidth+canceledWidth/2,
		outcomes.Canceled,
		80+completedWidth+canceledWidth+deadlineWidth/2,
		outcomes.DeadlineExceeded,
		80+completedWidth+canceledWidth+deadlineWidth+rejectedWidth/2,
		outcomes.Rejected,
		html.EscapeString(tenantA.Tenant),
		tenantA.Weight,
		tenantA.Submitted,
		tenantA.Admitted,
		tenantA.Completed,
		tenantA.Canceled,
		tenantA.DeadlineExceeded,
		tenantA.Rejected,
		tenantA.DispatchedRequests,
		tenantA.DispatchedWorkUnits,
		html.EscapeString(tenantB.Tenant),
		tenantB.Weight,
		tenantB.Submitted,
		tenantB.Admitted,
		tenantB.Completed,
		tenantB.Canceled,
		tenantB.DeadlineExceeded,
		tenantB.Rejected,
		tenantB.DispatchedRequests,
		tenantB.DispatchedWorkUnits,
		projection.CapacityProbes.Tenant.Rejected,
		projection.CapacityProbes.Tenant.StatusCode,
		projection.CapacityProbes.Tenant.UpstreamRequests,
		projection.CapacityProbes.Global.Rejected,
		projection.CapacityProbes.Global.StatusCode,
		projection.CapacityProbes.Global.UpstreamRequests,
		projection.Control.Completed,
		projection.Control.UpstreamRequests,
	)
	return visualSVGDocument(
		1200,
		650,
		"SSEmaphore exact saturation outcomes",
		"A stacked request-count outcome chart with exact per-tenant accounting and zero-upstream tenant and global capacity probes from one fixed-seed run.",
		body,
	)
}

func renderSaturationDispatchSVG(evidence saturationVisualEvidence) string {
	dispatches := evidence.Projection.Service.Observed
	expectedDispatches := evidence.Projection.Service.Expected
	var bars strings.Builder
	for index, dispatch := range dispatches {
		expected := expectedDispatches[index]
		x := 100 + index*62
		height := int(dispatch.WorkUnits) * 2
		y := 535 - height
		oracleX := 100 + (int(expected.Position)-1)*62 + 18
		oracleY := 535 - int(expected.WorkUnits)*2
		fill := "#0969da"
		if dispatch.Tenant == "tenant-b" {
			fill = "#8250df"
		}
		stroke := "#ffffff"
		strokeWidth := 1
		if dispatch.Mode == "sse" {
			stroke = "#bf8700"
			strokeWidth = 4
		}
		_, _ = fmt.Fprintf(
			&bars,
			"  <rect x=\"%d\" y=\"%d\" width=\"36\" height=\"%d\" rx=\"5\" "+
				"fill=\"%s\" stroke=\"%s\" stroke-width=\"%d\"/>\n",
			x,
			y,
			height,
			fill,
			stroke,
			strokeWidth,
		)
		_, _ = fmt.Fprintf(
			&bars,
			"  <circle cx=\"%d\" cy=\"%d\" r=\"4\" fill=\"#ffffff\" stroke=\"#24292f\"/>\n",
			oracleX,
			oracleY,
		)
		_, _ = fmt.Fprintf(
			&bars,
			"  <text x=\"%d\" y=\"%d\" text-anchor=\"middle\" fill=\"#24292f\" "+
				"font-family=\"DejaVu Sans Mono, monospace\" font-size=\"12\">%d</text>\n",
			x+18,
			y-10,
			dispatch.WorkUnits,
		)
		_, _ = fmt.Fprintf(
			&bars,
			"  <text x=\"%d\" y=\"561\" text-anchor=\"middle\" fill=\"#57606a\" "+
				"font-family=\"DejaVu Sans Mono, monospace\" font-size=\"12\">%d</text>\n",
			x+18,
			dispatch.Position,
		)
	}
	body := fmt.Sprintf(`  <rect width="1400" height="720" rx="18" fill="#f6f8fa"/>
  <text x="54" y="55" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="28" font-weight="700">Seeded WDRR dispatch trace - expected equals observed</text>
  <text x="54" y="84" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="16">20 production-path service dispatches; bar height is the bounded scheduling estimate in work units.</text>

  <rect x="54" y="105" width="18" height="18" rx="4" fill="#0969da"/>
  <text x="82" y="120" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">tenant-a - weight 1</text>
  <rect x="250" y="105" width="18" height="18" rx="4" fill="#8250df"/>
  <text x="278" y="120" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">tenant-b - weight 3</text>
  <rect x="446" y="103" width="22" height="22" rx="4" fill="none" stroke="#bf8700" stroke-width="4"/>
  <text x="480" y="120" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">SSE request</text>
  <circle cx="628" cy="114" r="4" fill="#ffffff" stroke="#24292f"/>
  <text x="642" y="120" fill="#24292f" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">independent oracle point - coincident on all 20</text>
  <rect x="1070" y="96" width="276" height="38" rx="12" fill="#dafbe1" stroke="#2da44e"/>
  <text x="1208" y="120" text-anchor="middle" fill="#116329" font-family="DejaVu Sans, Arial, sans-serif" font-size="15" font-weight="700">ORACLE MATCH 20 / 20</text>

  <line x1="88" y1="535" x2="1332" y2="535" stroke="#57606a" stroke-width="2"/>
  <line x1="88" y1="375" x2="1332" y2="375" stroke="#d0d7de" stroke-dasharray="5 6"/>
  <line x1="88" y1="215" x2="1332" y2="215" stroke="#d0d7de" stroke-dasharray="5 6"/>
  <text x="76" y="540" text-anchor="end" fill="#57606a" font-family="DejaVu Sans Mono, monospace" font-size="13">0</text>
  <text x="76" y="380" text-anchor="end" fill="#57606a" font-family="DejaVu Sans Mono, monospace" font-size="13">80</text>
  <text x="76" y="220" text-anchor="end" fill="#57606a" font-family="DejaVu Sans Mono, monospace" font-size="13">160</text>
  <text x="29" y="360" transform="rotate(-90 29 360)" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">estimated work units</text>
%s
  <text x="710" y="589" text-anchor="middle" fill="#57606a" font-family="DejaVu Sans, Arial, sans-serif" font-size="14">observed dispatch position</text>
  <rect x="54" y="620" width="1292" height="58" rx="12" fill="#fff8c5" stroke="#d4a72c"/>
  <text x="700" y="655" text-anchor="middle" fill="#633c01" font-family="DejaVu Sans, Arial, sans-serif" font-size="15">Weights are inputs. This finite trace is not a 3:1 allocation, fairness score, throughput, or latency result.</text>
`, bars.String())
	return visualSVGDocument(
		1400,
		720,
		"SSEmaphore fixed-seed dispatch trace",
		"Twenty observed weighted deficit round-robin dispatches plotted by estimated work units and tenant, with independent oracle points coincident at every position. Two SSE requests are outlined.",
		body,
	)
}
