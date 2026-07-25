# SSEmaphore v0.1 charter

This document defines the v0.1 target and the claims that its evidence must
support. A feature is out of scope unless it strengthens bounded admission,
request lifecycle correctness, or the observability of those decisions.

> **Implementation checkpoint:** the request contract, admission scheduler,
> injected streaming and non-streaming HTTP lifecycle, fixed-destination
> upstream HTTP transport, bounded inbound server, strict Linux policy loader,
> loopback listener selection, and signal-owned command now run as one tested
> path. The repository does not yet contain telemetry or a restart journal.
> Controls below that depend on those components remain release targets rather
> than current claims.

## Product claim

SSEmaphore will be a single-process, single-node admission-control laboratory
for one Chat Completions-compatible inference upstream. It will accept a
documented subset of `POST /v1/chat/completions`, authenticate a configured
tenant, reserve bounded estimated work, queue or reject before overload becomes
hidden latency, and relay one streaming or non-streaming response.

It is a reference implementation for production failure modes, not a claim of
production readiness. The upstream remains responsible for model execution,
tokenization, and GPU resource management.

## Trust and deployment boundary

- clients are untrusted, including authenticated tenants;
- the upstream may be slow, malformed, unavailable, or inconsistent;
- tenant and upstream credentials enter through operator configuration and are
  never accepted as repository configuration;
- a bearer credential maps to exactly one immutable tenant identity; no client
  header or JSON field can select or override a tenant;
- the upstream scheme, authority, and base path are fixed at startup and cannot
  be selected by a client;
- upstream redirects and environment-derived HTTP proxies are disabled;
- v0.1 is one process on one host; there is no distributed coordination;
- TLS termination and public edge protection belong to a trusted front proxy.

The detailed attacker model is in [threat-model.md](threat-model.md).

## Request boundary

The supported JSON subset is normative in [api.md](api.md). In particular:

- only `POST /v1/chat/completions` is served;
- the model must equal one configured public alias;
- messages are text strings with a bounded count and aggregate size;
- `max_completion_tokens` is required and bounded;
- `n` is absent or exactly `1`;
- unknown or unsupported fields fail closed instead of being silently dropped;
- client authorization terminates at the gateway; a separate configured
  credential is used upstream.

This is compatibility with a named subset, not compatibility with every
current or future OpenAI field. The official API explicitly permits new
optional fields and event types as backwards-compatible changes, so the subset
is versioned independently.

## Resource model

Every implemented limit is finite and validated before the server owns or
serves an already-created local listener. The executable completes policy,
credential, parser, scheduler, HTTP, upstream, and server validation before
constructing that listener. A 16 MiB hard
request-body ceiling is the allocation envelope for parsing;
operators may configure a lower limit but not a higher one. Semantic limits are
enforced during that bounded decode before queue admission, and all reservation
arithmetic is checked before accounting changes:

- accepted connections, request headers, header-read time, and body-read time;
- request body bytes;
- message count and aggregate text bytes;
- `max_completion_tokens`;
- queued requests, body bytes, and reservation units, globally and per tenant;
- in-flight requests and in-flight reservation units, globally and per tenant;
- upstream response bytes, individual SSE event bytes, relay buffer bytes,
  downstream write time, and total request duration;
- configured tenant count.

The planned telemetry milestone adds separate bounds for metric-label values,
the lifecycle-writer queue, and exporter queues. None of those telemetry or
persistence surfaces exists in the current executable.

Failed server construction leaves listener ownership with its caller. The
library neither creates the listener nor bounds its kernel listen backlog.
The implemented enclosing deadlines are checked sums:

```text
ReadTimeout  = HeaderReadTimeout + BodyReadTimeout
WriteTimeout = BodyReadTimeout + DefaultQueueTimeout
             + UpstreamTimeout + ResponseWriteTimeout
```

v0.1 does not embed a model tokenizer. It therefore does not call its admission
quantity a token count. For an accepted body:

```text
reservation_units = body_bytes
                  + completion_weight * max_completion_tokens
```

`completion_weight` is one positive operator-configured integer. Multiplication
and addition use checked unsigned arithmetic. A valid reservation is in
`1..max_request_units`; larger values fail before admission. Counting the exact
validated UTF-8 body is deterministic and makes extra whitespace cost the
sender that supplied it. The reservation is a scheduling proxy, not a token
count or a prediction of GPU time.

Actual token usage is not currently recorded. A future bounded telemetry
surface may record usage supplied by the upstream, but it must never rewrite a
scheduling decision after the fact.

The queued-body counter measures the exact retained raw JSON bytes, not total
Go heap usage. Decoded messages are separately bounded by the body envelope,
message-count limit, and queued-request limit. No load or RSS result is
published yet. Any future load evidence must report process RSS instead of
presenting the raw-body counter as a memory measurement.

## Admission and scheduling

Authentication and complete request validation happen before queue admission.
The authentication result, never a client-supplied tenant field, selects the
tenant queue and limits. A request is admitted only if adding its request
count, exact body bytes, and reservation units stays within both tenant and
global queue limits. Checks are count, bytes, then work; the tenant decision is
evaluated first, so tenant exhaustion wins when both scopes are full and maps
to a typed `429`. Global saturation maps to a typed `503`. No background work
is created, and v0.1 does not promise `Retry-After` without an honest wait
estimate.

Each tenant owns a FIFO queue. Active tenants are visited in configuration
order by weighted deficit round robin; map iteration never determines dispatch
order. A tenant receives `base_quantum * weight` credits on a visit, and credits
carry across visits until its head request can be dispatched. Dispatch requires
both tenant and global in-flight count and work capacity. Once a head is funded
but fragmented global work prevents it from fitting, that head reserves the
next capacity opportunity instead of being bypassed by later small requests.
This deliberately permits temporary underutilization to prevent starvation.
Weights and quanta are positive, arithmetic is checked, an empty queue resets
its deficit, and the configured cap is at least
`max_request_units - 1 + max_tenant_quantum`.

The implemented fairness check is deliberately narrow: it compares seeded
traces of bounded variable request costs with an independent weighted-DRR
oracle. A seeded saturated report comparing dispatched reservation-unit ratios
with configured weights and a published error bound remains a release
obligation. Neither the existing tests nor that future report establish
fairness in real GPU seconds, tokens, or end-user latency.

## Lifecycle

Every request follows one legal path owned by a serialized scheduler state
machine:

```text
received -> rejected
         -> queued -> terminal(queue_expired | canceled_queued | shutdown)
                   -> dispatched -> serving
                                 -> terminal(completed | canceled_inflight |
                                             upstream_failed | downstream_failed |
                                             shutdown)
```

Detailed terminal reasons, such as response-write timeout or invalid SSE, are
closed enums under those outcomes. Terminal transitions are idempotent. Queued
and in-flight counters are debited and returned exactly once, a work permit
exists only after dispatch, and terminal state cannot be replaced by a later
goroutine. At the serialized scheduler owner, canceled or expired queued work
can no longer dispatch. Dispatch creates an upstream request from the
downstream request context, so a client disconnect is observable as
cancellation upstream. This proves propagation only; it does not prove that an
inference server reclaimed accelerator work.

Queued requests carry an absolute deadline fixed when admission begins, before
the owner mailbox accepts the command, so mailbox delay consumes the timeout.
The earlier of the queue timeout and client deadline is used; a tie is
client-attributed. Expiry is checked before every dispatch, including after a
scheduler wake-up. A request that is expired or already canceled must never
reach the upstream. If cancellation races with dispatch, admission internally
finishes the permit and returns `canceled_before_start`; the worker never owns
that permit.

## Response commitment and retries

v0.1 performs no automatic retry. This makes the most important boundary easy
to audit: an upstream request is attempted at most once, and a response is
never replayed after the first downstream byte.

The implemented transport makes that promise mechanical: it enables HTTP/1
only, sends a POST body without `GetBody`, and disables redirects. Go therefore
cannot replay the request after a reused connection fails. The fixed client
also disables environment proxies and transparent decompression, and applies
finite connect, TLS-handshake, response-header, idle-connection, header-byte,
and connection-count bounds beneath the handler's total upstream deadline.
Address candidates are dialed sequentially so one counted dial cannot open two
TCP sockets through fast fallback.

The implemented non-streaming path reads at most the configured response limit
plus one byte, validates one JSON object whose `object` field is exactly
`chat.completion`, closes the upstream body, rechecks cancellation, and only
then commits a `200`. It relays no upstream header. Invalid metadata, read
failure, malformed JSON, duplicate keys, wrong object type, trailing data,
oversize data, and close failure become the same static `502` before commit.

The implemented streaming path accepts only unencoded `text/event-stream` and
retains one complete wire event at a time. Total bytes, event bytes, event
count, each upstream read, each event, and the whole upstream exchange are all
finite. Every event has exactly one lowercase `data:` field; normal payloads
are strict `chat.completion.chunk` objects. Each validated chunk is flushed
before another event is decoded. Physical read-ahead is bounded by a fixed
buffer no larger than the configured event and total envelopes. The exact
`[DONE]` event remains buffered until clean EOF and successful body close rule
out trailing data.

Before response commitment, a gateway failure is a typed JSON error. After a
stream begins, the HTTP status cannot be changed; the gateway cancels upstream,
closes the stream without a synthetic `[DONE]`, and records a content-free
terminal reason. It never injects a second JSON protocol into an active SSE
stream.

## Shutdown and planned restart reconciliation

`BeginDrain` stops new admission, rejects new requests with `503`, cancels
queued requests, and leaves dispatched requests a server-configured grace
period. On any graceful-phase failure or timeout, `ForceCancelInflight` signals
remaining permit contexts. In-flight accounting is deliberately retained until
every worker calls `Finish`, which records the shutdown terminal outcome and
releases capacity exactly once. `WaitDrained` observes that terminal accounting
state; `Close` does not create a grace timer or force cancellation itself.

The implemented inbound coordinator owns those primitives. It accepts only a
concrete numeric-loopback TCP or Unix byte-stream listener, proves that the
handler and server share one scheduler, derives HTTP deadlines from the handler
policy, and starts one graceful-to-forced cleanup on independent server-owned
contexts. `ForceCancelInflight` completes before HTTP context cancellation, so
a client-side race cannot replace terminal shutdown attribution. The caller's
context bounds waiting for the result, not the cleanup itself.

The current executable has no lifecycle journal, journal writer, database, or
restart reconciliation. A later milestone is constrained to use a bounded
writer queue with an explicit drop counter and to reconcile persisted
nonterminal rows as `restart_abandoned` without dispatching them. Those are
planned design requirements, not current controls or observed results. The
planned journal is not intended to be tamper-proof or exactly once, and restart
must never replay a generation or resurrect a client connection.

## Required evidence

The release status tracks all ten obligations below. Rows 1--9 are v0.1 gates;
row 10 is an optional compatibility report. **Implemented** means the complete
obligation has reproducible evidence in the repository; **Partial** means only
the linked subset exists; **Planned** means no qualifying result has landed. A
partial mandatory row is not release-complete.

| # | Status | Release obligation and current evidence |
| ---: | --- | --- |
| 1 | **Partial** | Strict request parsing and negative contract cases exist in the [request tests](../internal/contract/request_test.go) and [API contract](api.md); no test currently uses an official OpenAI SDK. |
| 2 | **Partial** | [Scheduler limit tests](../internal/admission/scheduler_test.go) distinguish tenant/global count, byte, and work decisions, and [handler tests](../internal/httpapi/handler_test.go) verify their HTTP mapping; the synchronized-burst report with exact aggregate counts is not implemented. |
| 3 | **Partial** | Parser/SSE fuzzing plus cancellation, expiry, and exact-release tests cover important invariants in [contract tests](../internal/contract) and [admission tests](../internal/admission); there is no property suite proving the full queue and in-flight obligation. |
| 4 | **Partial** | The [scheduler tests](../internal/admission/scheduler_test.go) compare randomized traces with an independent weighted-DRR oracle; no published saturated `1:3` allocation report or error bound exists. |
| 5 | **Partial** | Current [handler](../internal/httpapi) and [server](../internal/server) tests cover queued/in-flight cancellation and drain, while the [loopback evidence](visuals/generated/loopback-evidence.json) covers one in-flight stream; the specified storm and one-thousand-disconnect result do not exist. |
| 6 | **Partial** | A race-enabled lane is configured in [CI](../.github/workflows/ci.yml); no committed run receipt or bounded goroutine-return measurement covers the full obligation. |
| 7 | **Planned** | No lifecycle journal, database, or restart-reconciliation code exists, so interrupted-row recovery has no test evidence. |
| 8 | **Partial** | Canary and redaction tests cover current static errors, credentials, and generated evidence in [application tests](../internal/app) and [visual tests](../tools/render_readme_visuals/render_test.go); spans, metric labels, exporter queues, and a lifecycle database do not exist and therefore have no canary result. |
| 9 | **Planned** | No ten-minute load run, RSS report, seed, host-fact record, or raw load dataset is published. |
| 10 | **Planned** | No pinned llama.cpp CPU-server compatibility report is published; this obligation remains optional and cannot support a current compatibility claim. |

## Non-goals

- model hosting or tokenizer ownership;
- semantic, cost, replica, or KV-cache-aware routing;
- multiple providers, automatic failover, or retries;
- prompt caching, billing, quotas sold as a service, or content moderation;
- distributed queues, leader election, or high availability;
- restoring HTTP streams after a process restart;
- inspecting or storing prompts and completions;
- TLS or certificate management and public-internet edge protection;
- a durable or tamper-evident audit log;
- claiming GPU reclamation, production readiness, or a universal SLO.
