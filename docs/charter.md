# SSEmaphore v0.1 charter

This document defines the v0.1 target and the claims that its evidence must
support. A feature is out of scope unless it strengthens bounded admission,
request lifecycle correctness, or the observability of those decisions.

> **Implementation checkpoint:** the request contract, admission scheduler,
> injected non-streaming HTTP lifecycle, and fixed-destination upstream HTTP
> transport now run behind a bounded inbound server lifecycle. The repository
> does not yet contain a command, configuration loader, signal wiring,
> streaming relay, telemetry, or restart journal. Controls below that depend on
> those components remain release targets rather than current claims.

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
serves an already-created local listener. A later executable must complete all
configuration validation before constructing that listener. A 16 MiB hard
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
- tenant count, metric label values, lifecycle-writer queue depth, and exporter
  queue depth.

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

Actual token usage, when supplied by the upstream, is recorded separately as
bounded telemetry. It never rewrites a scheduling decision after the fact.

The queued-body counter measures the exact retained raw JSON bytes, not total
Go heap usage. Decoded messages are separately bounded by the body envelope,
message-count limit, and queued-request limit. Published load evidence reports
RSS instead of presenting the raw-body counter as a memory measurement.

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

The fairness claim is deliberately narrow: seeded traces of bounded variable
request costs must match an independent weighted-DRR oracle, and saturated
reports compare dispatched reservation-unit ratios with configured weights and
a published error bound. It is not a claim about real GPU seconds, tokens, or
end-user latency.

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

Before response commitment, a gateway failure is a typed JSON error. After a
stream begins, the HTTP status cannot be changed; the gateway cancels upstream,
closes the stream without a synthetic `[DONE]`, and records a content-free
terminal reason. It never injects a second JSON protocol into an active SSE
stream.

## Shutdown and restart

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

The later lifecycle-journal milestone uses a bounded writer queue and exposes a
drop counter. It is not tamper-proof or an exactly-once audit log. At startup,
persisted nonterminal rows are reconciled as `restart_abandoned`; this is a
recovery outcome, not a live state-machine transition. Restart never replays a
generation or resurrects a client connection.

## Required evidence

The v0.1 release is gated on all of the following:

1. strict request-contract tests use an official OpenAI SDK against the
   documented subset and negative fixtures for every unsupported field;
2. synchronized bursts distinguish per-tenant exhaustion (`429`) from global
   request, byte, or work saturation (`503`) with exact expected counts;
3. property and fuzz tests never exceed queue or in-flight limits, never
   dispatch expired work, and release every reservation exactly once;
4. randomized scheduler traces match an independent weighted-DRR oracle, and a
   seeded saturated two-tenant workload at weights `1:3` reports its bounded
   reservation-unit allocation error;
5. a queued-cancellation storm creates zero upstream attempts; one thousand
   in-flight disconnects are observed by the deterministic upstream within one
   second, followed by zero active requests after drain;
6. race-enabled tests pass and goroutine counts return to a documented bounded
   tolerance after fault scenarios;
7. restart tests reconcile interrupted journal rows without dispatching them;
8. canary prompt text and credentials are absent from logs, spans, metric
   labels, and the lifecycle database, whose event type cannot represent raw
   headers, bodies, or free-form errors;
9. a ten-minute published load scenario includes bursts, slow readers,
   upstream hangs, truncated SSE, `500`, and cancellation, with its seed,
   configuration, commit, host facts, and raw result data;
10. an optional pinned llama.cpp CPU-server report demonstrates wire
    compatibility without making throughput or GPU claims.

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
