# Threat model

This threat model covers the SSEmaphore v0.1 target. It distinguishes controls
the gateway can prove from properties that remain owned by its deployment or
inference upstream.

## Assets

- service availability and bounded memory, goroutines, file descriptors, and
  queue capacity;
- configured tenant and upstream credentials;
- tenant isolation and the integrity of scheduler weights and limits;
- prompt and completion confidentiality;
- lifecycle and telemetry integrity;
- the rule that one accepted request produces at most one upstream attempt.

## Trust boundaries

```text
untrusted client
    |  tenant credential, headers, JSON, disconnect timing
    v
SSEmaphore process
    |  configured upstream credential, bounded HTTP request
    v
untrusted-or-faulty inference upstream

operator configuration -> trusted input at process start
lifecycle database     -> local persistent state, not a content store
telemetry exporter     -> separate sink with bounded attributes
```

An authenticated tenant is still adversarial. Tenant identity comes only from
the authentication result, never `X-Tenant-ID`, `user`, `model`, or another
client-controlled value. The upstream is not trusted to
respect body sizes, SSE framing, response time, header hygiene, or usage
accounting. A local operator can misconfigure limits; the process must reject
invalid or internally inconsistent configuration before listening.

## Threats and target controls

### Resource exhaustion at ingress

Threats include slow headers, oversized bodies, JSON bombs, excessive message
counts, integer overflow, duplicate keys, and clients that disconnect during
decode.

Controls: finite accepted-connection, server header, body-read, and total
request limits; `http.MaxBytesReader`; strict single-value JSON parsing;
duplicate-key and invalid-UTF-8 rejection; checked reservation arithmetic;
bounded collections; and cancellation-aware decode. Validation completes
before queue insertion.

### Queue capture and unfair dispatch

One tenant may fill global capacity, split work into many small requests, send
one request larger than a scheduler quantum, or exploit nondeterministic map
order.

Controls: global and per-tenant request, body-byte, and reservation limits;
FIFO order inside a tenant; stable tenant ordering; carried DRR deficit;
positive bounded weights; checked cost and deficit arithmetic; deficit reset
for an empty queue; and property tests over adversarial sizes and arrival
schedules. An independent oracle checks randomized traces. Fairness is stated
only in bounded estimated-service units, never inferred GPU cost.

### Credential confusion and SSRF

A client may try to forward its bearer token, select a new upstream, inject
hop-by-hop headers, or use proxy-related environment variables.

Controls: client credentials terminate at ingress and map immutably to a
configured tenant; a separate upstream credential is loaded from runtime
configuration; the upstream URL is parsed and validated once at startup;
request paths cannot contain an authority; redirects are disabled;
`http.Transport.Proxy` is `nil`; and forwarded request and response headers use
allowlists.

### Slow, malformed, or malicious upstream

The upstream may hang before headers, stream forever, emit an oversized event,
truncate JSON, omit `[DONE]`, lie about content type, or return sensitive
headers.

Controls: distinct connect, response-header, idle-event, total-stream, event,
and total-response limits; bounded SSE parsing without the default
`bufio.Scanner` token limit; UTF-8 and framing checks; content-type checks;
transparent compression disabled; encoded responses rejected; response header
allowlists; one terminal owner for cleanup; no retry in v0.1.

### Client disconnects and slow readers

A disconnected client may leave upstream work running. A slow reader may block
the relay and retain an in-flight reservation indefinitely.

Controls: the upstream request derives from the incoming request context; Go
cancels an incoming request context when the client connection closes. Every
downstream write and flush receives a refreshed bounded write deadline, and a
total request deadline prevents indefinite slot retention. Tests observe
cancellation at the deterministic upstream. No claim is made about GPU resource
reclamation after HTTP cancellation.

### Retry and response confusion

An automatic retry can duplicate expensive work, and a gateway cannot replace
an HTTP status after the first response byte.

Controls: v0.1 attempts the upstream exactly once. State records whether the
response was committed. Before commitment, failures use the documented JSON
envelope; after commitment, upstream is canceled and the connection closes
without a synthetic `[DONE]`; only a private terminal reason is recorded.

### Content and credential leakage

Prompts, completions, authorization headers, or API keys may leak through logs,
span attributes, metric labels, error messages, request hashes, or the journal.

Controls: none of those surfaces record bodies or credentials. The lifecycle
event is a closed type containing fixed-size request and tenant identifiers,
numeric policy revision, bounded unit counts, timestamps, and terminal enums;
it cannot hold arbitrary strings, maps, headers, bodies, or raw errors. It
stores no body hash because low-entropy prompts can be guessed. Metric labels
use fixed bounded enums and never model, tenant, or request IDs. Export queues
are bounded and expose dropped-event counters. Telemetry tests inject canary
secrets and scan every sink.

### Journal corruption and restart ambiguity

A crash may leave reservations marked in flight, while a damaged database may
produce inconsistent recovery decisions.

Controls: a bounded lifecycle-writer queue with an explicit dropped counter;
schema and policy versions; SQLite transactions; integrity checks at startup;
fail-closed recovery; and an idempotent reconciliation that marks nonterminal
rows abandoned without replay. The journal is best-effort, not tamper-proof or
exactly once, and crash-tail loss remains possible. WAL mode is permitted only
with a SQLite version that contains the 2026 WAL-reset corruption fix or an
applicable backport, and the deployment keeps the database, WAL, and
shared-memory files on one local filesystem.

### Shutdown races

New admission during drain could create work that outlives the grace period,
while queued or active work could retain capacity after cancellation.

Controls: drain atomically stops admission; queued jobs transition to terminal
`shutdown`; active jobs receive a bounded grace period; and their contexts are
canceled at expiry. Tests assert that all request, byte, work, and telemetry
queue counters return to their terminal values.

## Explicitly unmitigated in v0.1

- compromise of the host, operator account, front proxy, or telemetry backend;
- malicious model output beyond transport and size validation;
- provider-side retention or logging;
- denial of service that exhausts bandwidth before the trusted edge;
- distributed coordination, multi-node replay, or regional failover;
- TLS termination, certificate management, or public-internet edge defense;
- proof that an upstream canceled GPU kernels after its HTTP context ended.

These are deployment or upstream properties and must not be presented as
SSEmaphore guarantees.
