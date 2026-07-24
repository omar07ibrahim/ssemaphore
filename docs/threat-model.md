# Threat model

This threat model covers the SSEmaphore v0.1 target. It distinguishes controls
the gateway can prove from properties that remain owned by its deployment or
inference upstream.

> **Current checkpoint:** strict request parsing, bounded admission, bearer to
> tenant mapping, pre-dispatch slots, fixed-destination upstream transport,
> bounded inbound HTTP serving, strict Linux policy loading, loopback listener
> selection, signal-owned invocation, and a bounded-read-ahead SSE relay are
> implemented as one runnable path. Telemetry and persistence below remain
> target controls.

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

Implemented controls: a concrete numeric-loopback TCP or Unix-stream listener
boundary; an exact accepted-connection cap; an 8--64 KiB header wire envelope;
finite header, total-read, write, and idle deadlines; nonblocking per-tenant and
global pre-dispatch request slots; `http.MaxBytesReader`; strict single-value
JSON parsing; duplicate-key and invalid-UTF-8 rejection; checked reservation
arithmetic; bounded collections; and cancellation-aware decode. Validation
completes before queue insertion. The application cap does not bound the
kernel listen backlog or bandwidth consumed before the trusted local edge.
HTTP/1 is the only enabled protocol; TLS termination and public-edge protection
remain deployment responsibilities.

### Queue capture and unfair dispatch

One tenant may fill global capacity, split work into many small requests, send
one request larger than a scheduler quantum, or exploit nondeterministic map
order.

Controls: global and per-tenant request, body-byte, and reservation limits;
FIFO order inside a tenant; stable tenant ordering; carried DRR deficit;
positive bounded weights; checked cost and deficit arithmetic; deficit reset
for an empty queue; and a funded-head barrier when global work is fragmented.
Adversarial tests verify that later small requests cannot indefinitely bypass a
large funded head, and an independent oracle checks randomized traces. Fairness
is stated only in bounded estimated-service units, never inferred GPU cost.

### Credential confusion and SSRF

A client may try to forward its bearer token, select a new upstream, inject
hop-by-hop headers, or use proxy-related environment variables.

Implemented controls: client credentials terminate at ingress, only SHA-256
digests of those tenant credentials are retained after construction,
credentials map immutably to a configured tenant, exact paths cannot contain an
authority or query, and the injected upstream receives no inbound header,
credential, URL, or response writer. The concrete transport accepts its
upstream credential separately from its serializable policy, validates one
fixed URL, permits plaintext only to a numeric loopback address, disables
redirects and environment proxies, disables transparent compression, and
constructs outbound headers from an allowlist.
The raw upstream credential must remain in the private transport object for the
lifetime of that client because every request needs it; it is never returned in
an error or included in the policy value. The outbound request context retains
the caller's cancellation and deadline but drops caller values, preventing an
installed `httptrace` callback from observing the authorization header or
blocking a transport callback.

The executable policy contains environment-variable names rather than token
values. It is read only from an effective-user-owned regular file with exact
mode `0600`; ambiguous JSON, symlinks, special files, unsafe ancestors, and
file-size changes fail closed. Every configured variable is read once and
unset in the child process before bind. Resolved tenant and upstream values
must all differ. Unsetting and overwriting Go string fields reduce accidental
inheritance and retention but are not memory-erasure or `/proc` guarantees.

### Slow, malformed, or malicious upstream

The upstream may hang before headers, stream forever, emit an oversized event,
truncate JSON, omit `[DONE]`, lie about content type, or return sensitive
headers.

Implemented controls: one finite upstream deadline; exact status, content-type,
and content-encoding checks; a 16 MiB hard response ceiling above the lower
configured limit; UTF-8, Unicode escape, nesting, duplicate-key,
trailing-value, and exact object checks; no upstream response headers; one
terminal cleanup owner; and no retry. Non-streaming JSON is fully validated
before commitment. SSE adds finite total-byte, event-byte, event-count,
read-idle, event, and total-stream limits; strict single-field framing; one
retained event; chunk-before-terminal ordering; and clean EOF verification
before `[DONE]` is flushed. The transport additionally bounds connect,
TLS-handshake, response-header, idle-connection, header-byte, and
connection-count resources; disables parallel IPv4/IPv6 fallback, redirects,
and decompression; and requires TLS 1.2 or newer for every HTTPS connection.

### Client disconnects and slow readers

A disconnected client may leave upstream work running. A slow reader may block
the relay and retain an in-flight reservation indefinitely.

Implemented controls: the scheduler permit context derives from the incoming
request context, the injected upstream must honor that context, a canceled
blocked body is closed, and the worker always calls `Finish` before releasing
in-flight accounting. The inbound server derives a finite write deadline that
covers body read, queue residence, upstream work, and the final response-write
allowance; a real blocked flush test reaches that deadline. Tests observe
queued and in-flight cancellation and prove client cancellation writes no
error response. A socket deadline cannot preempt CPU-bound code, and no claim
is made about GPU resource reclamation after HTTP cancellation.

### Retry and response confusion

An automatic retry can duplicate expensive work, and a gateway cannot replace
an HTTP status after the first response byte.

Controls: v0.1 attempts the upstream exactly once. The concrete transport is
HTTP/1-only and sends a non-replayable POST body with no `GetBody` function, so
Go cannot retry it after a stale connection or protocol failure. Redirects are
also disabled. State records whether the response was committed. Before
commitment, failures use the documented JSON envelope; after commitment,
upstream is canceled and the connection closes without a synthetic `[DONE]`;
only a private terminal reason is recorded.

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

Controls: one idempotent server-owned cleanup drains admission and the HTTP
listener on an independent grace deadline. If that phase fails or expires, it
seals new inner handler work, commits `ForceCancelInflight` before canceling
HTTP contexts, closes connections, and waits on both tracked handlers and
scheduler accounting under a separate force deadline. A caller context bounds
only that caller's wait and cannot abort cleanup. The handler and server must
own the same scheduler instance. Tests prove terminal `shutdown` attribution,
exact result counters, concurrent-caller identity, and zero scheduler counters
before owner close; uncooperative work yields static `ErrShutdownIncomplete`.

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
