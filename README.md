# SSEmaphore

SSEmaphore is a single-node, privacy-preserving admission-control laboratory
for a deliberately narrow subset of the OpenAI Chat Completions HTTP API. It
turns tenant weights, bounded estimated work, queue deadlines, and client
disconnects into explicit lifecycle decisions before an inference upstream is
overloaded.

> **Status:** the strict non-streaming request boundary and bounded admission
> scheduler are implemented. The HTTP handler and upstream relay have not
> landed, so there is no runnable gateway yet.

## Implemented now

The Go request parser and admission scheduler enforce the first two proof
boundaries:

- a 16 MiB hard body ceiling above the lower operator-configured limit;
- raw invalid UTF-8, unpaired Unicode surrogates, duplicate keys at any depth,
  trailing values, unknown fields, and non-decimal integers fail closed;
- message count, decoded text bytes, completion tokens, and checked reservation
  arithmetic are independently bounded;
- validated requests keep an exact-capacity body and expose only copy or
  read-only accessors;
- table, race, 32-bit, and corpus-seeded fuzz tests cover the parser boundary.

The scheduler adds:

- startup validation for every queue, in-flight, quantum, deficit, tenant, and
  scheduler-work bound, including hard caps on collection and funding work;
- tenant-first and global queue decisions over count, exact body bytes, and
  estimated work, followed by count-and-work in-flight enforcement;
- config-order, per-tenant FIFO weighted DRR with carried bounded deficit and a
  funded-head barrier that prevents small requests starving a large request
  when global work capacity is fragmented;
- absolute queue deadlines fixed before mailbox admission, client-cancellation
  attribution, exact-once accounting release, and graceful-then-forced drain;
- golden traces, an independent seeded DRR oracle, adversarial fragmentation
  tests, deterministic fake-clock races, race detection, and 32-bit tests.

Streaming is deliberately rejected by this first implementation slice. The
target v0.1 contract below remains broader than the code that exists today.

## The research question

An inference server can be healthy while its callers are already building an
unbounded queue. At that point, accepting every request hides overload as
latency, lets large tenants crowd out small ones, and makes disconnects or
expired work consume capacity that no client can use.

Can a deliberately small gateway make resource bounds, estimated-service
fairness, cancellation races, and content-free telemetry externally testable?
SSEmaphore puts one inspectable control point in front of one upstream:

```text
client
  -> strict bounded ingress
  -> per-tenant queues
  -> estimated-service weighted deficit scheduler
  -> one Chat Completions-compatible upstream
  -> bounded JSON or SSE relay
```

The v0.1 target is intentionally not a provider broker or a generic reverse
proxy. Its value is the failure behavior: deterministic admission, typed
rejection, cancellation propagation, no retry after response commitment,
privacy-safe telemetry, and restart reconciliation.

## Target v0.1 boundary

- `POST /v1/chat/completions`, streaming and non-streaming;
- one configured model alias and one fixed upstream;
- text-only messages and `n = 1`;
- bounded connections, headers, body bytes, message count, queued bytes,
  estimated work, responses, telemetry, and in-flight work;
- configured bearer credentials that map immutably to tenants;
- weighted deficit round robin over bounded estimated service cost;
- queue expiry before dispatch and client cancellation after dispatch;
- no automatic retry in v0.1;
- a content-free lifecycle journal, metrics, and traces;
- graceful drain and explicit reconciliation of interrupted work.

The exact request subset and rejection codes are in [the API contract](docs/api.md).
The lifecycle and evidence gates are in [the charter](docs/charter.md). Security
assumptions and abuse cases are in [the threat model](docs/threat-model.md).

## What this will prove

The release gate will include falsifiable scenarios rather than production
claims:

- a synchronized burst hits exact in-flight, queued, and rejected counts;
- queued requests that lose their deadline never reach the upstream;
- forced disconnects cancel the upstream request context;
- a slow reader cannot hold a response writer indefinitely;
- weighted tenants match an independent scheduler oracle under seeded
  variable-cost workloads;
- every reservation reaches exactly one terminal state and is released once;
- a cancellation storm creates zero post-cancellation upstream attempts;
- prompts, responses, bearer credentials, and raw API keys are absent from
  logs, spans, metrics, and the lifecycle database.

The published evidence will also report bounded RSS, gateway overhead, weighted
service ratios, queue-deadline accuracy, and dropped telemetry events with the
exact seed, configuration, commit, and host facts.

## Prior art and positioning

Existing systems already provide inference routing, prioritization, and flow
control. SSEmaphore does not claim a new scheduling algorithm or a replacement
for [LiteLLM scheduling](https://docs.litellm.ai/docs/scheduler) or the
[Kubernetes Inference Extension flow-control layer](https://gateway-api-inference-extension.sigs.k8s.io/guides/flow-control/).
Its narrower purpose is to make bounded estimated-service admission, exact
cancellation semantics, and telemetry that cannot represent prompt or response
content reviewable in one reference implementation.

## Non-goals

SSEmaphore v0.1 will not host models, route across providers or replicas,
inspect KV caches, perform billing, cache prompts, provide distributed high
availability, manage TLS, or act as a public-internet edge. It will not claim
that HTTP cancellation releases GPU memory, full OpenAI API compatibility,
exact tokenizer or GPU-cost prediction, or a real-world latency SLO.

## Design references

- the official
  [Chat Completions create reference][chat-completions-create] defines the
  upstream surface from which the smaller contract is selected;
- Go documents request-context cancellation, bounded request readers, response
  flushing, write deadlines, and graceful shutdown in [`net/http`](https://pkg.go.dev/net/http);
- the scheduler is based on Shreedhar and Varghese's
  [deficit round-robin paper](https://openscholarship.wustl.edu/cse_research/339/);
- telemetry will pin an explicit version of the
  [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/)
  while its exported event schema provides no field that can hold model input
  or output.

## License

[MIT](LICENSE)

[chat-completions-create]: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
