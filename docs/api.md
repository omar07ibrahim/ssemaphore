# HTTP API subset

The current code implements a tested streaming and non-streaming compatibility
subset of the official [Chat Completions create API][chat-create]. Anything
not listed here is unsupported even if another server accepts it.

## Endpoint

```text
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <tenant credential>
```

Other methods and paths fail without contacting the upstream. Client bearer
credentials select a configured immutable tenant and are never forwarded. No
header or JSON field can override that identity. The handler passes only the
validated JSON body to its upstream interface. The implemented HTTP transport
uses a separate operator credential and one startup-validated absolute
destination ending in exactly `/v1/chat/completions`.

An optional `X-SSEmaphore-Queue-Timeout-Ms` header may request a queue timeout
shorter than the configured default. It is a positive bounded decimal integer
and is removed before dispatch. Omitting it uses the operator default.

Every request that reaches the application handler receives its own 128-bit
lowercase hexadecimal `X-Request-Id`. Client-selected request IDs are ignored
so every application identifier remains bounded and server-owned, including
when the later lifecycle event lands.

## Request JSON

The smallest implemented request is:

```json
{
  "model": "local-model",
  "messages": [
    {"role": "user", "content": "Explain bounded backpressure."}
  ],
  "max_completion_tokens": 128
}
```

Supported top-level fields:

| Field | Contract |
| --- | --- |
| `model` | Required string equal to the configured public alias. |
| `messages` | Required nonempty array within the configured count limit. |
| `max_completion_tokens` | Required positive integer within the configured limit. |
| `stream` | Optional boolean. Absent or `false` selects a buffered JSON response; `true` selects SSE. |
| `stream_options` | Rejected in both modes. |
| `n` | Optional integer; if present it must equal `1`. |

Each message has exactly `role` and `content`. `role` is one of `developer`,
`system`, `user`, or `assistant`; `content` is a JSON string. Names, arrays of
content parts, images, audio, files, tool calls, function calls, refusals, and
assistant messages with null content are not supported in v0.

Unknown fields, duplicate object keys, trailing JSON values, invalid UTF-8,
out-of-range integers, and unsupported combinations fail with `400`. The
gateway never silently strips a field and sends a semantically different
request upstream.

## Successful responses

For non-streaming calls, the upstream must return one bounded JSON object with
`object: "chat.completion"`. SSEmaphore relays the response only after it passes
the v0 response boundary; a malformed or oversized upstream response becomes a
`502` before downstream commitment.

For streaming calls, the upstream must return unencoded `text/event-stream`
with an optional UTF-8 charset. The implemented SSE subset accepts LF or CRLF
framing and exactly one lowercase `data:` field followed by one empty delimiter
line per event. Comments, empty events, multiple fields, and `event`, `id`, or
`retry` fields fail closed. A data event is either one strict JSON object whose
`object` field is exactly `chat.completion.chunk`, or the exact terminal value
`[DONE]`. At least one chunk must precede exactly one terminal event.

The relay applies finite total-wire-byte, event-byte, event-count, per-read,
per-event, and total-upstream limits. It retains only one exact-capacity event,
writes and flushes each validated chunk before decoding the next event, and
therefore propagates downstream backpressure after bounded read-ahead. A fixed
input buffer may already hold up to the smaller of 4 KiB, the event limit plus
one byte, and the total limit plus one byte. The terminal event is withheld
until clean EOF and a successful upstream body close prove that no trailing
bytes exist. A malformed first event becomes a static `502`; a failure after
the first flush truncates the SSE response without injecting JSON or
synthesizing `[DONE]`.

The handler relays no upstream headers. Buffered responses set their own
`Content-Type`, exact `Content-Length`, `Cache-Control`,
`X-Content-Type-Options`, and `X-Request-Id`. Streaming responses omit
`Content-Length`, set the same safe headers with `text/event-stream`, and flush
every event. The upstream client constructs only the `Accept`, `Authorization`,
`Content-Type`, and `User-Agent` application headers; `Accept` is selected from
the validated request mode, never from an inbound header. Redirects,
environment proxies, cookies, and transparent compression are disabled.
Plaintext destinations must be numeric loopback addresses; other destinations
require HTTPS with TLS 1.2 or newer.

The upstream transport deliberately offers HTTP/1 only. Its POST body has no
replay function, which prevents Go's transport from automatically retrying a
request. Connect, TLS-handshake, response-header, idle-connection, header-byte,
and connection-count limits are finite, while the handler context supplies the
total upstream deadline. Cancellation and deadlines cross that boundary, but
arbitrary context values do not; this prevents caller-installed HTTP trace
hooks from observing the upstream credential.

The inbound server is also HTTP/1 only. Its library accepts only an already
bound numeric-loopback TCP or Unix byte-stream listener, caps accepted
connections, enforces a hard header wire envelope, and derives total read and
write deadlines from the handler policy. The Linux command validates a strict
policy and credentials before opening its numeric-loopback listener, then owns
signal-driven graceful-to-forced shutdown.

Automatic `OPTIONS *` handling is disabled, so that request reaches the normal
application policy. HTTP/2 and h2c negotiation are not supported; an h2c
upgrade remains an ordinary HTTP/1 request.

## Errors before response commitment

Application errors use one static JSON envelope:

```json
{
  "error": {
    "code": "tenant_capacity_exhausted",
    "message": "The tenant has no request capacity available."
  }
}
```

The stable v0 codes are:

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed JSON, an invalid queue header, a contract violation, or a body read failure/deadline. |
| `401` | `invalid_tenant_credential` | Missing or unknown tenant credential. |
| `404` | `unsupported_path` | The path is outside the v0 API. |
| `405` | `unsupported_method` | The endpoint does not accept the method. |
| `413` | `request_too_large` | A configured ingress limit was crossed. |
| `415` | `unsupported_media_type` | The request is not JSON. |
| `429` | `tenant_capacity_exhausted` | A tenant request, byte, or work limit is exhausted. |
| `500` | `internal_error` | An internal invariant or required server capability failed. |
| `502` | `invalid_upstream_response` | The upstream call, metadata, body, or close failed before response commitment. |
| `503` | `overloaded` | A global request, byte, or work limit is exhausted. |
| `503` | `queue_deadline_exceeded` | The request expired before upstream dispatch. |
| `503` | `draining` | The process is no longer accepting work. |
| `504` | `upstream_timeout` | A dispatched upstream timed out before commitment. |

`401` includes `WWW-Authenticate: Bearer`, and `405` includes `Allow: POST`.
SSEmaphore does not emit `Retry-After` until it can calculate an honest bounded
estimate. A non-streaming response is fully buffered before commit. A streaming
response commits only after its first complete chunk validates. After either
commit boundary, a downstream write failure records a downstream failure and
never appends a second JSON protocol.

Some requests fail inside `net/http` before the application handler exists.
Malformed input can therefore receive Go's plain built-in `400`, and a request
one byte beyond the configured header envelope receives its plain `431`.
Likewise, the server's HTTP/2 preface guard emits a body-free `505`, and a rare
request crossing the forced-shutdown dispatch boundary receives a body-free
`503`. These transport-level responses have no application request ID or JSON
envelope, their body shape is outside the API contract, and they never contact
the upstream.

## Compatibility statement

OpenAI documents that optional fields and new streaming event types can be
added compatibly to its `v1` API. SSEmaphore therefore versions this smaller
allowlist independently and will not claim full OpenAI API compatibility. A
future subset change requires contract fixtures and an explicit changelog.

[chat-create]: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
