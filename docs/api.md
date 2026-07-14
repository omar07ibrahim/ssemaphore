# HTTP API subset

This document separates the implemented non-streaming checkpoint from later
`ssemaphore.api v0` streaming work. The current code serves a tested
compatibility subset of the official
[Chat Completions create API][chat-create]. Anything not listed here is
unsupported even if another server accepts it.

## Endpoint

```text
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <tenant credential>
```

Other methods and paths fail without contacting the upstream. Client bearer
credentials select a configured immutable tenant and are never forwarded. No
header or JSON field can override that identity. The current injected upstream
receives neither the client credential nor any destination. A later real
transport will use a separate operator credential for one fixed upstream.

An optional `X-SSEmaphore-Queue-Timeout-Ms` header may request a queue timeout
shorter than the configured default. It is a positive bounded decimal integer
and is removed before dispatch. Omitting it uses the operator default.

The handler returns its own 128-bit lowercase hexadecimal `X-Request-Id`.
Client-selected request IDs are ignored so every downstream identifier remains
bounded and server-owned, including when the later lifecycle event lands.

## Request JSON

The smallest implemented non-streaming request is:

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
| `stream` | Optional boolean; if present it must be exactly `false`. |
| `stream_options` | Rejected by the current checkpoint. |
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

Streaming is not implemented and `stream: true` fails before admission. The
target streaming milestone will require `text/event-stream`, bounded complete
events, `chat.completion.chunk` payloads, and a terminal `[DONE]` marker.

The implemented non-streaming handler relays no upstream headers. It sets only
its own `Content-Type`, exact `Content-Length`, `Cache-Control`,
`X-Content-Type-Options`, and `X-Request-Id`. Redirect, environment proxy,
transparent compression, and fixed-destination rules belong to the not-yet-
implemented real HTTP transport.

## Errors before response commitment

Errors use one static JSON envelope:

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
estimate. The current non-streaming response is fully buffered before commit;
if a downstream write then fails, the handler records a downstream failure and
does not append a second JSON error envelope.

## Compatibility statement

OpenAI documents that optional fields and new streaming event types can be
added compatibly to its `v1` API. SSEmaphore therefore versions this smaller
allowlist independently and will not claim full OpenAI API compatibility. A
future subset change requires contract fixtures and an explicit changelog.

[chat-create]: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
