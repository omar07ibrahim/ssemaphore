# HTTP API subset

This is the target contract for `ssemaphore.api v0`. The implementation will
serve a tested compatibility subset of the official
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
header or JSON field can override that identity. The gateway uses a separate
operator-configured upstream credential for one fixed upstream.

An optional `X-SSEmaphore-Queue-Timeout-Ms` header may request a queue timeout
shorter than the configured default. It is a positive bounded decimal integer
and is removed before dispatch. Omitting it uses the operator default.

The gateway returns its own opaque `X-Request-Id`. Client-selected request IDs
are outside the v0 subset because an arbitrary client string cannot enter the
content-free lifecycle event type.

## Request JSON

The smallest streaming request is:

```json
{
  "model": "local-model",
  "messages": [
    {"role": "user", "content": "Explain bounded backpressure."}
  ],
  "max_completion_tokens": 128,
  "stream": true,
  "stream_options": {"include_usage": true}
}
```

Supported top-level fields:

| Field | Contract |
| --- | --- |
| `model` | Required string equal to the configured public alias. |
| `messages` | Required nonempty array within the configured count limit. |
| `max_completion_tokens` | Required positive integer within the configured limit. |
| `stream` | Optional boolean; defaults to `false`. |
| `stream_options` | Optional only when streaming; only `include_usage` is accepted. |
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

For streaming calls, the upstream must return `text/event-stream`. Each bounded
SSE event contains one `data:` payload holding either a
`chat.completion.chunk` JSON object or the terminal `[DONE]` marker. The gateway
flushes complete events, never partial JSON. If `include_usage` is requested,
the official contract permits a final usage chunk before `[DONE]`; an
interrupted stream may not contain that usage total.

Only an allowlist of end-to-end headers is relayed. Hop-by-hop headers,
upstream cookies, upstream authentication metadata, and internal server headers
are removed. Redirects are not followed, environment proxy settings are
ignored, transparent compression is disabled, and encoded upstream bodies are
rejected.

## Errors before response commitment

Errors use one JSON envelope:

```json
{
  "error": {
    "message": "tenant admission capacity is exhausted",
    "type": "ssemaphore_error",
    "param": null,
    "code": "tenant_capacity_exhausted"
  }
}
```

The stable v0 codes are:

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed JSON or a contract violation. |
| `401` | `invalid_tenant_credential` | Missing or unknown tenant credential. |
| `404` | `unsupported_path` | The path is outside the v0 API. |
| `405` | `unsupported_method` | The endpoint does not accept the method. |
| `413` | `request_too_large` | A configured ingress limit was crossed. |
| `415` | `unsupported_media_type` | The request is not JSON. |
| `429` | `tenant_capacity_exhausted` | A tenant request, byte, or work limit is exhausted. |
| `500` | `internal_error` | An internal invariant or required server capability failed. |
| `502` | `upstream_invalid` | Upstream failed before a response was committed. |
| `503` | `overloaded` | A global request, byte, or work limit is exhausted. |
| `503` | `queue_deadline_exceeded` | The request expired before upstream dispatch. |
| `503` | `draining` | The process is no longer accepting work. |
| `504` | `upstream_timeout` | A dispatched upstream timed out before commitment. |

`401` includes `WWW-Authenticate: Bearer`, and `405` includes `Allow: POST`.
SSEmaphore does not emit `Retry-After` until it can calculate an honest bounded
estimate. After an SSE response is committed, errors cancel the upstream and
close the stream without a synthetic `[DONE]`; only a private content-free
lifecycle reason records the incomplete stream. A second JSON error response
is impossible.

## Compatibility statement

OpenAI documents that optional fields and new streaming event types can be
added compatibly to its `v1` API. SSEmaphore therefore versions this smaller
allowlist independently and will not claim full OpenAI API compatibility. A
future subset change requires contract fixtures and an explicit changelog.

[chat-create]: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
