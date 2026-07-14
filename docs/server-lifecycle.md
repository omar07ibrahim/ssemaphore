# Inbound server lifecycle

This document describes the implemented `internal/server` library checkpoint.
The package is not itself an executable: it never creates a listener, chooses
an address, loads credentials, installs signal handlers, or exposes a public
port. The separate `internal/app` boundary now performs those local-only tasks
and supplies already validated components and an already bound listener. The
server constructor validates again before taking coordinated ownership.

## Construction and ownership

`server.New` accepts one `httpapi.Handler`, its exact `admission.Scheduler`, and
one listener. Construction fails before taking ownership unless all of these
conditions hold:

- the handler and server reference the same scheduler instance;
- the listener is a concrete `*net.TCPListener` bound to a numeric loopback
  address, or a concrete Unix byte-stream `*net.UnixListener`;
- every timeout and resource value is positive and inside its hard bound;
- the handler's body, queue, and upstream timeout policy can be combined with
  the server policy without duration overflow.

Arbitrary listener wrappers are rejected because their reported `Addr` does
not prove what socket they accept from. Unix packet sockets are rejected
because HTTP requires byte-stream framing. TLS termination therefore belongs
at the trusted local front proxy in this checkpoint. After successful
construction, the server owns the listener and scheduler through terminal
shutdown; callers must not close or reuse them independently.

Rejected construction leaves both resources with the caller. Successful
construction wraps the listener with a close-aware semaphore: a slot is taken
before the underlying `Accept`, returned on an accept error or exactly once on
connection close, and listener shutdown unblocks both a capacity wait and an
underlying accept. A connection returned concurrently after close is discarded
and cannot reach `net/http`.

## Resource and deadline envelope

There are no implicit zero-value defaults. The caller must provide every
finite value.

| Boundary | Implemented rule |
| --- | --- |
| Accepted connections | `1..1024`, with one release for each accepted connection close |
| Header wire envelope | `8 KiB..64 KiB` |
| Combined header reservation | `connections * header_envelope <= 64 MiB`, checked without overflow |
| Ordinary server and handler timeout components | positive and at most one hour each |
| Force phase | positive and at most one minute |

Go 1.26 initially permits `Server.MaxHeaderBytes + 4096` bytes to its buffered
HTTP parser. The public configuration names the hard wire envelope, so the
library sets:

```text
net_http_max_header_bytes = header_read_envelope_bytes - 4096
```

Raw TCP tests prove that a request of exactly the configured envelope reaches
the handler and one additional byte receives `431` before the handler. The
envelope bounds bytes read for the request line and headers; it is not a claim
that Go's map and parser heap overhead equals that byte count.

The enclosing deadlines are derived rather than configured twice:

```text
ReadTimeout  = HeaderReadTimeout + Handler.BodyReadTimeout

WriteTimeout = Handler.BodyReadTimeout
             + Handler.DefaultQueueTimeout
             + Handler.UpstreamTimeout
             + ResponseWriteTimeout
```

The server also sets `ReadHeaderTimeout` and `IdleTimeout` directly. Real
`net/http` tests cover fragmented headers, incomplete bodies, a blocked
response flush, and an idle persistent connection.

## Protocol boundary

The `http.Server` has only HTTP/1 enabled. A second guard rejects any parsed
request whose protocol major version is not one; this is necessary because
Go deliberately passes the raw `PRI * HTTP/2.0` preface to an HTTP/1 handler so
applications can implement h2c themselves. SSEmaphore returns a content-free
`505` and closes that connection. An h2c upgrade remains an ordinary HTTP/1
request and cannot switch protocols.

The server also sets `DisableGeneralOptionsHandler`, so `OPTIONS *` reaches the
normal application policy instead of receiving an unauthenticated built-in
`200`. Its private `ErrorLog` discards `net/http` panic output, preventing
panic values, remote addresses, and stack traces from reaching the global
logger. Canary tests exercise that boundary.

`Serve` is one-shot. A second call returns `ErrServeAlreadyStarted`; an
unexpected accept-loop failure starts owned cleanup and returns only the static
`ErrServeFailed`.

## Graceful and forced shutdown

`Shutdown(ctx)` starts at most one internal cleanup. The caller context limits
only that call's wait; cleanup uses independent configured deadlines and
continues after the caller returns. A context canceled before the first call
does not start shutdown. Concurrent callers observe the same terminal result.

The graceful phase is:

1. `BeginDrain` stops admission and terminally cancels queued work.
2. Reusable upstream connections are closed without interrupting active work.
3. `http.Server.Shutdown` closes the listener and waits for active HTTP work.
4. The owned listener is explicitly closed for the shutdown-before-serve case.
5. The application handler tracker and scheduler accounting are both observed
   drained.
6. The server base context is canceled, the drained scheduler owner is closed
   with a fresh finalization context, and idle upstream connections are closed
   again.

If the grace phase fails or expires, the forced phase is:

1. Seal application entry so no new inner handler work can begin.
2. Retry `BeginDrain` only if its first command was never committed, preserving
   the original queued and in-flight counts.
3. Complete `ForceCancelInflight` so active permits are attributed to terminal
   `shutdown` before any downstream cancellation can race with them.
4. Cancel the server base context, close HTTP connections and the listener,
   and close reusable upstream connections.
5. Wait within the force deadline for tracked handlers and exact scheduler
   accounting to drain.
6. If scheduler drain is confirmed, close its owner with a fresh bounded
   finalization context.

`ShutdownResult` exposes only bounded counters and whether force was required.
Unexpected serve failures become `ErrServeFailed`; incomplete terminal cleanup
becomes `ErrShutdownIncomplete`. Underlying addresses, request data, and raw
listener or scheduler errors are never returned from those paths.

## Deliberate limitations

- `net/http` can reject malformed or oversized input before the application
  handler. Its built-in `400` and `431` responses do not use the JSON error
  envelope, request ID, or application security headers.
- The connection cap starts after kernel acceptance. It does not bound the
  kernel listen backlog, pre-edge bandwidth, front-proxy resources,
  process-wide file descriptors or goroutines, RSS, or SYN handling.
- A write deadline bounds blocked socket writes; it cannot preempt a handler
  stuck in CPU code. Forced shutdown reports incomplete if a handler ignores
  all cancellation and outlives the force phase.
- Timeout values are resource envelopes, not latency SLOs or rolling per-write
  deadlines. The 64 MiB header product is a configuration check, not a heap
  reservation or RSS guarantee.
- Upstream `Complete` and the optional idle-connection closer must obey their
  documented cancellation and concurrency contracts. A panic from the
  optional closer is contained, but an implementation that never returns can
  still violate terminal latency.
- The runnable command and signal integration live outside this package. There
  is still no deployment manifest, TLS manager, telemetry sink, or
  public-internet listener in this checkpoint.
- Discarding the private `net/http` panic log does not constrain logging by a
  front proxy, the operating system, the upstream, or future telemetry code.
