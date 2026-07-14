# Running the local gateway

SSEmaphore's executable boundary is deliberately small: one strict policy
file, credentials resolved from named environment variables, and one numeric
loopback TCP listener. It does not terminate TLS or expose a public edge.

## Platform and build

The runnable command currently targets Linux. Its policy loader uses Linux
`O_NOFOLLOW` and `O_NONBLOCK` file-open guarantees to reject symlinks and
special files without blocking.

```sh
mkdir -p bin
go build -o bin/ssemaphore ./cmd/ssemaphore
```

## Prepare a policy

The committed example deliberately contains no credentials. Copy it to a
private local file, change the upstream endpoint and resource policy, and set
the exact required mode:

```sh
install -d -m 700 "$HOME/.config/ssemaphore"
install -m 600 configs/policy.example.json "$HOME/.config/ssemaphore/policy.json"
POLICY_PATH="$(realpath "$HOME/.config/ssemaphore/policy.json")"
```

The loader accepts only a nonempty, regular file that:

- has an absolute, clean path no longer than 4096 bytes;
- is owned by the effective process user and has exact mode `0600`;
- is at most 1 MiB;
- has no symlink path component; every ancestor must be a directory owned by
  root or the effective user and must not be group/other-writable, except for a
  root-owned sticky directory such as `/tmp`.

Policy JSON is versioned and has no defaults. Unknown fields, duplicate keys,
trailing values, invalid UTF-8, unpaired surrogates, fractional integers, and
out-of-range resource values are rejected. Every duration is an explicit
positive integer number of milliseconds.

Diagnostics deliberately use fixed categories and never echo a field, path,
endpoint, environment name, raw decoder error, or rejected value. When
`validate` rejects a setup, check file ownership/mode first, then the exact env
and bearer grammars below, and finally the cross-field resource envelopes in
the example policy.

The listener host must be a numeric loopback address such as `127.0.0.1` or
`::1`; DNS names, wildcards, mapped IPv4 addresses, zones, and port zero are
not accepted. Plaintext upstream HTTP is likewise accepted only for a numeric
loopback endpoint. Remote upstreams require HTTPS.

## Supply credentials

The policy contains environment-variable names, never bearer values. Names
must match `^[A-Z_][A-Z0-9_]{0,127}$`. Export one distinct token for every
configured tenant reference and a different token for the upstream:

```sh
export SSEMAPHORE_TENANT_1_TOKEN='replace-with-a-random-tenant-token'
export SSEMAPHORE_TENANT_2_TOKEN='replace-with-a-different-tenant-token'
export SSEMAPHORE_UPSTREAM_BEARER_TOKEN='replace-with-the-upstream-token'
```

Tokens are opaque 1--4096-byte values. They must contain one or more characters
from `A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, `~`, `+`, or `/`, optionally followed
by one or more `=` padding characters; whitespace, `:`, and `@` are rejected.
The process reads each configured variable exactly once and removes its own
copy from the environment before listener creation. Tenant tokens are retained
only as hashes; the upstream transport necessarily retains its Authorization
value. Clearing Go string fields and unsetting the child process environment
are best-effort exposure reductions, not claims of memory erasure or removal
from the parent shell.

## Validate without binding

```sh
./bin/ssemaphore validate --config "$POLICY_PATH"
```

Validation resolves the credentials, constructs the parser, scheduler, handler,
and upstream transport, then closes the scheduler and idle transport resources
and drops the other references. It performs no DNS lookup, outbound dial, or
listener bind. Success prints `gateway policy is valid`.

## Serve and stop

```sh
./bin/ssemaphore serve --config "$POLICY_PATH"
```

The command is silent after successful startup and never prints the policy
path, addresses, endpoints, environment names, credentials, headers, or
bodies. Send `SIGINT` or `SIGTERM` once to start the configured
graceful-to-forced shutdown. Signal handling is removed before cleanup, so a
second termination signal regains the operating system's default behavior.

After replacing the example upstream and starting `serve`, a client shell with
its tenant variable exported can exercise the documented subset with:

```sh
curl --fail-with-body --http1.1 --silent --show-error \
  -H "Authorization: Bearer $SSEMAPHORE_TENANT_1_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"model":"portfolio-model","messages":[{"role":"user","content":"hello"}],"max_completion_tokens":64}' \
  http://127.0.0.1:18080/v1/chat/completions
```

The exact accepted and rejected fields are in [the API contract](api.md).

Exit codes are stable:

- `0`: help, successful validation, or completed handled shutdown;
- `1`: listener, serving, shutdown, or internal cleanup failure;
- `2`: invalid invocation, unreadable/invalid policy, or rejected credentials.

The listener remains loopback-only. If traffic must arrive from another host,
a separately trusted TLS-terminating edge is required; that deployment and its
security policy are outside this repository's claims.
