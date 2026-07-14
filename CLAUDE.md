# gospan — Agent Instructions

Embedded span tracing for Go. Two modules: core (root, **zero third-party
dependencies — stdlib only, ever**) and `sqlite/` (the SQLite sink, owns
`modernc.org/sqlite`). A separate viewer repository consumes the file
format; it does not exist yet.

## Authority

- `docs/SPEC.md` — the build contract. §3–§5 (schema, time, compatibility)
  are frozen cross-repo promises; changing them is a versioned event, not
  an edit.
- `docs/DECISIONS.md` — append-only decision log; wins every conflict with
  older prose. New ambiguity resolutions get a D-number.
- `docs/DEFERRED.md` — consciously-not-in-v1 list. Don't build an entry
  without its stated trigger.
- `docs/RELEASING.md` — the two-tag release procedure (core `vX.Y.Z`,
  then `sqlite/vX.Y.Z`). Proxy-fetched versions are permanent: never
  retag.

## The gate

`make check` must pass before any commit is presented. It runs gofmt,
vet, the **version-pinned** golangci-lint (pinned because gosec findings
vary across releases — never float it), race tests, the allocation
ceilings, and govulncheck, over both modules.

- **allocs/op is the enforced performance claim** (`TestAllocationCeilings`,
  run without -race). ns/op is informational locally; CI defends it with a
  same-runner benchstat A/B on PRs and a trend chart on main.
- `make bench BENCHCOUNT=5` produces benchstat-comparable samples.

## Rules that are defects when broken

- Core gains no dependencies. Heavier destinations go in their own module.
- Never a `replace` directive. The committed `go.work` resolves siblings
  in-repo; `sqlite/go.mod` pins core at a published version so it builds
  standalone. Keep `go.work` tracked (the stock Go .gitignore excludes it —
  that once broke CI).
- Names are spelled in full — variables under 3 characters are banned
  (varnamelen enforces; `i/j/k`, `id`, `err`, `ok`, `ctx`, `db`, `tx` and
  short method receivers are the only exceptions). Descriptive beats
  terse: `blockOnQueueFull`, not `blocking`.
- Reasoning lives in the code: comments at decision points inside method
  bodies, and every non-obvious default explains itself where it's
  declared.
- The hot path (`Start`/`End`/`Fail`/`SetAttrs`) never blocks beyond one
  channel send and never gains an allocation without a decision. Sinks are
  called only from the writer goroutine.
- The never-crash guarantee is tested, not asserted: every seam where user
  code runs (sinks, loggers, error types) has a hostile-fixture test that
  panics into it. A new seam gets one before it ships.
- Tests are deterministic: `testing/synctest` for time and scheduling,
  gated fake sinks for ordering — never sleep-and-hope.

## Docs discipline

The README is a user-facing introduction to the package — never project
machinery (gates, CI strategy, release procedure; that lives here and in
`docs/`). SPEC §1–§2 migrate into godoc as implementation reality; §3–§5
stay in SPEC as the compatibility surface.
