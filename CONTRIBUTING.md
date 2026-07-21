# Contributing to gospan

gospan is pre-1.0 and opening up to testers. Bug reports, real-world usage
notes, and pull requests are all welcome — especially reports from
instrumenting an actual program, since that is exactly the feedback the
pre-1.0 window exists to gather.

This document is the contributor's map: how the repo is laid out, the one
gate every change clears, and the few rules that are defects when broken.
It is deliberately short. The *why* behind the design lives in `docs/`.

## The two modules

One repository, two Go modules:

- **`gospan` (repo root)** — the core: `Tracer`, `Span`, `Sink`, the slog
  sink, `Stats`, `Summary`. **Stdlib only, zero third-party dependencies.**
- **`gospan/sqlite`** — the flagship SQLite sink, in its own module with its
  own `go.mod`, because it carries `modernc.org/sqlite`. Users who choose
  another destination never pull that driver, not even into their `go.sum`.

A committed **`go.work`** ties the two together so a checkout builds and
tests against the sibling core in-repo. `sqlite/go.mod` pins core at a
published version so the module still builds standalone for consumers; the
`go.work` overrides that pin locally. Keep `go.work` tracked — the stock Go
`.gitignore` excludes it, and that once broke CI.

**Never add a `replace` directive.** It is the tempting shortcut for
cross-module work and it breaks every downstream consumer. `go.work` is how
this repo resolves siblings; there is no case where `replace` is the answer.

## The gate: `make check`

Nothing is committed unless `make check` passes. CI runs the same target, so
running it locally first is the fastest path through review. Over **both
modules**, it runs:

- `gofmt` and `go vet`
- **golangci-lint, version-pinned** — pinned on purpose: gosec findings drift
  between releases, so an unpinned binary would answer differently on
  different machines. Don't float it.
- the test suite under `-race`
- the **allocation ceilings** (`TestAllocationCeilings`), run *without*
  `-race` — allocs/op is the enforced performance claim, and the race
  instrumentation perturbs it, so it needs a clean build
- `govulncheck`

`make bench BENCHCOUNT=5` produces benchstat-comparable samples if you are
looking at the timing side; ns/op is informational locally, while allocs/op
is the number the suite defends.

## Rules that are defects when broken

- **Core gains no third-party dependencies, ever.** This is the load-bearing
  promise of the whole design: importing `gospan` costs you nothing but the
  standard library. A heavier destination goes in its own module, the way
  `gospan/sqlite` does — never in core. The linter enforces this, not just
  this document.
- **Names are spelled in full.** Variables under three characters are banned;
  `varnamelen` enforces it. The exceptions are a short, well-known set
  (`i`/`j`/`k`, `id`, `err`, `ok`, `ctx`, `db`, `tx`, and short method
  receivers). Descriptive beats terse — `blockOnQueueFull`, not `blocking`.
  A three-character abbreviation like `cfg` clears the linter mechanically
  but is still a review defect: spell it out.
- **Reasoning lives in the code.** Non-obvious decisions get a comment where
  they are made, and every non-obvious default explains itself where it is
  declared. The existing source is the reference for the level of detail.

The deeper invariants — the never-blocking hot path, the never-crash
guarantee and its hostile-fixture tests, deterministic time via
`testing/synctest` — are documented in `docs/DESIGN.md`. If a change touches
those seams, read it first.

## Before proposing a feature

gospan's scope is defended in writing, and reading the relevant page before
opening an issue or PR saves everyone a round-trip:

- **`docs/SPEC.md`** — the build contract: API surface, semantics, schema,
  file-format guarantees. Sections **§3–§5 (schema, time, compatibility) are
  frozen cross-repo promises** — a separate viewer repository builds against
  them, and trace files outlive library versions. Changing them is a
  versioned event, not a casual edit.
- **`docs/DECISIONS.md`** — an append-only log of why things are the way they
  are. It wins every conflict with older prose. A new ambiguity, once
  resolved, earns a numbered decision (a "D-number") here rather than a quiet
  change of mind.
- **`docs/DEFERRED.md`** — features consciously **not** in v1, each with a
  stated **trigger**: the observable condition under which it gets built. A
  deferred entry is a live obligation, not a rejection — but it stays unbuilt
  until its trigger fires.

**Check `docs/DEFERRED.md` before proposing a feature.** Many of the obvious
asks — span links, sampling, an OTel adapter, live streaming — are already
there, deferred with a reason and a trigger. If your need *is* the trigger
for one of them, say so in the issue: that is precisely the signal that moves
it forward.

## Filing issues

The `.github/ISSUE_TEMPLATE` bug and feature templates prompt for what a
maintainer needs to act without a round-trip — for bugs, that includes which
module (core or `sqlite`), your Go version and OS, a minimal repro, and any
relevant `Stats()` output. Filling them in is the fastest way to a fix.

Thanks for helping gospan get to 1.0.
