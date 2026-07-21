---
name: Bug report
about: Report something gospan does wrong
title: ''
labels: bug
assignees: ''
---

**What happened**

A clear, concise description of the bug — what you expected, and what gospan
did instead.

**Which module**

- [ ] core (`github.com/akmadian/gospan`)
- [ ] sqlite (`github.com/akmadian/gospan/sqlite`)

**Versions**

- gospan version (module version or commit SHA):
- Go version (`go version`):
- OS / architecture:

**Minimal repro**

The smallest program that shows the problem. A self-contained `main` or a
failing test is ideal — the closer to runnable, the faster the fix.

```go
// ...
```

**Expected vs. actual**

- Expected:
- Actual:

**Relevant `Stats()` output**

gospan degrades rather than failing loudly, so the counters are often where a
problem is visible. Paste the tracer's `Stats()` near the time of the issue —
`Dropped`, `WriteErrors`, and `QueueDepth` especially:

```
// tracer.Stats()
```

**Anything else**

Logs (if you ran with `WithLogger`), the trace file schema version, or other
context that might help.
