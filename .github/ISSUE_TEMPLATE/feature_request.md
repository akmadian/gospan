---
name: Feature request
about: Suggest a capability for gospan
title: ''
labels: enhancement
assignees: ''
---

**Please check `docs/DEFERRED.md` first**

Many capabilities — span links, sampling, an OTel adapter, live streaming,
span-scoped log capture, and more — are already **consciously deferred**,
each with a stated *trigger*: the observable condition under which it gets
built. If your idea is listed there, the most useful thing you can do is
describe how your situation *is* that trigger.

- [ ] I checked `docs/DEFERRED.md` and this isn't already a deferred entry
- [ ] It is a deferred entry, and I'm reporting that its trigger has fired
      (details below)

**The problem**

What are you trying to do, and where does gospan fall short today? Concrete
usage beats a hypothetical — the design leans on real workloads to decide
what earns its place.

**Proposed solution**

What you'd like gospan to do. If it touches the file format or public API,
note that `docs/SPEC.md` §3–§5 are frozen cross-repo promises, so schema and
compatibility changes are versioned events rather than edits.

**Alternatives considered**

Other ways you've solved or worked around this — including whether an
attribute, a `Track` span, or an out-of-tree sink already covers it.

**Anything else**

Any other context.
