# Security Policy

## Supported versions

gospan is pre-1.0. Security fixes land on the latest tagged release of each
module (core `vX.Y.Z` and `sqlite/vX.Y.Z`) and on `main`; older pre-1.0 tags
are not separately patched. Pin to the latest release to stay covered.

## Reporting a vulnerability

Please report security issues **privately** — not through public issues or
pull requests.

Use GitHub's private vulnerability reporting: the repository's **Security** tab
→ **Report a vulnerability**
(<https://github.com/akmadian/gospan/security/advisories/new>). That opens a
private advisory visible only to you and the maintainer.

Include what you'd want in any bug report: affected module (core or `sqlite`),
version or commit, Go version and OS, a minimal reproduction, and the impact
you have in mind. A draft fix is welcome but never required.

## What to expect

As a pre-1.0 project maintained by one person, response is best-effort — but
reports are triaged ahead of feature work. You'll get an acknowledgement, then
a fix or a decision with reasoning, and credit in the advisory and release
notes unless you'd rather stay anonymous.

## Scope

gospan is an in-process library with a deliberately small threat surface: it
takes no network input, opens no sockets, and the core has zero third-party
dependencies. The issue classes most worth reporting:

- a panic or crash path that escapes gospan's recovery boundaries into the
  traced program (the never-crash guarantee is the security boundary);
- unbounded resource growth — memory, file descriptors, disk — reachable from
  ordinary use;
- a trace file written outside its intended directory, or with unexpectedly
  broad permissions;
- a vulnerability in the `gospan/sqlite` module's driver dependency
  (`govulncheck` runs in CI on every change, but coverage gaps happen).
