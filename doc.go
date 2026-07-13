// Package gospan is embedded, span-based tracing for Go programs.
//
// Instrument work with named spans (Start/End, or one-line Track); span
// context rides context.Context; events flow through a bounded buffer to a
// single writer goroutine and on to a pluggable destination (Sink) — the
// SQLite trace file (the gospan/sqlite module) or your existing slog flow.
// After construction, nothing gospan does returns an error, panics into the
// caller, or blocks the hot path beyond a channel send. Every method on a
// nil *Tracer or nil *Span is a no-op: tracing disabled is a tracer never
// constructed.
//
// The conceptual model lives in DESIGN.md; the API, semantics, and
// file-format contract in SPEC.md.
package gospan
