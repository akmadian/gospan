package gospan

import "fmt"

// Status classifies how a span ended — it never says whether it ended.
// That is carried by the end timestamp alone (in trace files, end_ns IS
// NULL means running or incomplete), so a successful finish is StatusOK
// plus an end time, never a distinct status. The numeric values are
// frozen: they are written into trace files (spans.status) and never
// change meaning; future statuses append (SPEC §5).
type Status int

const (
	StatusOK       Status = 0 // ended without a recorded failure
	StatusError    Status = 1 // Fail recorded a non-cancellation error
	StatusCanceled Status = 2 // Fail recorded context.Canceled or context.DeadlineExceeded
)

// String returns "ok", "error", or "canceled"; unknown values format as
// "status(N)".
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	case StatusCanceled:
		return "canceled"
	default:
		return fmt.Sprintf("status(%d)", int(s))
	}
}
