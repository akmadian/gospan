module github.com/akmadian/gospan/sqlite

go 1.25.0

// The core requirement pins a published version so this module builds
// standalone for consumers; in this repo the committed go.work overrides
// it with the sibling directory, so dev and CI always test against the
// checked-out core (the OTel multi-module pattern — never a replace
// directive, which would break downstream consumers). Releases move it
// to a real tag per docs/RELEASING.md.
require github.com/akmadian/gospan v0.0.1

require modernc.org/sqlite v1.54.0

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.46.0 // indirect
	modernc.org/libc v1.74.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
