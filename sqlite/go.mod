module github.com/akmadian/gospan/sqlite

go 1.25.0

// github.com/akmadian/gospan is deliberately not required yet: no pushed
// commit exists to pin a pseudo-version against, and the committed
// go.work resolves the core module locally for dev and CI (the OTel
// multi-module pattern — never a replace directive, which would break
// downstream consumers). `go mod tidy` here pins the real version at
// first push.
require modernc.org/sqlite v1.53.0

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.44.0 // indirect
	modernc.org/libc v1.73.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
