# check is the pre-commit gate: nothing is presented for review unless it
# passes. CI runs the same target.

# The linter is version-pinned and run through the Go toolchain, not PATH:
# gosec's analysis (notably G115) differs across releases, so an unpinned
# binary makes make check answer differently on different machines.
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

# govulncheck is deliberately NOT pinned, unlike the linter: findings track
# the live vulnerability database either way, so pinning the binary buys no
# reproducibility — it only risks lagging the scanner itself.
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@latest

# Benchmark knobs: BENCHTIME=100x is the CI smoke run (do they still run?),
# BENCHCOUNT=10 produces samples benchstat can compare with significance.
BENCHTIME ?= 1s
BENCHCOUNT ?= 1

check: check-core check-sqlite vulncheck

check-core:
	test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...
	$(GOLANGCI_LINT) run
	go test -race ./...
	go test -run TestAllocationCeilings ./...  # alloc gate needs a non-race build

check-sqlite:
	cd sqlite && go vet ./...
	cd sqlite && $(GOLANGCI_LINT) run
	cd sqlite && go test -race ./...

vulncheck:
	$(GOVULNCHECK) ./...
	cd sqlite && $(GOVULNCHECK) ./...

bench:
	go test -bench . -benchmem -benchtime $(BENCHTIME) -count $(BENCHCOUNT) -run '^$$' ./...
	cd sqlite && go test -bench . -benchmem -benchtime $(BENCHTIME) -count $(BENCHCOUNT) -run '^$$' ./...

testcache-clean:
	go clean -testcache

.PHONY: check check-core check-sqlite vulncheck bench testcache-clean
