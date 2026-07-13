# check is the pre-commit gate: nothing is presented for review unless it
# passes. CI (when it exists) runs the same target.

# The linter is version-pinned and run through the Go toolchain, not PATH:
# gosec's analysis (notably G115) differs across releases, so an unpinned
# binary makes make check answer differently on different machines.
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

check: check-core check-sqlite

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

testcache-clean:
	go clean -testcache

.PHONY: check check-core check-sqlite
