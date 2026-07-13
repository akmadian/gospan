# check is the pre-commit gate: nothing is presented for review unless it
# passes. CI (when it exists) runs the same target.
check:
	test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...
	golangci-lint run
	go test -race ./...

.PHONY: check
