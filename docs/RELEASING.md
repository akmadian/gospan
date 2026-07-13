# Releasing

Two modules, two tag series: core is `vX.Y.Z`, the sqlite sink is
`sqlite/vX.Y.Z`. A version fetched by the module proxy is permanent —
never retag or force-push a release; fix forward with a new version.

## Core

1. On the release commit: `make check`.
2. Tag and push:

   ```sh
   git tag v0.X.Y
   git push origin v0.X.Y
   ```

## sqlite (after core, when it needs the new core)

1. Pin the sqlite module to the just-released core:

   ```sh
   cd sqlite
   go get github.com/akmadian/gospan@v0.X.Y
   go mod tidy
   ```

2. Commit, `make check`.
3. Tag and push:

   ```sh
   git tag sqlite/v0.X.Y
   git push origin sqlite/v0.X.Y
   ```

## First release only

`sqlite/go.mod` deliberately omits the core requirement while nothing is
published (the workspace supplies the sibling). The `go get` in step 1
adds it — expected, one-time.
