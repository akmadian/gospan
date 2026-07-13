//go:build race

package gospan_test

// raceDetectorEnabled reports whether this test binary was built with
// -race, which changes allocation behavior enough to invalidate exact
// allocation ceilings.
const raceDetectorEnabled = true
