package gospan

import "errors"

// MultiSink returns a Sink that delivers every call to each of sinks,
// sequentially and in order — the io.MultiWriter pattern, so composition
// is itself a Sink and the tracer never grows fan-out machinery (D22).
// Errors are joined: one failing sink never starves the rest. Nil entries
// are dropped. Because the same Batch visits every sink, the no-retention
// rule is what makes fan-out safe.
//
// Delivery is sequential in the writer goroutine by design — no per-sink
// goroutines. A slow member slows the whole fan-out, and that pressure
// surfaces as rising Stats QueueDepth rather than hiding in a buffer.
func MultiSink(sinks ...Sink) Sink {
	members := make([]Sink, 0, len(sinks))
	for _, sink := range sinks {
		if sink != nil {
			members = append(members, sink)
		}
	}
	return multiSink(members)
}

type multiSink []Sink

func (sinks multiSink) WriteBatch(batch Batch) error {
	var errs []error
	for _, sink := range sinks {
		if err := sink.WriteBatch(batch); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (sinks multiSink) Flush() error {
	var errs []error
	for _, sink := range sinks {
		if err := sink.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (sinks multiSink) Close() error {
	var errs []error
	for _, sink := range sinks {
		if err := sink.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
