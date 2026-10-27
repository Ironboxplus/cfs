package aspect

import "github.com/Ironboxplus/cfs/store"

// Options configure the standard decorator stack.
type Options struct {
	Gate  GateOpts
	Retry RetryOpts
}

// Stack wraps base as redact(retry(gate(meter(base)))): the meter sits
// directly above the real store, so it counts what the drive actually
// receives, retries included, and measures the in-flight peak after the gate
// has applied its limits. The returned Meter is for reading the numbers.
func Stack(base store.Store, o Options) (store.Store, *Meter) {
	m := NewMeter(base)
	return Redact(Retry(Gate(m, o.Gate), o.Retry)), m
}
