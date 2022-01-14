package pgds

import (
	"fmt"
)

// Options are Datastore options
type Options struct {
	Table        string
	MaxBatchSize uint16
}

// Option is the Datastore option type.
type Option func(*Options) error

// Apply applies the given options to this Option.
func (o *Options) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(o); err != nil {
			return fmt.Errorf("datastore option %d failed: %s", i, err)
		}
	}
	return nil
}

// OptionDefaults are the default datastore options. This option will be automatically
// prepended to any options you pass to the Hydra Head constructor.
var OptionDefaults = func(o *Options) error {
	o.Table = "blocks"
	o.MaxBatchSize = 0
	return nil
}

// Table configures the name of the postgres database table to store data in.
// Defaults to "blocks".
func Table(t string) Option {
	return func(o *Options) error {
		if t != "" {
			o.Table = t
		}
		return nil
	}
}

// MaxBatchSize sets the maximum number of updates that will be batched before committing.
// Default to 0, which means that the batch is only commited when Commit() is explicitly called.
func MaxBatchSize(size uint16) Option {
	return func(o *Options) error {
		o.MaxBatchSize = size
		return nil
	}
}
