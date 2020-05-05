package pgds

import (
	"fmt"
)

// Options are Datastore options
type Options struct {
	Table string
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
