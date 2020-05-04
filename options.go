package pgds

import (
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Options are Datastore options
type Options struct {
	Table string
	Pool  *pgxpool.Pool
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

// Pool configures the connection pool the datastore should use.
// Defaults to no pool.
func Pool(p *pgxpool.Pool) Option {
	return func(o *Options) error {
		o.Pool = p
		return nil
	}
}
