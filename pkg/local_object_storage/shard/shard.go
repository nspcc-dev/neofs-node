package shard

import (
	"sync"
)

// Shard represents single shard of NeoFS Local Storage Engine.
type Shard struct {
	*cfg

	mtx *sync.RWMutex

	weight WeightValues
}

// Option represents Shard's constructor option.
type Option func(*cfg)

type cfg struct {
	id *ID
}

func defaultCfg() *cfg {
	return new(cfg)
}

// New creates, initializes and returns new Shard instance.
func New(opts ...Option) *Shard {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Shard{
		cfg: c,
		mtx: new(sync.RWMutex),
	}
}

// WithID returns option to set shard identifier.
func WithID(id *ID) Option {
	return func(c *cfg) {
		c.id = id
	}
}
