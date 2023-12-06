package tombstone

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const defaultLRUCacheSize = 100

type cfg struct {
	log *zap.Logger

	cacheSize int

	tsSource Source
}

// Option is an option of ExpirationChecker's constructor.
type Option func(*cfg)

func defaultCfg() *cfg {
	return &cfg{
		log:       zap.NewNop(),
		cacheSize: defaultLRUCacheSize,
	}
}

// NewChecker creates, initializes and returns tombstone ExpirationChecker.
// The returned structure is ready to use.
//
// Panics if any of the provided options does not allow
// constructing a valid tombstone ExpirationChecker.
func NewChecker(oo ...Option) *ExpirationChecker {
	cfg := defaultCfg()

	for _, o := range oo {
		o(cfg)
	}

	panicOnNil := func(v any, name string) {
		if v == nil {
			panic(fmt.Sprintf("tombstone getter constructor: %s is nil", name))
		}
	}

	panicOnNil(cfg.tsSource, "Tombstone source")

	cache, err := lru.New[oid.Address, uint64](cfg.cacheSize)
	if err != nil {
		panic(fmt.Errorf("could not create LRU cache with %d size: %w", cfg.cacheSize, err))
	}

	return &ExpirationChecker{
		cache:    cache,
		log:      cfg.log,
		tsSource: cfg.tsSource,
	}
}

// WithLogger returns an option to specify
// logger.
func WithLogger(v *zap.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithCacheSize returns an option to specify
// LRU cache size.
func WithCacheSize(v int) Option {
	return func(c *cfg) {
		c.cacheSize = v
	}
}

// WithTombstoneSource returns an option
// to specify tombstone source.
func WithTombstoneSource(v Source) Option {
	return func(c *cfg) {
		c.tsSource = v
	}
}
