package pilorama

import (
	"io/fs"
	"time"
)

type Option func(*cfg)

type cfg struct {
	path          string
	perm          fs.FileMode
	noSync        bool
	maxBatchDelay time.Duration
	maxBatchSize  int
}

func WithPath(path string) Option {
	return func(c *cfg) {
		c.path = path
	}
}

func WithPerm(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.perm = perm
	}
}

func WithNoSync(noSync bool) Option {
	return func(c *cfg) {
		c.noSync = noSync
	}
}

func WithMaxBatchDelay(d time.Duration) Option {
	return func(c *cfg) {
		c.maxBatchDelay = d
	}
}

func WithMaxBatchSize(size int) Option {
	return func(c *cfg) {
		c.maxBatchSize = size
	}
}
