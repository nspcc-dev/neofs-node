package fstree

import (
	"io/fs"
	"time"

	"go.uber.org/zap"
)

type Option func(*FSTree)

func WithDepth(d uint64) Option {
	return func(f *FSTree) {
		f.Depth = d
		f.depthSet = true
	}
}

func WithPerm(p fs.FileMode) Option {
	return func(f *FSTree) {
		f.Permissions = p
	}
}

func WithPath(p string) Option {
	return func(f *FSTree) {
		f.RootPath = p
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(f *FSTree) {
		f.log = l
	}
}

func WithSubtype(st string) Option {
	return func(f *FSTree) {
		f.subtype = st
		f.subtypeSet = true
	}
}

func WithNoSync(noSync bool) Option {
	return func(f *FSTree) {
		f.noSync = noSync
	}
}

func WithCombinedCountLimit(limit int) Option {
	return func(f *FSTree) {
		f.combinedCountLimit = limit
	}
}

func WithCombinedSizeLimit(size int) Option {
	return func(f *FSTree) {
		f.combinedSizeLimit = size
	}
}

func WithCombinedSizeThreshold(size int) Option {
	return func(f *FSTree) {
		f.combinedSizeThreshold = size
	}
}

func WithCombinedWriteInterval(t time.Duration) Option {
	return func(f *FSTree) {
		f.combinedWriteInterval = t
	}
}
