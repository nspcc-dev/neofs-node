package fstree

import (
	"io/fs"
)

type Option func(*FSTree)

func WithDepth(d uint64) Option {
	return func(f *FSTree) {
		f.Depth = d
	}
}

func WithDirNameLen(l int) Option {
	return func(f *FSTree) {
		f.DirNameLen = l
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

func WithNoSync(noSync bool) Option {
	return func(f *FSTree) {
		f.noSync = noSync
	}
}
