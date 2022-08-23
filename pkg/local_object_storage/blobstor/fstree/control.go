package fstree

import (
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

// Open implements common.Storage.
func (t *FSTree) Open(ro bool) error {
	t.readOnly = ro
	return nil
}

// Init implements common.Storage.
func (t *FSTree) Init() error {
	return util.MkdirAllX(t.RootPath, t.Permissions)
}

// Close implements common.Storage.
func (*FSTree) Close() error { return nil }
