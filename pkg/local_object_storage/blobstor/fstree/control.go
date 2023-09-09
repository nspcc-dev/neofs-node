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
	err := util.MkdirAllX(t.RootPath, t.Permissions)
	if err != nil {
		return err
	}
	return t.initOSSpecific()
}

// Close implements common.Storage.
func (t *FSTree) Close() error {
	return t.closeOSSpecific()
}
