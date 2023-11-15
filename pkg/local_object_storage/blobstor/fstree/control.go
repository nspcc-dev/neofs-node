package fstree

import (
	"fmt"

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
		return fmt.Errorf("mkdir all for %q: %w", t.RootPath, err)
	}
	if !t.readOnly {
		f := newSpecificWriteData(t.RootPath, t.Permissions, t.noSync)
		if f != nil {
			t.writeData = f
		}
	}
	return nil
}

// Close implements common.Storage.
func (*FSTree) Close() error { return nil }
