package shard_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

type ModeAwareStorage struct {
	common.Storage
	currentMode mode.Mode
}

func NewModeAwareStorage(s common.Storage) *ModeAwareStorage {
	return &ModeAwareStorage{
		Storage: s,
	}
}

func (m *ModeAwareStorage) SetMode(newMode mode.Mode) error {
	if m.currentMode == newMode {
		return nil
	}

	err := m.Close()
	if err == nil {
		if err = m.Open(newMode.ReadOnly()); err == nil {
			err = m.Init()
		}
	}

	m.currentMode = newMode
	return err
}

func TestBlobstorGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newBlobstor := func(t *testing.T) storagetest.Component {
		n++
		dir := filepath.Join(t.Name(), strconv.Itoa(n))

		fsTree := fstree.New(
			fstree.WithPath(filepath.Join(dir, "fstree")),
			fstree.WithDepth(0),
			fstree.WithDirNameLen(1))
		comp := &compression.Config{
			Enabled: true,
		}
		fsTree.SetCompressor(comp)

		return NewModeAwareStorage(fsTree)
	}

	storagetest.TestAll(t, newBlobstor)
}
