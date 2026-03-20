package meta

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
)

type initMeta struct {
	*DB
}

func (i *initMeta) Init() error {
	return i.DB.Init(nil)
}

func TestGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newMetabase := func(t *testing.T) storagetest.Component {
		n++
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return &initMeta{New(
			WithEpochState(epochStateImpl{}),
			WithPath(dir))}
	}

	storagetest.TestAll(t, newMetabase)
}
