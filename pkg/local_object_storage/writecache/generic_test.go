package writecache

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/stretchr/testify/require"
)

func TestGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newCache := func(t *testing.T) storagetest.Component {
		n++
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		require.NoError(t, os.MkdirAll(dir, os.ModePerm))
		return New(
			WithLogger(test.NewLogger(false)),
			WithFlushWorkersCount(2),
			WithPath(dir))
	}

	storagetest.TestAll(t, newCache)
}
