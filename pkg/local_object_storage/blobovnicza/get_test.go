package blobovnicza

import (
	"os"
	"path/filepath"
	"testing"

	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestBlobovnicza_Get(t *testing.T) {
	t.Run("re-configure object size limit", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "blob")

		var blz *Blobovnicza

		t.Cleanup(func() {
			blz.Close()
			os.RemoveAll(filename)
		})

		fnInit := func(szLimit uint64) {
			if blz != nil {
				require.NoError(t, blz.Close())
			}

			blz = New(
				WithPath(filename),
				WithObjectSizeLimit(szLimit),
			)

			require.NoError(t, blz.Open())
			require.NoError(t, blz.Init())
		}

		// initial distribution: [0:32K] (32K:64K]
		fnInit(2 * firstBucketBound)

		addr := oidtest.Address()
		obj := make([]byte, firstBucketBound+1)

		var prmPut PutPrm
		prmPut.SetAddress(addr)
		prmPut.SetMarshaledObject(obj)

		// place object to [32K:64K] bucket
		_, err := blz.Put(prmPut)
		require.NoError(t, err)

		var prmGet GetPrm
		prmGet.SetAddress(addr)

		checkObj := func() {
			res, err := blz.Get(prmGet)
			require.NoError(t, err)
			require.Equal(t, obj, res.Object())
		}

		// object should be available
		checkObj()

		// new distribution (extended): [0:32K] (32K:64K] (64K:128K]
		fnInit(3 * firstBucketBound)

		// object should be still available
		checkObj()

		// new distribution (shrunk): [0:32K]
		fnInit(firstBucketBound)

		// object should be still available
		checkObj()
	})
}
