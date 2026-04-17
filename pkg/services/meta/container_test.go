package meta

import (
	"math"
	"math/rand"
	"path"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neofs-node/internal/signed256"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestIntBucketOrder(t *testing.T) {
	p := path.Join(t.TempDir(), "db.db")
	db, err := storage.NewBoltDBStore(dbconfig.BoltDBOptions{FilePath: p, ReadOnly: false})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	minSigned := signed256.Min()
	maxSigned := signed256.Max()
	one := signed256.NewInt(1)
	minPlusOne := signed256.Int{}
	require.NoError(t, minPlusOne.Add(&minSigned, &one))
	minusOne := signed256.NewInt(-1)
	maxMinusOne := signed256.Int{}
	require.NoError(t, maxMinusOne.Add(&maxSigned, &minusOne))
	maxUint64 := signed256.NewUint64(math.MaxUint64)

	ns := []signed256.Int{
		minSigned,
		minPlusOne,
		signed256.NewInt(math.MinInt64),
		minusOne,
		signed256.NewInt(0),
		one,
		maxUint64,
		maxMinusOne,
		maxSigned,
	}
	rand.Shuffle(len(ns), func(i, j int) { ns[i], ns[j] = ns[j], ns[i] })

	changeSet := make(map[string][]byte)
	for _, n := range ns {
		changeSet[string(objectcore.IntBytes(&n))] = []byte{}
	}
	err = db.PutChangeSet(changeSet, nil)
	require.NoError(t, err)

	var collected []string
	db.Seek(storage.SeekRange{}, func(k, v []byte) bool {
		val, err := objectcore.RestoreIntAttribute(k)
		require.NoError(t, err)
		collected = append(collected, val)

		return true
	})

	require.Equal(t, []string{
		"-115792089237316195423570985008687907853269984665640564039457584007913129639935",
		"-115792089237316195423570985008687907853269984665640564039457584007913129639934",
		"-9223372036854775808",
		"-1",
		"0",
		"1",
		"18446744073709551615",
		"115792089237316195423570985008687907853269984665640564039457584007913129639934",
		"115792089237316195423570985008687907853269984665640564039457584007913129639935",
	}, collected)
}
