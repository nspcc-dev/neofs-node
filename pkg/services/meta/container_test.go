package meta

import (
	"math"
	"math/big"
	"math/rand"
	"path"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
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

	ns := []*big.Int{
		maxUint256Neg,
		new(big.Int).Add(maxUint256Neg, big.NewInt(1)),
		big.NewInt(math.MinInt64),
		big.NewInt(-1),
		big.NewInt(0),
		big.NewInt(1),
		new(big.Int).SetUint64(math.MaxUint64),
		new(big.Int).Sub(maxUint256, big.NewInt(1)),
		maxUint256,
	}
	rand.Shuffle(len(ns), func(i, j int) { ns[i], ns[j] = ns[j], ns[i] })

	changeSet := make(map[string][]byte)
	for _, n := range ns {
		changeSet[string(objectcore.BigIntBytes(n))] = []byte{}
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
