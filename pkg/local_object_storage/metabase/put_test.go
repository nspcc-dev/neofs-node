package meta

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

type testPrm struct {
	withParent bool

	attrNum int
}

func (p testPrm) String() string {
	return fmt.Sprintf("[with_parent:%t, attributes:%d]",
		p.withParent,
		p.attrNum,
	)
}

func generateChecksum() *pkg.Checksum {
	cs := pkg.NewChecksum()

	sh := [sha256.Size]byte{}
	rand.Read(sh[:])

	cs.SetSHA256(sh)

	return cs
}

func generateObject(t require.TestingT, prm testPrm) *object.Object {
	obj := object.NewRaw()
	obj.SetID(testOID())
	obj.SetVersion(pkg.SDKVersion())
	obj.SetContainerID(testCID())
	obj.SetChildren(testOID())
	obj.SetPreviousID(testOID())
	obj.SetParentID(testOID())
	obj.SetPayloadChecksum(generateChecksum())
	obj.SetPayloadHomomorphicHash(generateChecksum())
	obj.SetType(objectSDK.TypeRegular)

	as := make([]*objectSDK.Attribute, 0, prm.attrNum)

	for i := 0; i < prm.attrNum; i++ {
		a := objectSDK.NewAttribute()

		k := make([]byte, 32)
		rand.Read(k)
		a.SetKey(string(k))

		v := make([]byte, 32)
		rand.Read(v)
		a.SetValue(string(v))

		as = append(as, a)
	}

	obj.SetAttributes(as...)

	wallet, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(wallet)

	obj.SetOwnerID(ownerID)

	if prm.withParent {
		prm.withParent = false
		obj.SetParent(generateObject(t, prm).SDK())
	}

	return obj.Object()
}

func BenchmarkDB_Put(b *testing.B) {
	db := newDB(b)

	defer releaseDB(db)

	for _, prm := range []testPrm{
		{
			withParent: false,
			attrNum:    0,
		},
		{
			withParent: true,
			attrNum:    0,
		},
		{
			withParent: false,
			attrNum:    100,
		},
		{
			withParent: true,
			attrNum:    100,
		},
		{
			withParent: false,
			attrNum:    1000,
		},
		{
			withParent: true,
			attrNum:    1000,
		},
	} {
		b.Run(prm.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				obj := generateObject(b, prm)
				b.StartTimer()

				err := db.Put(obj)

				b.StopTimer()
				require.NoError(b, err)
				b.StartTimer()
			}
		})
	}
}
