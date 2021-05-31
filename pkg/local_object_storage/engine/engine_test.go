package engine

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testNewEngineWithShards(shards ...*shard.Shard) *StorageEngine {
	engine := &StorageEngine{
		cfg: &cfg{
			log: zap.L(),
		},
		mtx:    new(sync.RWMutex),
		shards: make(map[string]*shard.Shard, len(shards)),
	}

	for _, s := range shards {
		engine.shards[s.ID().String()] = s
	}

	return engine
}

func testNewShard(t *testing.T, id int) *shard.Shard {
	sid, err := generateShardID()
	require.NoError(t, err)

	s := shard.New(
		shard.WithID(sid),
		shard.WithLogger(zap.L()),
		shard.WithBlobStorOptions(
			blobstor.WithRootPath(fmt.Sprintf("%s.%d.blobstor", t.Name(), id)),
			blobstor.WithBlobovniczaShallowWidth(2),
			blobstor.WithBlobovniczaShallowDepth(2),
			blobstor.WithRootPerm(0700),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(fmt.Sprintf("%s.%d.metabase", t.Name(), id)),
			meta.WithPermissions(0700),
		))

	require.NoError(t, s.Open())
	require.NoError(t, s.Init())

	return s
}

func testOID() *objectSDK.ID {
	cs := [sha256.Size]byte{}
	_, _ = rand.Read(cs[:])

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func generateRawObjectWithCID(t *testing.T, cid *cid.ID) *object.RawObject {
	version := pkg.NewVersion()
	version.SetMajor(2)
	version.SetMinor(1)

	w, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)

	csum := new(pkg.Checksum)
	csum.SetSHA256(sha256.Sum256(w.Bytes()))

	csumTZ := new(pkg.Checksum)
	csumTZ.SetTillichZemor(tz.Sum(csum.Sum()))

	obj := object.NewRaw()
	obj.SetID(testOID())
	obj.SetOwnerID(ownerID)
	obj.SetContainerID(cid)
	obj.SetVersion(version)
	obj.SetPayloadChecksum(csum)
	obj.SetPayloadHomomorphicHash(csumTZ)
	obj.SetPayload([]byte{1, 2, 3, 4, 5})

	return obj
}

func addAttribute(obj *object.RawObject, key, val string) {
	attr := objectSDK.NewAttribute()
	attr.SetKey(key)
	attr.SetValue(val)

	attrs := obj.Attributes()
	attrs = append(attrs, attr)
	obj.SetAttributes(attrs...)
}
