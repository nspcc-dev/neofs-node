package storagegroup

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/tzhash/tz"
)

// CollectMembers creates new storage group structure and fills it
// with information about members collected via HeadReceiver.
//
// Resulting storage group consists of physically stored objects only.
func CollectMembers(r objutil.HeadReceiver, cid *cid.ID, members []*objectSDK.ID) (*storagegroup.StorageGroup, error) {
	var (
		sumPhySize uint64
		phyMembers []*objectSDK.ID
		phyHashes  [][]byte
		addr       = objectSDK.NewAddress()
		sg         = storagegroup.New()
	)

	addr.SetContainerID(cid)

	for i := range members {
		addr.SetObjectID(members[i])

		if err := objutil.IterateAllSplitLeaves(r, addr, func(leaf *object.Object) {
			phyMembers = append(phyMembers, leaf.ID())
			sumPhySize += leaf.PayloadSize()
			phyHashes = append(phyHashes, leaf.PayloadHomomorphicHash().Sum())
		}); err != nil {
			return nil, err
		}
	}

	sumHash, err := tz.Concat(phyHashes)
	if err != nil {
		return nil, err
	}

	cs := pkg.NewChecksum()
	tzHash := [client.TZSize]byte{}
	copy(tzHash[:], sumHash)
	cs.SetTillichZemor(tzHash)

	sg.SetMembers(phyMembers)
	sg.SetValidationDataSize(sumPhySize)
	sg.SetValidationDataHash(cs)

	return sg, nil
}
