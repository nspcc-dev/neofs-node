package storagegroup

import (
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/tzhash/tz"
)

// CollectMembers creates new storage group structure and fills it
// with information about members collected via HeadReceiver.
//
// Resulting storage group consists of physically stored objects only.
func CollectMembers(r objutil.HeadReceiver, cid *cid.ID, members []oidSDK.ID, calcHomoHash bool) (*storagegroup.StorageGroup, error) {
	var (
		sumPhySize uint64
		phyMembers []oidSDK.ID
		phyHashes  [][]byte
		addr       = addressSDK.NewAddress()
		sg         = storagegroup.New()
	)

	addr.SetContainerID(cid)

	for i := range members {
		addr.SetObjectID(&members[i])

		if err := objutil.IterateAllSplitLeaves(r, addr, func(leaf *object.Object) {
			phyMembers = append(phyMembers, *leaf.ID())
			sumPhySize += leaf.PayloadSize()

			if calcHomoHash {
				phyHashes = append(phyHashes, leaf.PayloadHomomorphicHash().Sum())
			}
		}); err != nil {
			return nil, err
		}
	}

	sg.SetMembers(phyMembers)
	sg.SetValidationDataSize(sumPhySize)

	if calcHomoHash {
		sumHash, err := tz.Concat(phyHashes)
		if err != nil {
			return nil, err
		}

		cs := checksum.New()
		tzHash := [64]byte{}
		copy(tzHash[:], sumHash)
		cs.SetTillichZemor(tzHash)

		sg.SetValidationDataHash(cs)
	}

	return sg, nil
}
