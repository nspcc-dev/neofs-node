package storagegroup

import (
	"errors"
	"fmt"

	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/tzhash/tz"
)

var (
	errMissingHomomorphicChecksum = errors.New("missing homomorphic checksum in member's child header")
	errInvalidHomomorphicChecksum = errors.New("invalid homomorphic checksum in member's child header")
	errMissingSplitMemberID       = errors.New("missing object ID in member's child header")
)

// CollectMembers creates new storage group structure and fills it
// with information about members collected via ObjectSource.
//
// Resulting storage group consists of physically stored objects only.
func CollectMembers(r objutil.ObjectSource, cnr cid.ID, members []oid.ID, calcHomoHash bool) (*storagegroup.StorageGroup, error) {
	var (
		sumPhySize uint64
		phyMembers []oid.ID
		phyHashes  [][]byte
		addr       oid.Address
		sg         storagegroup.StorageGroup
	)

	addr.SetContainer(cnr)

	for i := range members {
		addr.SetObject(members[i])

		var errMember error

		if err := objutil.IterateSplitLeaves(r, addr, func(leaf *object.Object) bool {
			id, ok := leaf.ID()
			if !ok {
				errMember = errMissingSplitMemberID
				return true
			}

			phyMembers = append(phyMembers, id)
			sumPhySize += leaf.PayloadSize()
			cs, csSet := leaf.PayloadHomomorphicHash()

			if calcHomoHash {
				if !csSet {
					errMember = fmt.Errorf("%w '%s'", errMissingHomomorphicChecksum, id)
					return true
				} else if cs.Type() != checksum.TZ {
					errMember = fmt.Errorf("%w: type '%s' instead of '%s'", errInvalidHomomorphicChecksum, cs.Type(), checksum.TZ)
					return true
				}
				phyHashes = append(phyHashes, cs.Value())
			}

			return false
		}); err != nil {
			return nil, err
		}
		if errMember != nil {
			return nil, fmt.Errorf("collect split-chain for member #%d: %w", i, errMember)
		}
	}

	sg.SetMembers(phyMembers)
	sg.SetValidationDataSize(sumPhySize)

	if calcHomoHash {
		sumHash, err := tz.Concat(phyHashes)
		if err != nil {
			return nil, fmt.Errorf("concatenate '%s' checksums of all members: %w", checksum.TZ, err)
		}

		var cs checksum.Checksum
		tzHash := [64]byte{}
		copy(tzHash[:], sumHash)
		cs.SetTillichZemor(tzHash)

		sg.SetValidationDataHash(cs)
	}

	return &sg, nil
}
