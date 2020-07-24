package container

import (
	"crypto/sha256"

	"github.com/nspcc-dev/neofs-api-go/refs"
)

// ID represents the
// container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/refs.CID.
// FIXME: container id should be defined in core package.
type ID = refs.CID

// OwnerID represents the
// container owner identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/refs.OwnerID.
// FIXME: owner ID should be defined in core lib.
type OwnerID = refs.OwnerID

// OwnerIDSize is a size of OwnerID
// in a binary form.
const OwnerIDSize = refs.OwnerIDSize

// CalculateID calculates container identifier
// as SHA256 checksum of the binary form.
//
// If container is nil, ErrNilContainer is returned.
func CalculateID(cnr *Container) (*ID, error) {
	if cnr == nil {
		return nil, ErrNilContainer
	}

	data, err := cnr.MarshalBinary()
	if err != nil {
		return nil, err
	}

	res := new(ID)
	sh := sha256.Sum256(data)

	copy(res[:], sh[:])

	return res, nil
}
