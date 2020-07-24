package object

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap/epoch"
)

// CID represents the container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.ID.
type CID = container.ID

// OwnerID represents the container
// owner identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.OwnerID.
type OwnerID = container.OwnerID

// Epoch represents the NeoFS epoch number.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/netmap/epoch.Epoch.
type Epoch = epoch.Epoch

// SystemHeader represents the
// system header of NeoFS Object.
type SystemHeader struct {
	version uint64 // object version

	payloadLen uint64 // length of the payload bytes

	id ID // object ID

	cid CID // container ID

	ownerID OwnerID // object owner ID

	creatEpoch Epoch // creation epoch number
}

// Version returns the object version number.
func (s *SystemHeader) Version() uint64 {
	return s.version
}

// SetVersion sets the object version number.
func (s *SystemHeader) SetVersion(v uint64) {
	s.version = v
}

// PayloadLength returns the length of the
// object payload bytes.
func (s *SystemHeader) PayloadLength() uint64 {
	return s.payloadLen
}

// SetPayloadLength sets the length of the object
// payload bytes.
func (s *SystemHeader) SetPayloadLength(v uint64) {
	s.payloadLen = v
}

// ID returns the object identifier.
func (s *SystemHeader) ID() ID {
	return s.id
}

// SetID sets the object identifier.
func (s *SystemHeader) SetID(v ID) {
	s.id = v
}

// CID returns the container identifier
// to which the object belongs.
func (s *SystemHeader) CID() CID {
	return s.cid
}

// SetCID sets the container identifier
// to which the object belongs.
func (s *SystemHeader) SetCID(v CID) {
	s.cid = v
}

// OwnerID returns the object owner identifier.
func (s *SystemHeader) OwnerID() OwnerID {
	return s.ownerID
}

// SetOwnerID sets the object owner identifier.
func (s *SystemHeader) SetOwnerID(v OwnerID) {
	s.ownerID = v
}

// CreationEpoch returns the epoch number
// in which the object was created.
func (s *SystemHeader) CreationEpoch() Epoch {
	return s.creatEpoch
}

// SetCreationEpoch sets the epoch number
// in which the object was created.
func (s *SystemHeader) SetCreationEpoch(v Epoch) {
	s.creatEpoch = v
}
