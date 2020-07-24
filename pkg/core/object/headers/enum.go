package headers

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

// Header represents object extended header.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/object.ExtendedHeader.
type Header = object.ExtendedHeader

// Type represents extended header type.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/object.ExtendedHeaderType.
type Type = object.ExtendedHeaderType

const (
	// this is the only place where this cast is appropriate,
	// use object.TypeFromUint32 instead.
	lowerUndefined = Type(iota) // lower unsupported Type value

	// TypeLink is the type of object reference header.
	TypeLink

	// TypeUser is the of user key-value string header.
	TypeUser

	// TypeTransform is the type of transformation mark header.
	TypeTransform

	// TypeTombstone is the type of tombstone mark header.
	TypeTombstone

	// TypeSessionToken is the type of session token header.
	TypeSessionToken

	// TypeHomomorphicHash is the type of homomorphic hash header.
	TypeHomomorphicHash

	// TypePayloadChecksum is the type of payload checksum header.
	TypePayloadChecksum

	// TypeIntegrity is the type of integrity header.
	TypeIntegrity

	// TypeStorageGroup is the type of storage group header.
	TypeStorageGroup

	// TypePublicKey is the type of public key header.
	TypePublicKey

	upperUndefined // upper unsupported Type value
)

// SupportedType returns true if Type is
// the known type of extended header. Each
// supported type has named constant.
func SupportedType(t Type) bool {
	return object.TypesGT(t, lowerUndefined) &&
		object.TypesLT(t, upperUndefined)
}
