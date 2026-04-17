package meta

import (
	"math"
)

const (
	// Metadata contract identifiers.
	MetaDataContractID   = math.MinInt32
	MetaDataContractName = "MetaData"
)

const (
	// storage prefixes.
	metaContainersPrefix = iota
	containerPlacementPrefix

	// object prefixes.
	addrIndex
	lockedByIndex
)

const (
	// event names.
	objectPutEvent     = "ObjectPut"
	objectDeletedEvent = "ObjectDeleted"
	objectLockedEvent  = "ObjectLocked"
)
