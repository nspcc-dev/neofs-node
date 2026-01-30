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
	sizeIndex
	typeIndex
	firstPartIndex
	previousPartIndex
	deletedIndex
	lockedIndex
	lockedByIndex
)

const (
	// event names.
	putObjectEvent = "ObjectPut"

	// limits.
	maxREPsClauses = 255
)
