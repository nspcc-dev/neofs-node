package meta

import (
	"math"
)

const (
	// Metadata contract identifiers.
	MetaDataContractID   = math.MinInt32
	MetaDataContractName = "MetaData"

	// event names
	putObjectEvent = "ObjectPut"

	// storage prefixes
	metaContainersPrefix     = 0x01
	containerPlacementPrefix = 0x02

	// limits
	maxREPsClauses = 255
)
