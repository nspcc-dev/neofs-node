package contracts

import "github.com/nspcc-dev/neo-go/pkg/core/native/nativeids"

const (
	// TODO
	MetaDataContractID   = nativeids.Notary - 1
	MetaDataContractName = "MetaData"

	// event names
	putObjectEvent = "ObjectPut"

	// storage prefixes
	metaContainersPrefix     = '1'
	containerPlacementPrefix = '2'

	// limits
	maxREPsClauses = 255
)
