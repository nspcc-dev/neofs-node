package pilorama

import (
	"math"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
)

// Timestamp is an alias for integer timestamp type.
// TODO: remove after the debugging.
type Timestamp = uint64

// Node is used to represent nodes.
// TODO: remove after the debugging.
type Node = uint64

// Meta represents arbitrary meta information.
// TODO: remove after the debugging or create a proper interface.
type Meta struct {
	Time  Timestamp
	Items []KeyValue
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value []byte
}

// Move represents a single move operation.
type Move struct {
	Parent Node
	Meta
	// Child represents the ID of a node being moved. If zero, new ID is generated.
	Child Node
}

// LogMove represents log record for a single move operation.
type LogMove = Move

const (
	// RootID represents the ID of a root node.
	RootID = 0
	// TrashID is a parent for all removed nodes.
	TrashID = math.MaxUint64
)

var (
	// ErrTreeNotFound is returned when the requested tree is not found.
	ErrTreeNotFound = logicerr.New("tree not found")
	// ErrNotPathAttribute is returned when the path is trying to be constructed with a non-internal
	// attribute. Currently the only attribute allowed is AttributeFilename.
	ErrNotPathAttribute = logicerr.New("attribute can't be used in path construction")
)

// isAttributeInternal returns true iff key can be used in `*ByPath` methods.
// For such attributes an additional index is maintained in the database.
func isAttributeInternal(key string) bool {
	return key == AttributeFilename
}
