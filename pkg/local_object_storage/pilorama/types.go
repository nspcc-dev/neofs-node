package pilorama

import (
	"errors"
	"math"
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
type LogMove struct {
	Move
	HasOld bool
	Old    nodeInfo
}

const (
	// RootID represents the ID of a root node.
	RootID = 0
	// TrashID is a parent for all removed nodes.
	TrashID = math.MaxUint64
)

// ErrTreeNotFound is returned when the requested tree is not found.
var ErrTreeNotFound = errors.New("tree not found")
