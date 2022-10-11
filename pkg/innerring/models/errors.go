package models

import "errors"

// ErrNonAlphabet is returned when Inner Ring node from the context is not an
// Alphabet one.
var ErrNonAlphabet = errors.New("non-Alphabet node")

// ErrStorageEmissionDisabled is returned when GAS emission for storage nodes
// is disabled.
var ErrStorageEmissionDisabled = errors.New("storage emission is disabled")

// ErrEmptyNetmap is returned when there is no storage nodes in the network map.
var ErrEmptyNetmap = errors.New("empty netmap")

// ErrSubnetAccess is returned when some entity has no access to the particular
// subnet.
var ErrSubnetAccess = errors.New("no access to the subnet")
