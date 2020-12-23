package acl

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
)

// wrapper around basic ACL to provide easy access to basic ACL fields
type basicACLHelper uint32

const (
	reservedBitNumber = 2                 // first left bits are reserved
	stickyBitPos      = reservedBitNumber // X-bit after reserved bits
	finalBitPos       = stickyBitPos + 1  // F-bit after X-bit
)

const (
	opOffset  = finalBitPos + 1 // offset of operation bits
	bitsPerOp = 4               // number of bits per operation
	opNumber  = 7               // number of operation bit sections
)

const (
	bitUser uint8 = iota
	bitSystem
	bitOthers
	bitBearer
)

const leftACLBitPos = opOffset + bitsPerOp*opNumber - 1

var (
	order = map[eacl.Operation]uint8{
		eacl.OperationRangeHash: 0,
		eacl.OperationRange:     1,
		eacl.OperationSearch:    2,
		eacl.OperationDelete:    3,
		eacl.OperationPut:       4,
		eacl.OperationHead:      5,
		eacl.OperationGet:       6,
	}
)

// returns true if n-th left bit is set (starting at 0).
func isLeftBitSet(value basicACLHelper, n uint8) bool {
	bitMask := basicACLHelper(1 << (leftACLBitPos - n))
	return bitMask != 0 && value&bitMask == bitMask
}

// sets n-th left bit (starting at 0).
func setLeftBit(value *basicACLHelper, n uint8) {
	*value |= basicACLHelper(1 << (leftACLBitPos - n))
}

// resets n-th left bit (starting at 0).
func resetLeftBit(value *basicACLHelper, n uint8) {
	*value &= ^basicACLHelper(1 << (leftACLBitPos - n))
}

// Final returns true if final option is enabled in ACL.
func (a basicACLHelper) Final() bool {
	return isLeftBitSet(a, finalBitPos)
}

// SetFinal enables final option in ACL.
func (a *basicACLHelper) SetFinal() {
	setLeftBit(a, finalBitPos)
}

// ResetFinal disables final option in ACL.
func (a *basicACLHelper) ResetFinal() {
	resetLeftBit(a, finalBitPos)
}

// Sticky returns true if sticky option is enabled in ACL.
func (a basicACLHelper) Sticky() bool {
	return isLeftBitSet(a, stickyBitPos)
}

// SetSticky enables the sticky option in ACL.
func (a *basicACLHelper) SetSticky() {
	setLeftBit(a, stickyBitPos)
}

// ResetSticky disables the sticky option in ACL.
func (a *basicACLHelper) ResetSticky() {
	resetLeftBit(a, stickyBitPos)
}

// UserAllowed returns true if user allowed the n-th operation in ACL.
func (a basicACLHelper) UserAllowed(op eacl.Operation) bool {
	if n, ok := order[op]; ok {
		return isLeftBitSet(a, opOffset+n*bitsPerOp+bitUser)
	}
	return false
}

// AllowUser allows user the n-th operation in ACL.
func (a *basicACLHelper) AllowUser(op eacl.Operation) {
	if n, ok := order[op]; ok {
		setLeftBit(a, opOffset+n*bitsPerOp+bitUser)
	}
}

// ForbidUser forbids user the n-th operation in ACL.
func (a *basicACLHelper) ForbidUser(op eacl.Operation) {
	if n, ok := order[op]; ok {
		resetLeftBit(a, opOffset+n*bitsPerOp+bitUser)
	}
}

// SystemAllowed returns true if System group allowed the n-th operation is set in ACL.
func (a basicACLHelper) SystemAllowed(op eacl.Operation) bool {
	if op != eacl.OperationDelete && op != eacl.OperationRange {
		return true
	}

	if n, ok := order[op]; ok {
		return isLeftBitSet(a, opOffset+n*bitsPerOp+bitSystem)
	}

	return false
}

// InnerRingAllowed returns true if the operation is allowed by ACL for
// InnerRing nodes, as part of System group.
func (a basicACLHelper) InnerRingAllowed(op eacl.Operation) bool {
	switch op {
	case eacl.OperationSearch, eacl.OperationRangeHash, eacl.OperationHead, eacl.OperationGet:
		return true
	default:
		if n, ok := order[op]; ok {
			return isLeftBitSet(a, opOffset+n*bitsPerOp+bitSystem)
		}

		return false
	}
}

// AllowSystem allows System group the n-th operation in ACL.
func (a *basicACLHelper) AllowSystem(op eacl.Operation) {
	if n, ok := order[op]; ok {
		setLeftBit(a, opOffset+n*bitsPerOp+bitSystem)
	}
}

// ForbidSystem forbids System group the n-th operation in ACL.
func (a *basicACLHelper) ForbidSystem(op eacl.Operation) {
	if n, ok := order[op]; ok {
		resetLeftBit(a, opOffset+n*bitsPerOp+bitSystem)
	}
}

// OthersAllowed returns true if Others group allowed the n-th operation is set in ACL.
func (a basicACLHelper) OthersAllowed(op eacl.Operation) bool {
	if n, ok := order[op]; ok {
		return isLeftBitSet(a, opOffset+n*bitsPerOp+bitOthers)
	}
	return false
}

// AllowOthers allows Others group the n-th operation in ACL.
func (a *basicACLHelper) AllowOthers(op eacl.Operation) {
	if n, ok := order[op]; ok {
		setLeftBit(a, opOffset+n*bitsPerOp+bitOthers)
	}
}

// ForbidOthers forbids Others group the n-th operation in ACL.
func (a *basicACLHelper) ForbidOthers(op eacl.Operation) {
	if n, ok := order[op]; ok {
		resetLeftBit(a, opOffset+n*bitsPerOp+bitOthers)
	}
}

// BearerAllowed returns true if Bearer token usage is allowed for n-th operation in ACL.
func (a basicACLHelper) BearerAllowed(op eacl.Operation) bool {
	if n, ok := order[op]; ok {
		return isLeftBitSet(a, opOffset+n*bitsPerOp+bitBearer)
	}
	return false
}

// AllowBearer allows Bearer token usage for n-th operation in ACL.
func (a *basicACLHelper) AllowBearer(op eacl.Operation) {
	if n, ok := order[op]; ok {
		setLeftBit(a, opOffset+n*bitsPerOp+bitBearer)
	}
}

// ForbidBearer forbids Bearer token usage for n-th operation in ACL.
func (a *basicACLHelper) ForbidBearer(op eacl.Operation) {
	if n, ok := order[op]; ok {
		resetLeftBit(a, opOffset+n*bitsPerOp+bitBearer)
	}
}
