package basic

// ACL represents a container's
// basic permission bits.
type ACL uint32

const (
	reservedBitNumber = 2 // first left bits are reserved

	stickyBitPos = reservedBitNumber // X-bit after reserved bits

	finalBitPos = stickyBitPos + 1 // F-bit after X-bit
)

const (
	opOffset = finalBitPos + 1 // offset of operation bits

	bitsPerOp = 4 // number of bits per operation

	opNumber = 7 // number of operation bit sections
)

const (
	bitUser uint8 = iota
	bitSystem
	bitOthers
	bitBearer
)

const leftACLBitPos = opOffset + bitsPerOp*opNumber - 1

// returns true if n-th left bit is set (starting at 0).
func isLeftBitSet(value ACL, n uint8) bool {
	bitMask := ACL(1 << (leftACLBitPos - n))
	return bitMask != 0 && value&bitMask == bitMask
}

// sets n-th left bit (starting at 0).
func setLeftBit(value *ACL, n uint8) {
	*value |= ACL(1 << (leftACLBitPos - n))
}

// resets n-th left bit (starting at 0).
func resetLeftBit(value *ACL, n uint8) {
	*value &= ^ACL(1 << (leftACLBitPos - n))
}

// Reserved returns true if n-th reserved option is enabled in basic ACL.
func (a ACL) Reserved(n uint8) bool {
	return n < reservedBitNumber && isLeftBitSet(a, n)
}

// SetReserved enables the n-th reserved option in basic ACL.
func (a *ACL) SetReserved(bit uint8) {
	if bit < reservedBitNumber {
		setLeftBit(a, bit)
	}
}

// ResetReserved disables the n-th reserved option in basic ACL.
func (a *ACL) ResetReserved(bit uint8) {
	if bit < reservedBitNumber {
		resetLeftBit(a, bit)
	}
}

// Final returns true if final option is enabled in basic ACL.
func (a ACL) Final() bool {
	return isLeftBitSet(a, finalBitPos)
}

// SetFinal enables final option in basic ACL.
func (a *ACL) SetFinal() {
	setLeftBit(a, finalBitPos)
}

// ResetFinal disables final option in basic ACL.
func (a *ACL) ResetFinal() {
	resetLeftBit(a, finalBitPos)
}

// Sticky returns true if sticky option is enabled in basic ACL.
func (a ACL) Sticky() bool {
	return isLeftBitSet(a, stickyBitPos)
}

// SetSticky enables the sticky option in basic ACL.
func (a *ACL) SetSticky() {
	setLeftBit(a, stickyBitPos)
}

// ResetSticky disables the sticky option in basic ACL.
func (a *ACL) ResetSticky() {
	resetLeftBit(a, stickyBitPos)
}

// UserAllowed returns true if user allowed the n-th operation in basic ACL.
func (a ACL) UserAllowed(n uint8) bool {
	return isLeftBitSet(a, opOffset+n*bitsPerOp+bitUser)
}

// AllowUser allows user the n-th operation in basic ACL.
func (a *ACL) AllowUser(n uint8) {
	setLeftBit(a, opOffset+n*bitsPerOp+bitUser)
}

// ForbidUser forbids user the n-th operation in basic ACL.
func (a *ACL) ForbidUser(n uint8) {
	resetLeftBit(a, opOffset+n*bitsPerOp+bitUser)
}

// SystemAllowed returns true if System group allowed the n-th operation is set in basic ACL.
func (a ACL) SystemAllowed(n uint8) bool {
	if n != OpDelete && n != OpGetRange {
		return true
	}

	return isLeftBitSet(a, opOffset+n*bitsPerOp+bitSystem)
}

// AllowSystem allows System group the n-th operation in basic ACL.
func (a *ACL) AllowSystem(op uint8) {
	setLeftBit(a, opOffset+op*bitsPerOp+bitSystem)
}

// ForbidSystem forbids System group the n-th operation in basic ACL.
func (a *ACL) ForbidSystem(op uint8) {
	resetLeftBit(a, opOffset+op*bitsPerOp+bitSystem)
}

// OthersAllowed returns true if Others group allowed the n-th operation is set in basic ACL.
func (a ACL) OthersAllowed(op uint8) bool {
	return isLeftBitSet(a, opOffset+op*bitsPerOp+bitOthers)
}

// AllowOthers allows Others group the n-th operation in basic ACL.
func (a *ACL) AllowOthers(op uint8) {
	setLeftBit(a, opOffset+op*bitsPerOp+bitOthers)
}

// ForbidOthers forbids Others group the n-th operation in basic ACL.
func (a *ACL) ForbidOthers(op uint8) {
	resetLeftBit(a, opOffset+op*bitsPerOp+bitOthers)
}

// BearerAllowed returns true if Bearer token usage is allowed for n-th operation in basic ACL.
func (a ACL) BearerAllowed(op uint8) bool {
	return isLeftBitSet(a, opOffset+op*bitsPerOp+bitBearer)
}

// AllowBearer allows Bearer token usage for n-th operation in basic ACL.
func (a *ACL) AllowBearer(op uint8) {
	setLeftBit(a, opOffset+op*bitsPerOp+bitBearer)
}

// ForbidBearer forbids Bearer token usage for n-th operation in basic ACL.
func (a *ACL) ForbidBearer(op uint8) {
	resetLeftBit(a, opOffset+op*bitsPerOp+bitBearer)
}
