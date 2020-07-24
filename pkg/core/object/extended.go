package object

// ExtendedHeaderType represents the enumeration
// of extended header types of the NeoFS object.
type ExtendedHeaderType uint32

// ExtendedHeader represents the extended
// header of NeoFS object.
type ExtendedHeader struct {
	typ ExtendedHeaderType

	val interface{}
}

// Type returns the extended header type.
func (h ExtendedHeader) Type() ExtendedHeaderType {
	return h.typ
}

// SetType sets the extended header type.
func (h *ExtendedHeader) SetType(v ExtendedHeaderType) {
	h.typ = v
}

// Value returns the extended header value.
//
// In the case of a reference type, the value is
// returned by reference, so value mutations affect
// header state. Therefore, callers must first copy
// the value before changing manipulations.
func (h ExtendedHeader) Value() interface{} {
	return h.val
}

// SetValue sets the extended header value.
//
// Caller must take into account that each type of
// header usually has a limited set of expected
// value types.
//
// In the case of a reference type, the value is set
// by reference, so source value mutations affect
// header state. Therefore, callers must first copy
// the source value before changing manipulations.
func (h *ExtendedHeader) SetValue(v interface{}) {
	h.val = v
}

// TypeFromUint32 converts builtin
// uint32 value to Epoch.
//
// Try to avoid direct cast for
// better portability.
func TypeFromUint32(v uint32) ExtendedHeaderType {
	return ExtendedHeaderType(v)
}

// TypeToUint32 converts Epoch value
// to builtin uint32.
//
// Try to avoid direct cast for
// better portability.
func TypeToUint32(v ExtendedHeaderType) uint32 {
	return uint32(v)
}

// TypesEQ reports whether t1 and t2 are the same ExtendedHeaderType.
//
// Function defines the relation of equality
// between two ExtendedHeaderType. Try to avoid comparison through
// "==" operator for better portability.
func TypesEQ(t1, t2 ExtendedHeaderType) bool {
	return TypeToUint32(t1) == TypeToUint32(t2)
}

// TypesLT reports whether t1 ExtendedHeaderType
// is less than t2.
//
// Function defines the "less than" relation
// between two ExtendedHeaderType. Try to avoid
// comparison through "<" operator for better portability.
func TypesLT(t1, t2 ExtendedHeaderType) bool {
	return TypeToUint32(t1) < TypeToUint32(t2)
}

// TypesGT reports whether t1 ExtendedHeaderType
// is greater than t2.
//
// Function defines the "greater than" relation
// between two ExtendedHeaderType. Try to avoid
// comparison through ">" operator for better portability.
func TypesGT(t1, t2 ExtendedHeaderType) bool {
	return TypeToUint32(t1) > TypeToUint32(t2)
}
