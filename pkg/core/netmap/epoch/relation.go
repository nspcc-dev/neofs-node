package epoch

// EQ reports whether e and e2 are the same Epoch.
//
// Function defines the relation of equality
// between two Epoch. Try to avoid comparison through
// "==" operator for better portability.
func EQ(e1, e2 Epoch) bool {
	return ToUint64(e1) == ToUint64(e2)
}

// NE reports whether e1 and e2 are the different Epoch.
//
// Method defines the relation of inequality
// between two Epoch. Try to avoid comparison through
// "!=" operator for better portability.
func NE(e1, e2 Epoch) bool {
	return ToUint64(e1) != ToUint64(e2)
}

// LT reports whether e1 is less Epoch than e2.
//
// Method defines the "less than" relation
// between two Epoch. Try to avoid comparison through
// "<" operator for better portability.
func LT(e1, e2 Epoch) bool {
	return ToUint64(e1) < ToUint64(e2)
}

// GT reports whether e1 is greater Epoch than e2.
//
// Method defines the "greater than" relation
// between two Epoch. Try to avoid comparison through
// ">" operator for better portability.
func GT(e1, e2 Epoch) bool {
	return ToUint64(e1) > ToUint64(e2)
}

// LE reports whether e1 is less or equal Epoch than e2.
//
// Method defines the "less or equal" relation
// between two Epoch. Try to avoid comparison through
// "<=" operator for better portability.
func LE(e1, e2 Epoch) bool {
	return ToUint64(e1) <= ToUint64(e2)
}

// GE reports whether e1 is greater or equal Epoch than e2.
//
// Method defines the "greater or equal" relation
// between two Epoch. Try to avoid comparison through
// ">=" operator for better portability.
func GE(e1, e2 Epoch) bool {
	return ToUint64(e1) >= ToUint64(e2)
}
