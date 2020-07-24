package epoch

// Sum returns the result of
// summing up two Epoch.
//
// Function defines a binary
// operation of summing two Epoch.
// Try to avoid using operator
// "+" for better portability.
func Sum(a, b Epoch) Epoch {
	return FromUint64(ToUint64(a) + ToUint64(b))
}
