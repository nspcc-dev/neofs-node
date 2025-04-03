package internal

// CheckPtrBool returns the value of v if it is not nil, otherwise returns def.
// If both v and def are nil, returns a new bool.
func CheckPtrBool(v, def *bool) *bool {
	if v != nil {
		return v
	}
	if def != nil {
		return def
	}
	return new(bool)
}
