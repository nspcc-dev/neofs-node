package event

// Type is a notification event enumeration type.
type Type string

// Event is an interface that is
// provided by Neo:Morph event structures.
type Event interface {
	MorphEvent()
}

// Equal compares two Type values and
// returns true if they are equal.
func (t Type) Equal(t2 Type) bool {
	return string(t) == string(t2)
}

// String returns casted to string Type.
func (t Type) String() string {
	return string(t)
}

// TypeFromBytes converts bytes slice to Type.
func TypeFromBytes(data []byte) Type {
	return Type(data)
}

// TypeFromString converts string to Type.
func TypeFromString(str string) Type {
	return Type(str)
}
