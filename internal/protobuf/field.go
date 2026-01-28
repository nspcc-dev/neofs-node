package protobuf

// One-byte tags for varlen fields.
const (
	_ = iota<<3 | 2
	/* TagBytes1 */ _
	TagBytes2
	TagBytes3
)

// FieldBounds represents bounds of some field in some continuous protobuf
// message.
type FieldBounds struct {
	From      int // tag index
	ValueFrom int // first value byte index
	To        int // last byte index
}

// IsMissing returns field absence flag.
func (x FieldBounds) IsMissing() bool {
	return x.From == x.To
}
