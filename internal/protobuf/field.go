package protobuf

// One-byte tags for varlen fields.
const (
	_ = iota<<3 | 2
	TagBytes1
	TagBytes2
	TagBytes3
	TagBytes4
	/* TagBytes5 */ _
	TagBytes6
)

// One-byte tags for varint fields.
const (
	_ = iota << 3
	TagVarint1
	TagVarint2
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
