package pilorama

import "github.com/nspcc-dev/neo-go/pkg/io"

func (x *Meta) FromBytes(data []byte) error {
	if len(data) == 0 {
		x.Items = nil
		x.Time = 0
		return nil
	}

	r := io.NewBinReaderFromBuf(data)
	x.DecodeBinary(r)
	return r.Err
}

func (x Meta) Bytes() []byte {
	w := io.NewBufBinWriter()
	x.EncodeBinary(w.BinWriter)
	return w.Bytes()
}

func (x Meta) GetAttr(name string) []byte {
	for _, kv := range x.Items {
		if kv.Key == name {
			return kv.Value
		}
	}
	return nil
}

// DecodeBinary implements the io.Serializable interface.
func (x *Meta) DecodeBinary(r *io.BinReader) {
	ts := r.ReadVarUint()
	size := r.ReadVarUint()
	m := make([]KeyValue, size)
	for i := range m {
		m[i].Key = r.ReadString()
		m[i].Value = r.ReadVarBytes()
	}
	if r.Err != nil {
		return
	}

	x.Time = ts
	x.Items = m
}

// EncodeBinary implements the io.Serializable interface.
func (x Meta) EncodeBinary(w *io.BinWriter) {
	w.WriteVarUint(x.Time)
	w.WriteVarUint(uint64(len(x.Items)))
	for _, e := range x.Items {
		w.WriteString(e.Key)
		w.WriteVarBytes(e.Value)
	}
}

// Size returns size of x in bytes.
func (x Meta) Size() int {
	size := getVarIntSize(x.Time)
	size += getVarIntSize(uint64(len(x.Items)))
	for i := range x.Items {
		ln := len(x.Items[i].Key)
		size += getVarIntSize(uint64(ln)) + ln

		ln = len(x.Items[i].Value)
		size += getVarIntSize(uint64(ln)) + ln
	}
	return size
}

// getVarIntSize returns the size in number of bytes of a variable integer.
// (reference: GetVarSize(int value),  https://github.com/neo-project/neo/blob/master/neo/IO/Helper.cs)
func getVarIntSize(value uint64) int {
	var size int

	if value < 0xFD {
		size = 1 // unit8
	} else if value <= 0xFFFF {
		size = 3 // byte + uint16
	} else {
		size = 5 // byte + uint32
	}
	return size
}
