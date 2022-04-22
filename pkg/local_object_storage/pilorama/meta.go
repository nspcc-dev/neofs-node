package pilorama

import "github.com/nspcc-dev/neo-go/pkg/io"

func (x *Meta) FromBytes(data []byte) error {
	if len(data) == 0 {
		x.Items = nil
		x.Time = 0
		return nil
	}

	r := io.NewBinReaderFromBuf(data)
	ts := r.ReadVarUint()
	size := r.ReadVarUint()
	m := make([]KeyValue, size)
	for i := range m {
		m[i].Key = r.ReadString()
		m[i].Value = r.ReadVarBytes()
	}
	if r.Err != nil {
		return r.Err
	}

	x.Time = ts
	x.Items = m
	return nil
}

func (x Meta) Bytes() []byte {
	w := io.NewBufBinWriter()
	w.WriteVarUint(x.Time)
	w.WriteVarUint(uint64(len(x.Items)))
	for _, e := range x.Items {
		w.WriteString(e.Key)
		w.WriteVarBytes(e.Value)
	}

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
