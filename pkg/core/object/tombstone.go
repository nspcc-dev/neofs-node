package object

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/util/proto"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/pkg/errors"
)

// FIXME: replace this code to neofs-api-go

const addrListField = 1

type TombstoneContent TombstoneContentV2

type TombstoneContentV2 struct {
	addrList []*refs.Address
}

func NewTombstoneContentFromV2(cV2 *TombstoneContentV2) *TombstoneContent {
	return (*TombstoneContent)(cV2)
}

func NewTombstoneContent() *TombstoneContent {
	return NewTombstoneContentFromV2(new(TombstoneContentV2))
}

func (c *TombstoneContent) MarshalBinary() ([]byte, error) {
	return c.ToV2().StableMarshal(nil)
}

func (c *TombstoneContent) SetAddressList(v ...*object.Address) {
	if c != nil {
		addrV2 := make([]*refs.Address, 0, len(v))

		for i := range v {
			addrV2 = append(addrV2, v[i].ToV2())
		}

		(*TombstoneContentV2)(c).SetAddressList(addrV2)
	}
}

func (c *TombstoneContent) GetAddressList() []*object.Address {
	if c != nil {
		addrV2 := (*TombstoneContentV2)(c).GetAddressList()
		addr := make([]*object.Address, 0, len(addrV2))

		for i := range addrV2 {
			addr = append(addr, object.NewAddressFromV2(addrV2[i]))
		}

		return addr
	}

	return nil
}

func (c *TombstoneContent) ToV2() *TombstoneContentV2 {
	return (*TombstoneContentV2)(c)
}

func (c *TombstoneContentV2) StableMarshal(buf []byte) ([]byte, error) {
	if c == nil {
		return make([]byte, 0), nil
	}

	if buf == nil {
		buf = make([]byte, c.StableSize())
	}

	offset := 0

	for i := range c.addrList {
		n, err := proto.NestedStructureMarshal(addrListField, buf[offset:], c.addrList[i])
		if err != nil {
			return nil, err
		}

		offset += n
	}

	return buf, nil
}

func (c *TombstoneContentV2) StableSize() (sz int) {
	if c == nil {
		return
	}

	for i := range c.addrList {
		sz += proto.NestedStructureSize(addrListField, c.addrList[i])
	}

	return
}

func (c *TombstoneContentV2) StableUnmarshal(data []byte) error {
	c.addrList = c.addrList[:0]

	for len(data) > 0 {
		expPrefix, _ := proto.NestedStructurePrefix(addrListField)

		prefix, ln := binary.Uvarint(data)
		if ln <= 0 {
			return io.ErrUnexpectedEOF
		} else if expPrefix != prefix {
			return errors.Errorf("wrong prefix %d", prefix)
		}

		data = data[ln:]

		sz, ln := binary.Uvarint(data)
		if ln <= 0 {
			return io.ErrUnexpectedEOF
		}

		data = data[ln:]

		addr := new(refs.Address)
		if err := addr.StableUnmarshal(data[:sz]); err != nil {
			fmt.Println(err)
			return err
		}

		c.addrList = append(c.addrList, addr)

		data = data[sz:]
	}

	return nil
}

func (c *TombstoneContentV2) SetAddressList(v []*refs.Address) {
	if c != nil {
		c.addrList = v
	}
}

func (c *TombstoneContentV2) GetAddressList() []*refs.Address {
	if c != nil {
		return c.addrList
	}

	return nil
}

func TombstoneContentFromBytes(data []byte) (*TombstoneContent, error) {
	if len(data) == 0 {
		return nil, nil
	}

	cV2 := new(TombstoneContentV2)
	if err := cV2.StableUnmarshal(data); err != nil {
		return nil, err
	}

	return NewTombstoneContentFromV2(cV2), nil
}
