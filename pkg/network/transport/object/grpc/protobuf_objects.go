package object

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	objectv2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	refsv2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	objIDValLen   = sha256.Size
	objIDLenLimit = 1 << 7

	objHdrLenDefault = 4 << 10
	objHdrLenLimit   = 4 << 20
)

type decodedObject struct {
	id []byte

	hdr    object.Object
	hdrFld []byte

	pldDataOff int
	pldFld     []byte
}

func (x decodedObject) releaseBuffers() {
	if x.hdrFld != nil {
		objectHeaderFieldBuffers.put(x.hdrFld)
	}
	if x.pldFld != nil {
		objectPayloadFieldBuffers.put(x.pldFld)
	}
}

func decodeObject(r bytesReader) (res decodedObject, err error) {
	defer func() {
		if err != nil {
			res.releaseBuffers()
		}
	}()

	fullLen, err := readLENFieldSize(r)
	if err != nil {
		return res, err
	} else if fullLen == 0 {
		return res, fmt.Errorf("read message length: %w", io.ErrUnexpectedEOF)
	}
	// TODO: check fullLen <= (max header + max payload)?

	r = limitBytesReader(r, fullLen)

	const (
		fieldNumID        = 1
		fieldNumSignature = 2
		fieldNumHeader    = 3
		fieldNumPayload   = 4
	)

	var varintArr [2 * binary.MaxVarintLen64]byte
	var doneLen, tagLen, sizeLen int
	var num protowire.Number
	var typ protowire.Type
	var size uint64
	var n int
	var sigV2 *refsv2.Signature
	var hdrV2 *objectv2.Header

	growHdrBuf := func(s int) []byte {
		if res.hdrFld != nil {
			if cap(res.hdrFld) < len(res.hdrFld)+s {
				tmp := make([]byte, len(res.hdrFld), len(res.hdrFld)+s)
				copy(tmp, res.hdrFld)
				if res.id != nil {
					res.id = slice.Copy(res.id)
				}
				objectHeaderFieldBuffers.put(res.hdrFld)
				res.hdrFld = tmp
			}
			res.hdrFld = res.hdrFld[:len(res.hdrFld)+s]
		} else {
			res.hdrFld = objectHeaderFieldBuffers.get(tagLen + sizeLen + int(size))
		}
		return res.hdrFld[len(res.hdrFld)-s:]
	}

	for {
		tagLen, err = readVarint(r, varintArr[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				if doneLen > 0 {
					break
				}
				return res, io.ErrUnexpectedEOF
			}
			return res, err
		}
		doneLen += tagLen
		if doneLen > fullLen {
			return res, errLENFieldInval
		}
		num, typ, n = protowire.ConsumeTag(varintArr[:tagLen])
		if n < 0 {
			return res, fmt.Errorf("invalid field tag: %w", protowire.ParseError(n))
		}
		if typ != protowire.BytesType {
			return res, fmt.Errorf("invalid type of field #%d: %d!=%d (bytes)", num, typ, protowire.BytesType)
		}
		sizeLen, err = readVarint(r, varintArr[tagLen:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, io.ErrUnexpectedEOF
			}
			return res, err
		}
		doneLen += sizeLen
		if doneLen > fullLen {
			return res, errLENFieldInval
		}
		size, n = protowire.ConsumeVarint(varintArr[tagLen:][:sizeLen])
		if n < 0 {
			return res, fmt.Errorf("invalid length of field #%v (type=%v): %w", num, typ, protowire.ParseError(n))
		}
		if size > maxLENFieldSize {
			return res, fmt.Errorf("too big field #%v (type=%v): %d>%d", num, typ, size, maxLENFieldSize)
		}
		doneLen += int(size) // legit cast after previous check
		if doneLen > fullLen {
			return res, errLENFieldInval
		}
		switch num {
		default:
			// TODO: can be forward compatible?
			return res, fmt.Errorf("unsupported field #%v (type=%v)", num, typ)
		case fieldNumID:
			if res.id != nil {
				return res, fmt.Errorf("repeated field #%d (ID)", num)
			} else if tagLen+sizeLen+int(size) > objIDLenLimit {
				return res, fmt.Errorf("too big field #%v (ID) for this server: %d>%d", num, size, objIDLenLimit)
			}

			b := growHdrBuf(tagLen + sizeLen + int(size))
			vb := b[tagLen+sizeLen:]
			if _, err = io.ReadFull(r, vb); err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return res, fmt.Errorf("read field #%d (ID): %w", num, err)
			}
			r, err := parseObjectID(vb)
			if err != nil {
				return res, fmt.Errorf("invalid field #%v (ID): %w", num, err)
			}
			res.id = vb[r.valOff:][:r.valLen]
			copy(b, varintArr[:tagLen])
			copy(b[tagLen:], varintArr[tagLen:][:sizeLen])
		case fieldNumSignature:
			if sigV2 != nil {
				return res, fmt.Errorf("repeated field #%d (signature)", num)
			} else if tagLen+sizeLen+int(size) > sigLenLimitHard {
				return res, fmt.Errorf("too big field #%v (signature) for this server: %d>%d", num, size, sigLenLimitHard)
			}

			b := growHdrBuf(tagLen + sizeLen + int(size))
			vb := b[tagLen+sizeLen:]
			if _, err = io.ReadFull(r, vb); err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return res, fmt.Errorf("read field #%d (signature): %w", num, err)
			}
			sigV2, err = parseSignatureMessage(vb)
			if err != nil {
				return res, fmt.Errorf("invalid field #%v (signature): %w", num, err)
			}
			copy(b, varintArr[:tagLen])
			copy(b[tagLen:], varintArr[tagLen:][:sizeLen])
		case fieldNumHeader:
			if hdrV2 != nil {
				return res, fmt.Errorf("repeated field #%d (header)", num)
			} else if tagLen+sizeLen+int(size) > objHdrLenLimit {
				return res, fmt.Errorf("too big field #%v (header) for this server: %d>%d", num, size, objHdrLenLimit)
			}

			b := growHdrBuf(tagLen + sizeLen + int(size))
			vb := b[tagLen+sizeLen:]
			if _, err = io.ReadFull(r, vb); err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return res, fmt.Errorf("read field #%d (header): %w", num, err)
			}
			hdrV2, err = parseObjectHeader(vb)
			if err != nil {
				return res, fmt.Errorf("invalid field #%v (header): %w", num, err)
			}
			copy(b, varintArr[:tagLen])
			copy(b[tagLen:], varintArr[tagLen:][:sizeLen])
		case fieldNumPayload:
			if res.pldFld != nil {
				return res, fmt.Errorf("repeated field #%d (payload)", num)
			} else if size > maxLENFieldSize {
				return res, fmt.Errorf("invalid field #%v (payload): %w", num, errLENFieldOverflow)
			}

			res.pldFld = objectPayloadFieldBuffers.get(tagLen + sizeLen + int(size))
			if size > 0 {
				_, err = io.ReadFull(r, res.pldFld[tagLen+sizeLen:])
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = io.ErrUnexpectedEOF
					}
					return res, fmt.Errorf("read field #%v (payload): %w", num, err)
				}
			}
			copy(res.pldFld, varintArr[:tagLen])
			copy(res.pldFld[tagLen:], varintArr[tagLen:][:sizeLen])
			res.pldDataOff = tagLen + sizeLen
		}
	}

	var objV2 objectv2.Object
	if res.id != nil {
		var idV2 refsv2.ObjectID
		idV2.SetValue(res.id)
		objV2.SetObjectID(&idV2)
	}
	if sigV2 != nil {
		objV2.SetSignature(sigV2)
	}
	if hdrV2 != nil {
		objV2.SetHeader(hdrV2)
	}

	res.hdr = *object.NewFromV2(&objV2)
	return res, nil
}

type parsedObjectID struct {
	valOff, valLen int
}

func parseObjectID(b []byte) (parsedObjectID, error) {
	var res parsedObjectID
	const fieldNumVal = 1
	val, err := seekField(b, fieldNumVal, protowire.BytesType)
	if err != nil {
		return res, fmt.Errorf("invalid field #%d: %w", fieldNumVal, err)
	} else if val.valLen != objIDValLen {
		return res, fmt.Errorf("invalid field #%d (value): %d != %d", fieldNumVal, val.valLen, objIDValLen)
	}
	res.valOff = val.valOff
	res.valLen = val.valLen
	return res, nil
}

func parseObjectHeader(b []byte) (*objectv2.Header, error) {
	var res objectv2.Header
	// TODO: unmarshal makes undesired allocations of bytes-type fields
	err := res.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

type objectHeaderFieldBuffersT struct{ p sync.Pool }

var objectHeaderFieldBuffers = objectHeaderFieldBuffersT{
	p: sync.Pool{New: func() any { return make([]byte, 0, objHdrLenDefault) }},
}

func (x *objectHeaderFieldBuffersT) get(ln int) []byte {
	b := x.p.Get().([]byte)
	if cap(b) >= ln {
		return b[:ln]
	}
	x.p.Put(b)
	if ln > objHdrLenDefault {
		return make([]byte, ln)
	}
	return make([]byte, ln, objHdrLenDefault)
}

func (x *objectHeaderFieldBuffersT) put(b []byte) {
	x.p.Put(b[:0])
}

const objPayloadBuffersNum = 5

const (
	_                    = 1<<(11+3*iota) + 1 + binary.MaxVarintLen64 // + tag + len
	objPayloadLenLimitL0                                              // 16KB
	objPayloadLenLimitL1                                              // 128KB
	objPayloadLenLimitL2                                              // 1MB
	objPayloadLenLimitL3                                              // 8MB
	objPayloadLenLimitL4                                              // 64MB
)

type objectPayloadFieldBuffersT struct {
	ps [objPayloadBuffersNum]sync.Pool
}

var objectPayloadFieldBuffers = objectPayloadFieldBuffersT{
	ps: [objPayloadBuffersNum]sync.Pool{
		{New: func() any { return make([]byte, objPayloadLenLimitL0) }},
		{New: func() any { return make([]byte, objPayloadLenLimitL1) }},
		{New: func() any { return make([]byte, objPayloadLenLimitL2) }},
		{New: func() any { return make([]byte, objPayloadLenLimitL3) }},
		{New: func() any { return make([]byte, objPayloadLenLimitL4) }},
	},
}

func (x *objectPayloadFieldBuffersT) get(s int) []byte {
	switch {
	case s <= objPayloadLenLimitL0:
		return x.ps[0].Get().([]byte)[:s]
	case s <= objPayloadLenLimitL1:
		return x.ps[1].Get().([]byte)[:s]
	case s <= objPayloadLenLimitL2:
		return x.ps[2].Get().([]byte)[:s]
	case s <= objPayloadLenLimitL3:
		return x.ps[3].Get().([]byte)[:s]
	case s <= objPayloadLenLimitL4:
		return x.ps[4].Get().([]byte)[:s]
	default:
		return make([]byte, s)
	}
}

func (x *objectPayloadFieldBuffersT) put(b []byte) {
	switch {
	case cap(b) <= objPayloadLenLimitL0:
		x.ps[0].Put(b)
	case cap(b) <= objPayloadLenLimitL1:
		x.ps[1].Put(b)
	case cap(b) <= objPayloadLenLimitL2:
		x.ps[2].Put(b)
	case cap(b) <= objPayloadLenLimitL3:
		x.ps[3].Put(b)
	case cap(b) <= objPayloadLenLimitL4:
		x.ps[4].Put(b)
	default:
		x.ps[4].Put(b[:len(b):len(b)])
		x.put(b[len(b):])
	}
}
