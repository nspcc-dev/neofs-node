package object

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// TODO: may be needed for other RPC servers, move to shared codespace

const maxLENFieldSize = math.MaxInt32

var (
	errInvalidFieldTag  = errors.New("invalid field tag")
	errLENFieldOverflow = fmt.Errorf("LEN field overflows %d", maxLENFieldSize)
	errLENFieldInval    = fmt.Errorf("invald LEN field size")
	errMissingField     = errors.New("missing required field")
	errWrongFieldType   = errors.New("wrong field type")
)

func readVarint(r bytesReader, buf []byte) (int, error) {
	var b byte
	var ln int
	var err error
	for ln = range buf {
		b, err = r.ReadByte()
		if err != nil {
			if ln != 0 && errors.Is(err, io.EOF) {
				return ln, io.ErrUnexpectedEOF
			}
			return ln, err
		}
		buf[ln] = b
		if b < 0x80 {
			break
		}
	}
	return ln + 1, nil
}

func readLENFieldSize(r bytesReader) (int, error) {
	u, err := binary.ReadUvarint(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("read LEN prefix: %w", err)
	}
	if u > maxLENFieldSize {
		return 0, errLENFieldOverflow
	}
	return int(u), nil
}

// reads next field number and type from r. Returns [io.EOF] only if no bytes
// were read.
func readFieldTag(r bytesReader) (protowire.Number, protowire.Type, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, err
	}

	num, typ := protowire.DecodeTag(n)
	if !num.IsValid() {
		return 0, 0, errInvalidFieldTag
	}

	return num, typ, nil
}

// discards next field of specified type t from r.
func discardField(r bytesReader, t protowire.Type) error {
	var n int

	switch t {
	default:
		return fmt.Errorf("unsupported field type %v", t)
	case protowire.VarintType:
		for i := 0; i < binary.MaxVarintLen64; i++ {
			b, err := r.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return io.ErrUnexpectedEOF
				}
				return err
			}
			if b < 0x80 {
				return nil
			}
		}
		return nil
	case protowire.Fixed32Type:
		n = 4
	case protowire.Fixed64Type:
		n = 8
	case protowire.BytesType:
		var err error
		n, err = readLENFieldSize(r)
		if err != nil {
			return err
		}
	}

	_, err := io.CopyN(io.Discard, r, int64(n))
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// reads exactly len(b) bytes of LEN field from r into b.
func readLENFieldFull(r bytesReader, b []byte) error {
	ln, err := readLENFieldSize(r)
	if err != nil {
		return err
	}

	if ln != len(b) {
		return fmt.Errorf("length != %d", len(b))
	}

	_, err = io.ReadFull(r, b)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	return nil
}

// reads at most len(b) bytes of LEN field from r into b. Return number of bytes
// read only if err == nil.
//
// Note that b should not exceed maxLENFieldSize.
func readLENFieldAtMost(r bytesReader, b []byte) (int, error) {
	n, err := readLENFieldSize(r)
	if err != nil {
		return 0, err
	}

	if n > len(b) {
		return 0, fmt.Errorf("too big length > %d", len(b))
	}

	n, err = io.ReadFull(r, b[:n])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}

	return n, nil
}

type fieldPos struct{ tagOff, valOff, valLen int }

func seekField(b []byte, num protowire.Number, typ protowire.Type) (res fieldPos, err error) {
	for len(b[res.tagOff:]) > 0 {
		n, t, ln := protowire.ConsumeTag(b[res.tagOff:])
		if ln < 0 {
			return res, protowire.ParseError(ln)
		}
		if n == num && t != typ {
			return res, errWrongFieldType
		}

		res.valOff = res.tagOff + ln
		switch t {
		default:
			return res, fmt.Errorf("unsupported field #%d type %v", n, t)
		case protowire.VarintType:
			_, ln = protowire.ConsumeVarint(b[res.valOff:])
			if ln < 0 {
				return res, fmt.Errorf("invalid field #%d: %w", n, protowire.ParseError(ln))
			}
			if n == num {
				res.valLen = ln
				return res, nil
			}
		case protowire.BytesType:
			var u uint64
			u, ln = protowire.ConsumeVarint(b[res.valOff:])
			if ln < 0 {
				return res, fmt.Errorf("invalid field #%d: %w", n, protowire.ParseError(ln))
			} else if u > math.MaxInt {

			} else if len(b) < res.valOff+ln+int(u) {
				return res, fmt.Errorf("invalid field #%d: %w", n, io.ErrUnexpectedEOF)
			}
			res.valOff += ln
			if n == num {
				res.valLen = int(u)
				return res, nil
			}
			ln = int(u)
		case protowire.Fixed64Type:
			ln = 8
		case protowire.Fixed32Type:
			ln = 4
		}
		res.tagOff = res.valOff + ln
	}
	return res, errMissingField
}
