package common

import (
	"fmt"
	"io"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// PayloadRangeMode identifies the meaning of values stored in [PayloadRange].
type PayloadRangeMode uint8

const (
	// PayloadRangeModeNone means that no payload range is set.
	PayloadRangeModeNone PayloadRangeMode = iota
	// PayloadRangeModeOffsetLength interprets values as offset and length.
	PayloadRangeModeOffsetLength
	// PayloadRangeModeBounds interprets values as inclusive first and last positions.
	PayloadRangeModeBounds
	// PayloadRangeModeFrom interprets the first value as a position to read from.
	PayloadRangeModeFrom
	// PayloadRangeModeSuffix interprets the first value as a suffix length.
	PayloadRangeModeSuffix
)

// PayloadRange describes a payload range independently of its full length.
type PayloadRange struct {
	First  uint64
	Second uint64
	Mode   PayloadRangeMode
}

// NewPayloadRange returns an offset-length payload range.
func NewPayloadRange(off, ln uint64) PayloadRange {
	return PayloadRange{First: off, Second: ln, Mode: PayloadRangeModeOffsetLength}
}

// NewPayloadRangeBounds returns a payload range with inclusive bounds.
func NewPayloadRangeBounds(first, last uint64) PayloadRange {
	return PayloadRange{First: first, Second: last, Mode: PayloadRangeModeBounds}
}

// NewPayloadRangeFrom returns a payload range from first to the payload end.
func NewPayloadRangeFrom(first uint64) PayloadRange {
	return PayloadRange{First: first, Mode: PayloadRangeModeFrom}
}

// NewPayloadRangeSuffix returns a payload suffix range of the given length.
func NewPayloadRangeSuffix(ln uint64) PayloadRange {
	return PayloadRange{First: ln, Mode: PayloadRangeModeSuffix}
}

// IsSet reports whether the range is configured.
func (r PayloadRange) IsSet() bool {
	return r.Mode != PayloadRangeModeNone
}

// Resolve converts the range into offset-length form using the full payload
// length and validates the result.
func (r PayloadRange) Resolve(payloadLen uint64) (uint64, uint64, error) {
	var off, ln uint64
	switch r.Mode {
	case PayloadRangeModeNone:
		ln = payloadLen
	case PayloadRangeModeOffsetLength:
		off, ln = r.First, r.Second
		if ln == 0 {
			if off != 0 {
				return 0, 0, apistatus.ErrObjectOutOfRange
			}
			ln = payloadLen
		}
	case PayloadRangeModeBounds:
		if r.First > r.Second || r.First >= payloadLen {
			return 0, 0, apistatus.ErrObjectOutOfRange
		}
		last := min(r.Second, payloadLen-1)
		off, ln = r.First, last-r.First+1
	case PayloadRangeModeFrom:
		if r.First >= payloadLen {
			return 0, 0, apistatus.ErrObjectOutOfRange
		}
		off, ln = r.First, payloadLen-r.First
	case PayloadRangeModeSuffix:
		if r.First == 0 {
			return 0, 0, apistatus.ErrObjectOutOfRange
		}
		ln = min(r.First, payloadLen)
		off = payloadLen - ln
	default:
		return 0, 0, fmt.Errorf("unsupported payload range mode %d", r.Mode)
	}

	if ln != 0 && (off >= payloadLen || payloadLen-off < ln) {
		return 0, 0, apistatus.ErrObjectOutOfRange
	}
	return off, ln, nil
}

// Resolved converts the range into validated offset-length form.
func (r PayloadRange) Resolved(payloadLen uint64) (PayloadRange, error) {
	off, ln, err := r.Resolve(payloadLen)
	if err != nil {
		return PayloadRange{}, err
	}
	return NewPayloadRange(off, ln), nil
}

// Storage represents key-value object storage.
// It is used as a building block for a blobstor of a shard.
type Storage interface {
	Open(readOnly bool) error
	Init(ID) error
	Close() error

	Type() string
	Path() string
	ShardID() ID

	// GetBytes reads object by address into memory buffer in a canonical NeoFS
	// binary format. Returns [apistatus.ObjectNotFound] if object is missing.
	GetBytes(oid.Address) ([]byte, error)
	Get(oid.Address) (*object.Object, error)
	GetRangeStream(addr oid.Address, rng PayloadRange, readHeader bool) (*object.Object, uint64, io.ReadCloser, error)
	GetStream(oid.Address) (*object.Object, io.ReadCloser, error)
	Head(oid.Address) (*object.Object, error)
	ReadHeader(oid.Address, []byte) (int, error)
	ReadObject(oid.Address, []byte) (int, io.ReadCloser, error)
	ReadPayloadRange(oid.Address, uint64, uint64, []byte) (io.ReadCloser, error)
	Exists(oid.Address) (bool, error)
	Put(oid.Address, []byte) error
	PutBatch(map[oid.Address][]byte) error
	Delete(oid.Address) error
	Iterate(func(oid.Address, []byte) error, func(oid.Address, error) error) error
	IterateAddresses(func(oid.Address) error, bool) error
}

// Copy copies all objects from source Storage into the destination one. If any
// object cannot be stored, Copy immediately fails.
func Copy(dst, src Storage) error {
	return CopyBatched(dst, src, 0)
}

// CopyBatched copies all objects from source Storage into the destination one
// using batched API. If any batch cannot be stored, Copy immediately fails.
// batchSize less than or equal to 1 makes it the same as [Copy].
func CopyBatched(dst, src Storage, batchSize int) error {
	err := src.Open(true)
	if err != nil {
		return fmt.Errorf("open source sub-storage: %w", err)
	}

	defer func() { _ = src.Close() }()

	err = src.Init(ID{})
	if err != nil {
		return fmt.Errorf("initialize source sub-storage: %w", err)
	}

	err = dst.Open(false)
	if err != nil {
		return fmt.Errorf("open destination sub-storage: %w", err)
	}

	defer func() { _ = dst.Close() }()

	err = dst.Init(ID{})
	if err != nil {
		return fmt.Errorf("initialize destination sub-storage: %w", err)
	}

	var objBatch = make(map[oid.Address][]byte)

	err = src.Iterate(func(addr oid.Address, data []byte) error {
		exists, err := dst.Exists(addr)
		if err != nil {
			return fmt.Errorf("check presence of object %s in the destination sub-storage: %w", addr, err)
		} else if exists {
			return nil
		}

		if batchSize <= 1 {
			err = dst.Put(addr, data)
			if err != nil {
				return fmt.Errorf("put object %s into destination sub-storage: %w", addr, err)
			}
			return nil
		}
		objBatch[addr] = data
		if len(objBatch) == batchSize {
			err = dst.PutBatch(objBatch)
			if err != nil {
				return fmt.Errorf("put batch into destination sub-storage: %w", err)
			}
			clear(objBatch)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("iterate over source sub-storage: %w", err)
	}

	if len(objBatch) > 0 {
		err = dst.PutBatch(objBatch)
		if err != nil {
			return fmt.Errorf("put batch into destination sub-storage: %w", err)
		}
	}
	return nil
}
