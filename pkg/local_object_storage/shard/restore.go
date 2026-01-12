package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// ErrInvalidMagic is returned when dump format is invalid.
var ErrInvalidMagic = logicerr.New("invalid magic")

// Restore restores objects from the dump prepared by Dump. If ignoreErrors
// is set any restore errors are ignored (corrupted objects are just skipped).
//
// Returns two numbers: successful and failed restored objects, as well as any
// error encountered.
func (s *Shard) Restore(r io.Reader, ignoreErrors bool) (int, int, error) {
	// Disallow changing mode during restore.
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.ReadOnly() {
		return 0, 0, ErrReadOnlyMode
	}

	var m [4]byte
	_, _ = io.ReadFull(r, m[:])
	if !bytes.Equal(m[:], dumpMagic) {
		return 0, 0, ErrInvalidMagic
	}

	var count, failCount int
	var data []byte
	var size [4]byte
	for {
		// If there are less than 4 bytes left, `Read` returns nil error instead of
		// io.ErrUnexpectedEOF, thus `ReadFull` is used.
		_, err := io.ReadFull(r, size[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, failCount, err
		}

		sz := binary.LittleEndian.Uint32(size[:])
		if uint32(cap(data)) < sz {
			data = make([]byte, sz)
		} else {
			data = data[:sz]
		}

		_, err = r.Read(data)
		if err != nil {
			return count, failCount, err
		}

		obj := new(object.Object)
		err = obj.Unmarshal(data)
		if err != nil {
			if ignoreErrors {
				failCount++
				continue
			}
			return count, failCount, err
		}

		err = s.Put(obj, nil)
		if err != nil && !IsErrObjectExpired(err) && !IsErrRemoved(err) {
			return count, failCount, err
		}

		count++
	}

	return count, failCount, nil
}
