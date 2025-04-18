package shard

import (
	"encoding/binary"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var dumpMagic = []byte("NEOF")

var ErrMustBeReadOnly = logicerr.New("shard must be in read-only mode")

// DumpToStream dumps all objects from the shard to a given stream.
//
// Returns any error encountered and the number of objects written.
func (s *Shard) Dump(w io.Writer, ignoreErrors bool) (int, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if !s.info.Mode.ReadOnly() {
		return 0, ErrMustBeReadOnly
	}

	_, err := w.Write(dumpMagic)
	if err != nil {
		return 0, err
	}

	var count int

	if s.hasWriteCache() {
		var iterHandler = func(_ oid.Address, data []byte) error {
			var size [4]byte
			binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
			if _, err := w.Write(size[:]); err != nil {
				return err
			}

			if _, err := w.Write(data); err != nil {
				return err
			}

			count++
			return nil
		}

		err := s.writeCache.Iterate(iterHandler, ignoreErrors)
		if err != nil {
			return count, err
		}
	}

	var objHandler = func(addr oid.Address, data []byte) error {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], uint32(len(data)))
		if _, err := w.Write(size[:]); err != nil {
			return err
		}

		if _, err := w.Write(data); err != nil {
			return err
		}

		count++
		return nil
	}

	var errorHandler func(oid.Address, error) error
	if ignoreErrors {
		errorHandler = func(oid.Address, error) error { return nil }
	}

	err = s.blobStor.Iterate(objHandler, errorHandler)

	return count, err
}
