package shard

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var dumpMagic = []byte("NEOF")

// DumpPrm groups the parameters of Dump operation.
type DumpPrm struct {
	path         string
	stream       io.Writer
	ignoreErrors bool
}

// WithPath is an Dump option to set the destination path.
func (p *DumpPrm) WithPath(path string) {
	p.path = path
}

// WithStream is an Dump option to set the destination stream.
// It takes priority over `path` option.
func (p *DumpPrm) WithStream(r io.Writer) {
	p.stream = r
}

// WithIgnoreErrors is an Dump option to allow ignore all errors during iteration.
// This includes invalid peapods as well as corrupted objects.
func (p *DumpPrm) WithIgnoreErrors(ignore bool) {
	p.ignoreErrors = ignore
}

// DumpRes groups the result fields of Dump operation.
type DumpRes struct {
	count int
}

// Count return amount of object written.
func (r DumpRes) Count() int {
	return r.count
}

var ErrMustBeReadOnly = logicerr.New("shard must be in read-only mode")

// Dump dumps all objects from the shard to a file or stream.
//
// Returns any error encountered.
func (s *Shard) Dump(prm DumpPrm) (DumpRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if !s.info.Mode.ReadOnly() {
		return DumpRes{}, ErrMustBeReadOnly
	}

	w := prm.stream
	if w == nil {
		f, err := os.OpenFile(prm.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
		if err != nil {
			return DumpRes{}, err
		}
		defer f.Close()

		w = f
	}

	_, err := w.Write(dumpMagic)
	if err != nil {
		return DumpRes{}, err
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

		err := s.writeCache.Iterate(iterHandler, prm.ignoreErrors)
		if err != nil {
			return DumpRes{}, err
		}
	}

	var pi common.IteratePrm
	pi.IgnoreErrors = prm.ignoreErrors
	pi.Handler = func(elem common.IterationElement) error {
		data := elem.ObjectData

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

	if _, err := s.blobStor.Iterate(pi); err != nil {
		return DumpRes{}, err
	}

	return DumpRes{count: count}, nil
}
