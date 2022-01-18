package shard

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
)

var dumpMagic = []byte("NEOF")

// EvacuatePrm groups the parameters of Evacuate operation.
type EvacuatePrm struct {
	path   string
	stream io.Writer
}

// WithPath is an Evacuate option to set the destination path.
func (p *EvacuatePrm) WithPath(path string) *EvacuatePrm {
	p.path = path
	return p
}

// WithStream is an Evacuate option to set the destination stream.
// It takes priority over `path` option.
func (p *EvacuatePrm) WithStream(r io.Writer) *EvacuatePrm {
	p.stream = r
	return p
}

// EvacuateRes groups the result fields of Evacuate operation.
type EvacuateRes struct {
	count int
}

// Count return amount of object written.
func (r *EvacuateRes) Count() int {
	return r.count
}

var ErrMustBeReadOnly = errors.New("shard must be in read-only mode")

// Evacuate dumps all objects from the shard to a file or stream.
//
// Returns any error encountered.
func (s *Shard) Evacuate(prm *EvacuatePrm) (*EvacuateRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode != ModeReadOnly {
		return nil, ErrMustBeReadOnly
	}

	w := prm.stream
	if w == nil {
		f, err := os.OpenFile(prm.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		w = f
	}

	_, err := w.Write(dumpMagic)
	if err != nil {
		return nil, err
	}

	var count int

	if s.hasWriteCache() {
		// TODO evacuate objects from write cache
	}

	var pi blobstor.IteratePrm

	pi.SetIterationHandler(func(elem blobstor.IterationElement) error {
		data := elem.ObjectData()

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
	})

	if _, err := s.blobStor.Iterate(pi); err != nil {
		return nil, err
	}

	return &EvacuateRes{count: count}, nil
}
