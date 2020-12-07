package rangehashsvc

import (
	"context"
	"hash"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/tzhash/tz"
)

type onceHashWriter struct {
	once *sync.Once

	traverser *placement.Traverser

	resp *Response

	cancel context.CancelFunc
}

type hasher interface {
	WriteChunk([]byte) error
	sum() ([]byte, error)
}

type tzHasher struct {
	hashes [][]byte
}

type commonHasher struct {
	h hash.Hash
}

type singleHasher struct {
	hash []byte
}

func (h *singleHasher) WriteChunk(p []byte) error {
	h.hash = p

	return nil
}

func (h *singleHasher) sum() ([]byte, error) {
	return h.hash, nil
}

func (w *onceHashWriter) write(hs [][]byte) {
	w.once.Do(func() {
		w.resp.hashes = hs
		w.traverser.SubmitSuccess()
		w.cancel()
	})
}

func (h *tzHasher) WriteChunk(p []byte) error {
	h.hashes = append(h.hashes, p)

	return nil
}

func (h *tzHasher) sum() ([]byte, error) {
	return tz.Concat(h.hashes)
}

func (h *commonHasher) WriteChunk(p []byte) error {
	h.h.Write(p)

	return nil
}

func (h *commonHasher) sum() ([]byte, error) {
	return h.h.Sum(nil), nil
}
