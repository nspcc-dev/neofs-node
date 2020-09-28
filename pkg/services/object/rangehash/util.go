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
	add([]byte)
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

func (h *singleHasher) add(p []byte) {
	h.hash = p
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

func (h *tzHasher) add(p []byte) {
	h.hashes = append(h.hashes, p)

	return
}

func (h *tzHasher) sum() ([]byte, error) {
	return tz.Concat(h.hashes)
}

func (h *commonHasher) add(p []byte) {
	h.h.Write(p)
}

func (h *commonHasher) sum() ([]byte, error) {
	return h.h.Sum(nil), nil
}
