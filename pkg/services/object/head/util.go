package headsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
)

type onceHeaderWriter struct {
	once *sync.Once

	traverser *placement.Traverser

	resp *Response

	cancel context.CancelFunc
}

func (w *onceHeaderWriter) write(hdr *object.Object) {
	w.once.Do(func() {
		w.resp.hdr = hdr
		w.traverser.SubmitSuccess()
		w.cancel()
	})
}
