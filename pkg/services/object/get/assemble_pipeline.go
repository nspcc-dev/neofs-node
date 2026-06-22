package getsvc

import (
	"context"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// prefetchWindow is the split-assembly look-ahead: one child streams to the
// client while up to prefetchWindow-1 others are fetched concurrently.
// value <=1 disables pipelining and falls back to the serial path.
var prefetchWindow = 2

// childSlot carries one child's stream and the producer's result.
type childSlot struct {
	ready chan struct{}
	rc    io.ReadCloser
	hdr   *object.Object
	se    statusError
}

// streamChildrenPipelined fetches children concurrently, up to prefetchWindow in flight,
// and writes their payloads to the client writer strictly in index order. at(i)
// yields the i-th child ID and its sub-range; ok=false means there are no more
// children.
func (exec *execCtx) streamChildrenPipelined(at func(i int) (oid.ID, *object.Range, bool), checkHdr bool) statusError {
	ring := make([]*childSlot, prefetchWindow)

	gCtx, cancel := context.WithCancel(exec.context())
	defer cancel()

	var wg sync.WaitGroup
	launch := func(i int) bool {
		id, rng, ok := at(i)
		if !ok {
			return false
		}

		s := &childSlot{ready: make(chan struct{})}
		ring[i%len(ring)] = s
		wg.Go(func() {
			defer close(s.ready)
			s.hdr, s.rc, s.se = exec.fetchChildStream(gCtx, id, rng)
		})
		return true
	}

	launched := 0
	for launched < prefetchWindow && launch(launched) {
		launched++
	}
	if launched == 0 {
		return statusError{status: statusOK}
	}
	if launched < len(ring) {
		ring = ring[:launched]
	}

	buf := bufferPool.Get()
	defer bufferPool.Put(buf)
	dst := exec.prm.objWriter

	result := statusError{status: statusOK}
	for i := 0; i < launched; i++ {
		if se := exec.streamChildSlot(ring[i%len(ring)], dst, *buf.(*[]byte), checkHdr); se.status != statusOK {
			result = se
			break
		}

		if launch(launched) {
			launched++
		}
	}

	cancel()
	closeChildSlots(ring)
	wg.Wait()
	return result
}

func (exec *execCtx) streamChildSlot(s *childSlot, dst ChunkWriter, buf []byte, checkHdr bool) statusError {
	<-s.ready

	if s.se.status != statusOK {
		return s.se
	}

	if checkHdr && s.hdr != nil && !exec.isChild(s.hdr) {
		exec.log.Debug("parent address in child object differs")
	}

	_, copyErr := copyPayloadStreamBuffer(dst, s.rc, buf)
	closeErr := s.rc.Close()
	s.rc = nil

	if copyErr == nil && closeErr != nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return statusError{status: statusUndefined, err: copyErr}
	}

	return statusError{status: statusOK}
}

func closeChildSlots(slots []*childSlot) {
	for _, s := range slots {
		if s == nil {
			continue
		}
		<-s.ready
		if s.rc != nil {
			_ = s.rc.Close()
			s.rc = nil
		}
	}
}
