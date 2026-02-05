package getsvc

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (exec *execCtx) executeLocal() {
	var err error

	localGET := exec.isLocal() && !exec.headOnly() && exec.ctxRange() == nil
	if localGET {
		var n int
		var stream io.ReadCloser
		n, stream, err = exec.svc.localObjects.OpenStream(exec.address(), exec.prm.getBufferFn)
		if err == nil {
			exec.prm.putBytesReadWithStreamFn(n, stream)
		}
	} else {
		exec.collectedHeader, exec.collectedReader, err = exec.svc.localStorage.get(exec)
	}

	var errSplitInfo *object.SplitInfoError

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("local get failed",
			zap.Error(err),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		if localGET {
			break
		}

		exec.writeCollectedObject()
	case errors.Is(err, apistatus.Error):
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			exec.status = statusNotFound
			exec.err = err

			return
		}

		exec.status = statusAPIResponse
		exec.err = err
	case errors.As(err, &errSplitInfo):
		exec.status = statusVIRTUAL
		mergeSplitInfo(exec.splitInfo(), errSplitInfo.SplitInfo())
		exec.err = object.NewSplitInfoError(exec.infoSplit)
	}
}

func (s *Service) copyLocalObjectHeader(dst internal.HeaderWriter, cnr cid.ID, id oid.ID, raw bool) error {
	hdr, err := s.localObjects.Head(oid.NewAddress(cnr, id), raw)
	if err != nil {
		return fmt.Errorf("get object header from local storage: %w", err)
	}

	if err := dst.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return nil
}
