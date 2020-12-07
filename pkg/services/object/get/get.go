package getsvc

import (
	"context"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.uber.org/zap"
)

// Get serves a request to get an object by address, and returns Streamer instance.
func (s *Service) Get(ctx context.Context, prm Prm) error {
	return s.get(ctx, RangePrm{
		commonPrm: prm.commonPrm,
	}).err
}

// GetRange serves a request to get an object by address, and returns Streamer instance.
func (s *Service) GetRange(ctx context.Context, prm RangePrm) error {
	return s.get(ctx, prm).err
}

func (s *Service) get(ctx context.Context, prm RangePrm) statusError {
	exec := &execCtx{
		svc:       s,
		ctx:       ctx,
		prm:       prm,
		infoSplit: objectSDK.NewSplitInfo(),
	}

	exec.setLogger(s.log)

	exec.execute()

	return exec.statusError
}

func (exec *execCtx) execute() {
	exec.log.Debug("serving request...")

	// perform local operation
	exec.executeLocal()

	exec.analyzeStatus(true)
}

func (exec *execCtx) analyzeStatus(execCnr bool) {
	// analyze local result
	switch exec.status {
	case statusOK:
		exec.log.Debug("operation finished successfully")
	case statusINHUMED:
		exec.log.Debug("requested object was marked as removed")
	case statusVIRTUAL:
		exec.log.Debug("requested object is virtual")
		exec.assemble()
	case statusOutOfRange:
		exec.log.Debug("requested range is out of object bounds")
	default:
		exec.log.Debug("operation finished with error",
			zap.String("error", exec.err.Error()),
		)

		if execCnr {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	}
}
