package searchsvc

import (
	"context"
	"fmt"
	"math/big"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Search serves a request to select the objects.
//
// Only creation epoch, payload size, user attributes and unknown system ones
// are allowed with numeric operators. Values of numeric filters must be base-10
// integers.
//
// Returns [object.ErrInvalidSearchQuery] if specified query is invalid.
func (s *Service) Search(ctx context.Context, prm Prm) error {
	err := verifyQuery(prm)
	if err != nil {
		return err
	}

	exec := &execCtx{
		svc: s,
		prm: prm,
	}

	exec.prepare()

	exec.setLogger(s.log)

	exec.execute(ctx)

	return exec.statusError.err
}

func (exec *execCtx) execute(ctx context.Context) {
	exec.log.Debug("serving request...")

	// perform local operation
	exec.executeLocal()

	exec.analyzeStatus(ctx, true)
}

func (exec *execCtx) analyzeStatus(ctx context.Context, execCnr bool) {
	// analyze local result
	switch exec.status {
	default:
		exec.log.Debug("operation finished with error",
			zap.Error(exec.err),
		)
	case statusOK:
		exec.log.Debug("operation finished successfully")
	}

	if execCnr {
		exec.executeOnContainer(ctx)
		exec.analyzeStatus(ctx, false)
	}
}

func verifyQuery(prm Prm) error {
	for i := range prm.filters {
		//nolint:exhaustive
		switch prm.filters[i].Operation() {
		case object.MatchNumGT, object.MatchNumGE, object.MatchNumLT, object.MatchNumLE:
			// TODO: big math takes less code but inefficient
			_, ok := new(big.Int).SetString(prm.filters[i].Value(), 10)
			if !ok {
				return fmt.Errorf("%w: invalid filter #%d: numeric filter with non-decimal value",
					objectcore.ErrInvalidSearchQuery, i)
			}
		}
	}

	return nil
}
