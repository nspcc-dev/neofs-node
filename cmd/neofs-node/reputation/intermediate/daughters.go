package intermediate

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/daughters"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

type DaughterStorageWriterProvider struct {
	Log     *logger.Logger
	Storage *daughters.Storage
}

type DaughterTrustWriter struct {
	log     *logger.Logger
	storage *daughters.Storage
}

func (w *DaughterTrustWriter) Write(ctx common.Context, t reputation.Trust) error {
	w.storage.Put(ctx.Epoch(), t)
	return nil
}

func (w *DaughterTrustWriter) Close() error {
	return nil
}

func (s *DaughterStorageWriterProvider) InitWriter(_ reputationcommon.Context) (reputationcommon.Writer, error) {
	return &DaughterTrustWriter{
		log:     s.Log,
		storage: s.Storage,
	}, nil
}
