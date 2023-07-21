package intermediate

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/daughters"
	"go.uber.org/zap"
)

// DaughterStorageWriterProvider is an implementation of the reputation.WriterProvider
// interface that provides DaughterTrustWriter writer.
type DaughterStorageWriterProvider struct {
	Log     *zap.Logger
	Storage *daughters.Storage
}

// DaughterTrustWriter is an implementation of the reputation.Writer interface
// that writes passed daughter's Trust values to Daughter storage. After writing
// that, values can be used in eigenTrust algorithm's iterations.
type DaughterTrustWriter struct {
	log     *zap.Logger
	storage *daughters.Storage
	ctx     reputationcommon.Context
}

func (w *DaughterTrustWriter) Write(t reputation.Trust) error {
	w.log.Debug("writing received daughter's trusts",
		zap.Uint64("epoch", w.ctx.Epoch()),
		zap.Stringer("trusting_peer", t.TrustingPeer()),
		zap.Stringer("trusted_peer", t.Peer()),
	)

	w.storage.Put(w.ctx.Epoch(), t)
	return nil
}

func (w *DaughterTrustWriter) Close() error {
	return nil
}

func (s *DaughterStorageWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &DaughterTrustWriter{
		log:     s.Log,
		storage: s.Storage,
		ctx:     ctx,
	}, nil
}
