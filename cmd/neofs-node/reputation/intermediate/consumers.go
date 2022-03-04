package intermediate

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigencalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	consumerstorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/consumers"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

var ErrIncorrectContextPanicMsg = "could not write intermediate trust: passed context incorrect"

// ConsumerStorageWriterProvider is implementation of reputation.WriterProvider
// interface that provides ConsumerTrustWriter writer.
type ConsumerStorageWriterProvider struct {
	Log     *logger.Logger
	Storage *consumerstorage.Storage
}

// ConsumerTrustWriter is implementation of reputation.Writer interface
// that writes passed consumer's Trust values to Consumer storage. After writing
// that values can be used in eigenTrust algorithm's iterations.
type ConsumerTrustWriter struct {
	log     *logger.Logger
	storage *consumerstorage.Storage
	eiCtx   eigencalc.Context
}

func (w *ConsumerTrustWriter) Write(t reputation.Trust) error {
	w.log.Debug("writing received consumer's trusts",
		zap.Uint64("epoch", w.eiCtx.Epoch()),
		zap.Uint32("iteration", w.eiCtx.I()),
		zap.String("trusting_peer", hex.EncodeToString(t.TrustingPeer().Bytes())),
		zap.String("trusted_peer", hex.EncodeToString(t.Peer().Bytes())),
	)

	trust := eigentrust.IterationTrust{Trust: t}

	trust.SetEpoch(w.eiCtx.Epoch())
	trust.SetI(w.eiCtx.I())

	w.storage.Put(trust)
	return nil
}

func (w *ConsumerTrustWriter) Close() error {
	return nil
}

func (s *ConsumerStorageWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	eiCtx, ok := ctx.(eigencalc.Context)
	if !ok {
		panic(ErrIncorrectContextPanicMsg)
	}

	return &ConsumerTrustWriter{
		log:     s.Log,
		storage: s.Storage,
		eiCtx:   eiCtx,
	}, nil
}
