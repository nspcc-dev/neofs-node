package intermediate

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigencalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	consumerstorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/consumers"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

var ErrIncorrectContext = errors.New("could not write intermediate trust: passed context incorrect")

type ConsumerStorageWriterProvider struct {
	Log     *logger.Logger
	Storage *consumerstorage.Storage
}

type ConsumerTrustWriter struct {
	log     *logger.Logger
	storage *consumerstorage.Storage
}

func (w *ConsumerTrustWriter) Write(ctx common.Context, t reputation.Trust) error {
	eiCtx, ok := ctx.(eigencalc.Context)
	if !ok {
		return ErrIncorrectContext
	}

	trust := eigentrust.IterationTrust{Trust: t}

	trust.SetEpoch(eiCtx.Epoch())
	trust.SetI(eiCtx.I())

	w.storage.Put(trust)
	return nil
}

func (w *ConsumerTrustWriter) Close() error {
	return nil
}

func (s *ConsumerStorageWriterProvider) InitWriter(_ reputationcommon.Context) (reputationcommon.Writer, error) {
	return &ConsumerTrustWriter{
		log:     s.Log,
		storage: s.Storage,
	}, nil
}
