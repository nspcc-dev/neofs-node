package local

import (
	"bytes"
	"errors"

	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"go.uber.org/zap"
)

type TrustStorage struct {
	Log *zap.Logger

	Storage *truststorage.Storage

	NmSrc netmapcore.Source

	LocalKey []byte
}

func (s *TrustStorage) InitIterator(ctx reputationcommon.Context) (trustcontroller.Iterator, error) {
	epoch := ctx.Epoch()

	s.Log.Debug("initializing iterator over trusts",
		zap.Uint64("epoch", epoch),
	)

	epochStorage, err := s.Storage.DataForEpoch(epoch)
	if err != nil && !errors.Is(err, truststorage.ErrNoPositiveTrust) {
		return nil, err
	}

	return &TrustIterator{
		ctx:          ctx,
		storage:      s,
		epochStorage: epochStorage,
	}, nil
}

type TrustIterator struct {
	ctx reputationcommon.Context

	storage *TrustStorage

	epochStorage *truststorage.EpochTrustValueStorage
}

func (it *TrustIterator) Iterate(h reputation.TrustHandler) error {
	if it.epochStorage != nil {
		err := it.epochStorage.Iterate(h)
		if !errors.Is(err, truststorage.ErrNoPositiveTrust) {
			return err
		}
	}

	nm, err := it.storage.NmSrc.GetNetMapByEpoch(it.ctx.Epoch())
	if err != nil {
		return err
	}

	// find out if local node is presented in netmap
	localIndex := -1

	nmNodes := nm.Nodes()
	for i := range nmNodes {
		if bytes.Equal(nmNodes[i].PublicKey(), it.storage.LocalKey) {
			localIndex = i
			break
		}
	}

	ln := len(nmNodes)
	if localIndex >= 0 && ln > 0 {
		ln--
	}

	// calculate Pj http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Chapter 4.5.
	p := reputation.TrustOne.Div(reputation.TrustValueFromInt(ln))

	for i := range nmNodes {
		if i == localIndex {
			continue
		}

		var trusted, trusting apireputation.PeerID

		trusted.SetPublicKey(nmNodes[i].PublicKey())
		trusting.SetPublicKey(it.storage.LocalKey)

		trust := reputation.Trust{}
		trust.SetPeer(trusted)
		trust.SetValue(p)
		trust.SetTrustingPeer(trusting)

		if err := h(trust); err != nil {
			return err
		}
	}

	return nil
}
