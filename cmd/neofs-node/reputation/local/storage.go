package local

import (
	"bytes"

	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
)

type TrustStorage struct {
	Log *logger.Logger

	Storage *truststorage.Storage

	NmSrc netmapcore.Source

	LocalKey []byte
}

func (s *TrustStorage) InitIterator(ctx reputationcommon.Context) (trustcontroller.Iterator, error) {
	epochStorage, err := s.Storage.DataForEpoch(ctx.Epoch())
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

	for i := range nm.Nodes {
		if bytes.Equal(nm.Nodes[i].PublicKey(), it.storage.LocalKey) {
			localIndex = i
			break
		}
	}

	ln := len(nm.Nodes)
	if localIndex >= 0 && ln > 0 {
		ln--
	}

	// calculate Pj http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Chapter 4.5.
	p := reputation.TrustOne.Div(reputation.TrustValueFromInt(ln))

	for i := range nm.Nodes {
		if i == localIndex {
			continue
		}

		trust := reputation.Trust{}
		trust.SetPeer(reputation.PeerIDFromBytes(nm.Nodes[i].PublicKey()))
		trust.SetValue(p)
		trust.SetTrustingPeer(reputation.PeerIDFromBytes(it.storage.LocalKey))

		if err := h(trust); err != nil {
			return err
		}
	}

	return nil
}
