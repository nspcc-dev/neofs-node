package processors

import (
	"crypto/elliptic"
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/meta"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// TODO
func UpdateMetaPlacement(metaClient *notary.Actor, cid cid.ID, vectors [][]netmap.NodeInfo, policy netmap.PlacementPolicy, nonce uint32) error {
	var placement meta.Placement
	for i, v := range vectors {
		var cnrVector meta.PlacementVector
		cnrVector.REP = uint8(policy.ReplicaNumberByIndex(i))

		kk := make(keys.PublicKeys, 0, len(v))
		for _, n := range v {
			k, err := keys.NewPublicKeyFromBytes(n.PublicKey(), elliptic.P256())
			if err != nil {
				return fmt.Errorf("could not parse public key for %s meta container placement: %w", cid, err)
			}
			kk = append(kk, k)
		}

		sort.Sort(kk)
		cnrVector.Nodes = kk

		placement = append(placement, cnrVector)
	}

	_, err := metaClient.WaitSuccess(metaClient.Notarize(metaClient.MakeTunedCall(meta.Hash, "updateContainerList", nil, func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != vmstate.Halt.String() {
			return fmt.Errorf("script failed (%s state) due to an error: %s", r.State, r.FaultException)
		}

		vub, err := CalculateVUB(metaClient)
		if err != nil {
			return fmt.Errorf("could not calculate vub: %w", err)
		}

		t.ValidUntilBlock = vub
		t.Nonce = nonce

		// Add 10% GAS to prevent this errors:
		// "at instruction 1689 (SYSCALL): System.Runtime.Log failed: insufficient amount of gas"
		t.SystemFee += t.SystemFee / 10

		return nil
	}, cid[:], &placement)))
	return err
}

// TODO
func CalculateVUB(cli *notary.Actor) (uint32, error) {
	bc, err := cli.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get current blockchain height: %w", err)
	}

	const (
		defaultNotaryValidTime = 50
		defaultNotaryRoundTime = 100
	)

	minIndex := bc + defaultNotaryValidTime
	rounded := (minIndex/defaultNotaryRoundTime + 1) * defaultNotaryRoundTime

	return rounded, nil
}
