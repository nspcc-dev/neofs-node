package invoke

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	UpdatePeerArgs struct {
		Key    *keys.PublicKey
		Status netmap.NodeState
	}

	SetConfigArgs struct {
		ID    []byte
		Key   []byte
		Value []byte
	}
)

const (
	getEpochMethod          = "epoch"
	setNewEpochMethod       = "newEpoch"
	approvePeerMethod       = "addPeer"
	updatePeerStateMethod   = "updateState"
	setConfigMethod         = "setConfig"
	setInnerRingMethod      = "updateInnerRing"
	getNetmapSnapshotMethod = "netmap"
)

// Epoch return epoch value from contract.
func Epoch(cli *client.Client, con util.Uint160) (int64, error) {
	if cli == nil {
		return 0, client.ErrNilClient
	}

	val, err := cli.TestInvoke(con, getEpochMethod)
	if err != nil {
		return 0, err
	}

	epoch, err := client.IntFromStackItem(val[0])
	if err != nil {
		return 0, err
	}

	return epoch, nil
}

// SetNewEpoch invokes newEpoch method.
func SetNewEpoch(cli *client.Client, con util.Uint160, fee SideFeeProvider, epoch uint64) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), setNewEpochMethod, int64(epoch))
}

// ApprovePeer invokes addPeer method.
func ApprovePeer(cli *client.Client, con util.Uint160, fee SideFeeProvider, peer []byte) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), approvePeerMethod, peer)
}

// UpdatePeerState invokes addPeer method.
func UpdatePeerState(cli *client.Client, con util.Uint160, fee SideFeeProvider, args *UpdatePeerArgs) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), updatePeerStateMethod,
		int64(args.Status.ToV2()),
		args.Key.Bytes(),
	)
}

// SetConfig invokes setConfig method.
func SetConfig(cli *client.Client, con util.Uint160, fee SideFeeProvider, args *SetConfigArgs) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), setConfigMethod,
		args.ID,
		args.Key,
		args.Value,
	)
}

// SetInnerRing invokes update inner ring method. This should be used only
// without notary support.
func SetInnerRing(cli *client.Client, con util.Uint160, fee SideFeeProvider, list keys.PublicKeys) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), setInnerRingMethod, list)
}

// NetmapSnapshot returns current netmap node infos.
// Consider using pkg/morph/client/netmap for this.
func NetmapSnapshot(cli *client.Client, con util.Uint160) (*netmap.Netmap, error) {
	if cli == nil {
		return nil, client.ErrNilClient
	}

	rawNetmapStack, err := cli.TestInvoke(con, getNetmapSnapshotMethod)
	if err != nil {
		return nil, err
	}

	if ln := len(rawNetmapStack); ln != 1 {
		return nil, errors.New("invalid RPC response")
	}

	rawNodeInfos, err := client.ArrayFromStackItem(rawNetmapStack[0])
	if err != nil {
		return nil, err
	}

	result := make([]netmap.NodeInfo, 0, len(rawNodeInfos))

	for i := range rawNodeInfos {
		nodeInfo, err := peerInfoFromStackItem(rawNodeInfos[i])
		if err != nil {
			return nil, fmt.Errorf("invalid RPC response: %w", err)
		}

		result = append(result, *nodeInfo)
	}

	return netmap.NewNetmap(netmap.NodesFromInfo(result))
}

func peerInfoFromStackItem(prm stackitem.Item) (*netmap.NodeInfo, error) {
	node := netmap.NewNodeInfo()

	subItems, err := client.ArrayFromStackItem(prm)
	if err != nil {
		return nil, err
	} else if ln := len(subItems); ln != 1 {
		return nil, errors.New("invalid RPC response")
	} else if rawNodeInfo, err := client.BytesFromStackItem(subItems[0]); err != nil {
		return nil, err
	} else if err = node.Unmarshal(rawNodeInfo); err != nil {
		return nil, err
	}

	return node, nil
}
