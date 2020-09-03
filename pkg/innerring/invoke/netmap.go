package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	UpdatePeerArgs struct {
		Key    *keys.PublicKey
		Status uint32
	}
)

const (
	getEpochMethod        = "epoch"
	setNewEpochMethod     = "newEpoch"
	approvePeerMethod     = "addPeer"
	updatePeerStateMethod = "updateState"
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

// SetNewEpoch invokes NewEpoch method.
func SetNewEpoch(cli *client.Client, con util.Uint160, epoch uint64) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, setNewEpochMethod, int64(epoch))
}

// ApprovePeer invokes AddPeer method.
func ApprovePeer(cli *client.Client, con util.Uint160, peer []byte) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, approvePeerMethod, peer)
}

// UpdatePeerState invokes AddPeer method.
func UpdatePeerState(cli *client.Client, con util.Uint160, args *UpdatePeerArgs) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, updatePeerStateMethod,
		args.Key.Bytes(),
		int64(args.Status),
	)
}
