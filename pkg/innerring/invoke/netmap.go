package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const (
	getEpochMethod    = "Epoch"
	setNewEpochMethod = "NewEpoch"
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

	epoch, err := client.IntFromStackParameter(val[0])
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
