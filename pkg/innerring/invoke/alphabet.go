package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const (
	emitMethod = "emit"
)

// AlphabetEmit invokes emit method on alphabet contract.
func AlphabetEmit(cli *client.Client, con util.Uint160) error {
	if cli == nil {
		return client.ErrNilClient
	}

	// there is no signature collecting, so we don't need extra fee
	return cli.Invoke(con, 0, emitMethod)
}
