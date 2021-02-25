package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

const (
	emitMethod = "emit"
	voteMethod = "vote"
)

// AlphabetEmit invokes emit method on alphabet contract.
func AlphabetEmit(cli *client.Client, con util.Uint160) error {
	if cli == nil {
		return client.ErrNilClient
	}

	// there is no signature collecting, so we don't need extra fee
	return cli.Invoke(con, 0, emitMethod)
}

// AlphabetVote invokes vote method on alphabet contract.
func AlphabetVote(cli *client.Client, con util.Uint160, epoch uint64, keys []keys.PublicKey) error {
	if cli == nil {
		return client.ErrNilClient
	}

	binaryKeys := make([][]byte, 0, len(keys))
	for i := range keys {
		binaryKeys = append(binaryKeys, keys[i].Bytes())
	}

	return cli.NotaryInvoke(con, voteMethod, int64(epoch), binaryKeys)
}
