package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// ContainerParams for container put invocation.
	ContainerParams struct {
		Key       *keys.PublicKey
		Container []byte
		Signature []byte
	}

	// RemoveContainerParams for container delete invocation.
	RemoveContainerParams struct {
		ContainerID []byte
		Signature   []byte
	}
)

const (
	putContainerMethod    = "put"
	deleteContainerMethod = "delete"
)

// RegisterContainer invokes Put method.
func RegisterContainer(cli *client.Client, con util.Uint160, fee SideFeeProvider, p *ContainerParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), putContainerMethod,
		p.Container,
		p.Signature,
		p.Key.Bytes(),
	)
}

// RemoveContainer invokes Delete method.
func RemoveContainer(cli *client.Client, con util.Uint160, fee SideFeeProvider, p *RemoveContainerParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), deleteContainerMethod,
		p.ContainerID,
		p.Signature,
	)
}
