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

	// ContainerParams for container put invocation.
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
func RegisterContainer(cli *client.Client, con util.Uint160, p *ContainerParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, putContainerMethod,
		p.Container,
		p.Signature,
		p.Key.Bytes(),
	)
}

// RegisterContainer invokes Delete method.
func RemoveContainer(cli *client.Client, con util.Uint160, p *RemoveContainerParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, deleteContainerMethod,
		p.ContainerID,
		p.Signature,
	)
}
