package invoke

import (
	"crypto/sha256"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
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

var ErrParseTestInvoke = errors.New("can't parse NEO node response")

const (
	putContainerMethod    = "put"
	deleteContainerMethod = "delete"
	listContainersMethod  = "list"
)

// RegisterContainer invokes Put method.
func RegisterContainer(cli *client.Client, con util.Uint160, p *ContainerParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, 5*extraFee, putContainerMethod,
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

	return cli.Invoke(con, extraFee, deleteContainerMethod,
		p.ContainerID,
		p.Signature,
	)
}

func ListContainers(cli *client.Client, con util.Uint160) ([]*container.ID, error) {
	if cli == nil {
		return nil, client.ErrNilClient
	}

	item, err := cli.TestInvoke(con, listContainersMethod, []byte{})
	if err != nil {
		return nil, err
	}

	if len(item) < 1 {
		return nil, errors.Wrap(ErrParseTestInvoke, "nested array expected")
	}

	rawIDs, err := client.ArrayFromStackItem(item[0])
	if err != nil {
		return nil, err
	}

	result := make([]*container.ID, 0, len(rawIDs))

	var bufHash [sha256.Size]byte

	for i := range rawIDs {
		cid, err := client.BytesFromStackItem(rawIDs[i])
		if err != nil {
			return nil, err
		}

		if len(cid) != sha256.Size {
			return nil, errors.Wrap(ErrParseTestInvoke, "invalid container ID size")
		}

		copy(bufHash[:], cid)

		containerID := container.NewID()
		containerID.SetSHA256(bufHash)

		result = append(result, containerID)
	}

	return result, nil
}
