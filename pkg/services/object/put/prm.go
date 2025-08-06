package putsvc

import (
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

type PutInitOptions struct {
	cnr containerSDK.Container

	copiesNumber uint32

	relay func(client.NodeInfo, client.MultiAddressClient) error

	containerNodes       ContainerNodes
	ecPart               iec.PartInfo
	localNodeInContainer bool
	localSignerRFC6979   neofscrypto.Signer
	localNodeSigner      neofscrypto.Signer
	sessionSigner        neofscrypto.Signer
}

func (p *PutInitOptions) WithRelay(f func(client.NodeInfo, client.MultiAddressClient) error) *PutInitOptions {
	if p != nil {
		p.relay = f
	}

	return p
}

func (p *PutInitOptions) WithCopiesNumber(cn uint32) *PutInitOptions {
	if p != nil {
		p.copiesNumber = cn
	}

	return p
}
