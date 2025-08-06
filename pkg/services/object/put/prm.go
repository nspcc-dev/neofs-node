package putsvc

import (
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

// RelayFunc relays request using given connection to SN.
type RelayFunc = func(client.NodeInfo, client.MultiAddressClient) error

type PutInitOptions struct {
	cnr containerSDK.Container

	copiesNumber uint32

	relay RelayFunc

	containerNodes       ContainerNodes
	ecPart               iec.PartInfo
	localNodeInContainer bool
	localSignerRFC6979   neofscrypto.Signer
	localNodeSigner      neofscrypto.Signer
	sessionSigner        neofscrypto.Signer
}

func (p *PutInitOptions) WithRelay(f RelayFunc) *PutInitOptions {
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
