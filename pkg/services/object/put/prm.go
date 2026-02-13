package putsvc

import (
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

type PutInitPrm struct {
	common *util.CommonPrm

	hdr *object.Object

	cnr containerSDK.Container

	copiesNumber uint32

	relay func(client.MultiAddressClient) error

	containerNodes       ContainerNodes
	ecPart               iec.PartInfo
	localNodeInContainer bool
	localSignerRFC6979   neofscrypto.Signer
	localNodeSigner      neofscrypto.Signer
	sessionSigner        neofscrypto.Signer
}

type PutChunkPrm struct {
	chunk []byte
}

func (p *PutInitPrm) WithCommonPrm(v *util.CommonPrm) *PutInitPrm {
	if p != nil {
		p.common = v
	}

	return p
}

func (p *PutInitPrm) WithObject(v *object.Object) *PutInitPrm {
	if p != nil {
		p.hdr = v
	}

	return p
}

func (p *PutInitPrm) WithRelay(f func(client.MultiAddressClient) error) *PutInitPrm {
	if p != nil {
		p.relay = f
	}

	return p
}

func (p *PutInitPrm) WithCopiesNumber(cn uint32) *PutInitPrm {
	if p != nil {
		p.copiesNumber = cn
	}

	return p
}

func (p *PutChunkPrm) WithChunk(v []byte) *PutChunkPrm {
	if p != nil {
		p.chunk = v
	}

	return p
}
