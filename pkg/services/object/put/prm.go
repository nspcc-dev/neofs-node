package putsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

type PutInitPrm struct {
	common *util.CommonPrm

	hdr *object.Object

	cnr containerSDK.Container

	traverseOpts []placement.Option

	copiesNumber uint32

	relay func(client.NodeInfo, client.MultiAddressClient) error
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

func (p *PutInitPrm) WithRelay(f func(client.NodeInfo, client.MultiAddressClient) error) *PutInitPrm {
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
