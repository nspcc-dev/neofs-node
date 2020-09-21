package putsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
)

type PutInitPrm struct {
	local bool

	hdr *object.RawObject

	token *token.SessionToken

	traverseOpts []placement.Option
}

type PutChunkPrm struct {
	chunk []byte
}

func (p *PutInitPrm) WithObject(v *object.RawObject) *PutInitPrm {
	if p != nil {
		p.hdr = v
	}

	return p
}

func (p *PutInitPrm) WithSession(v *token.SessionToken) *PutInitPrm {
	if p != nil {
		p.token = v
	}

	return p
}

func (p *PutInitPrm) OnlyLocal(v bool) *PutInitPrm {
	if p != nil {
		p.local = v
	}

	return p
}

func (p *PutChunkPrm) WithChunk(v []byte) *PutChunkPrm {
	if p != nil {
		p.chunk = v
	}

	return p
}
