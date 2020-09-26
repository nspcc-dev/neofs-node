package getsvc

import (
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	"github.com/pkg/errors"
)

type Streamer struct {
	headSent bool

	rngRes *rangesvc.Result
}

func (p *Streamer) Recv() (interface{}, error) {
	if !p.headSent {
		p.headSent = true
		return p.rngRes.Head(), nil
	}

	rngResp, err := p.rngRes.Stream().Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive range response", p)
	}

	return rngResp.PayloadChunk(), nil
}
