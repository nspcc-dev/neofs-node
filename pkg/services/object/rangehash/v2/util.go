package rangehashsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	rangehashsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/rangehash"
	"github.com/pkg/errors"
)

func toPrm(req *objectV2.GetRangeHashRequest) (*rangehashsvc.Prm, error) {
	body := req.GetBody()

	var typ pkg.ChecksumType
	switch t := body.GetType(); t {
	default:
		return nil, errors.Errorf("unknown checksum type %v", t)
	case refs.SHA256:
		typ = pkg.ChecksumSHA256
	case refs.TillichZemor:
		typ = pkg.ChecksumTZ
	}

	rngsV2 := body.GetRanges()
	rngs := make([]*object.Range, 0, len(rngsV2))

	for i := range rngsV2 {
		rngs = append(rngs, object.NewRangeFromV2(rngsV2[i]))
	}

	return new(rangehashsvc.Prm).
		WithAddress(
			object.NewAddressFromV2(body.GetAddress()),
		).
		OnlyLocal(req.GetMetaHeader().GetTTL() == 1). // FIXME: use constant
		WithChecksumType(typ).
		FromRanges(rngs...), nil
}

func fromResponse(r *rangehashsvc.Response, typ refs.ChecksumType) *objectV2.GetRangeHashResponse {
	body := new(objectV2.GetRangeHashResponseBody)
	body.SetType(typ)
	body.SetHashList(r.Hashes())

	resp := new(objectV2.GetRangeHashResponse)
	resp.SetBody(body)

	return resp
}
