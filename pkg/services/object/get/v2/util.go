package getsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func toPrm(req *objectV2.GetRequest) *getsvc.Prm {
	return new(getsvc.Prm).
		WithAddress(
			object.NewAddressFromV2(req.GetBody().GetAddress()),
		).
		WithCommonPrm(util.CommonPrmFromV2(req))
}

func fromResponse(res *getsvc.Streamer) objectV2.GetObjectStreamer {
	return &streamer{
		stream: res,
		body:   new(objectV2.GetResponseBody),
	}
}
