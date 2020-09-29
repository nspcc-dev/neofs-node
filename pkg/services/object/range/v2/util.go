package rangesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func toPrm(req *objectV2.GetRangeRequest) *rangesvc.Prm {
	body := req.GetBody()

	return new(rangesvc.Prm).
		WithAddress(
			object.NewAddressFromV2(body.GetAddress()),
		).
		WithRange(object.NewRangeFromV2(body.GetRange())).
		WithCommonPrm(util.CommonPrmFromV2(req))
}

func fromResponse(stream rangesvc.Streamer) objectV2.GetRangeObjectStreamer {
	return &streamer{
		stream: stream,
		body:   new(objectV2.GetRangeResponseBody),
	}
}
