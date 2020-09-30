package deletesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func toPrm(req *objectV2.DeleteRequest) *deletesvc.Prm {
	body := req.GetBody()

	return new(deletesvc.Prm).
		WithAddress(
			object.NewAddressFromV2(body.GetAddress()),
		).
		WithCommonPrm(util.CommonPrmFromV2(req))
}

func fromResponse(r *deletesvc.Response) *objectV2.DeleteResponse {
	return new(objectV2.DeleteResponse)
}
