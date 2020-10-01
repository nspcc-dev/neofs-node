package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	queryV1 "github.com/nspcc-dev/neofs-node/pkg/services/object/search/query/v1"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

func toPrm(body *object.SearchRequestBody, req *object.SearchRequest) (*searchsvc.Prm, error) {
	var q query.Query

	switch v := body.GetVersion(); v {
	default:
		return nil, errors.Errorf("unsupported query version #%d", v)
	case 1:
		q = queryV1.New(
			objectSDK.NewSearchFiltersFromV2(body.GetFilters()),
		)
	}

	return new(searchsvc.Prm).
		WithContainerID(
			container.NewIDFromV2(body.GetContainerID()),
		).
		WithSearchQuery(q).
		WithCommonPrm(util.CommonPrmFromV2(req)), nil
}

func fromResponse(r *searchsvc.Response) *object.SearchResponse {
	ids := r.IDList()
	idsV2 := make([]*refs.ObjectID, 0, len(ids))

	for i := range ids {
		idsV2 = append(idsV2, ids[i].ToV2())
	}

	body := new(object.SearchResponseBody)
	body.SetIDList(idsV2)

	resp := new(object.SearchResponse)
	resp.SetBody(body)

	return resp
}
