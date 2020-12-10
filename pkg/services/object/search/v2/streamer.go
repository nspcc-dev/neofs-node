package searchsvc

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

type streamWriter struct {
	stream objectSvc.SearchStream
}

func (s *streamWriter) WriteIDs(ids []*objectSDK.ID) error {
	r := new(object.SearchResponse)

	body := new(object.SearchResponseBody)
	r.SetBody(body)

	idsV2 := make([]*refs.ObjectID, len(ids))

	for i := range ids {
		idsV2[i] = ids[i].ToV2()
	}

	body.SetIDList(idsV2)

	return s.stream.Send(r)
}
