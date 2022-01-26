package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type streamWriter struct {
	stream objectSvc.SearchStream
}

func (s *streamWriter) WriteIDs(ids []*oidSDK.ID) error {
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
