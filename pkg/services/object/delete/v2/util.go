package deletesvc

import (
	"errors"
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type tombstoneBodyWriter struct {
	body *objectV2.DeleteResponseBody
}

func (s *Service) toPrm(req *objectV2.DeleteRequest, respBody *objectV2.DeleteResponseBody) (*deletesvc.Prm, error) {
	body := req.GetBody()

	addrV2 := body.GetAddress()
	if addrV2 == nil {
		return nil, errors.New("missing object address")
	}

	var addr oid.Address

	err := addr.ReadFromV2(*addrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid object address: %w", err)
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(deletesvc.Prm)
	p.SetCommonParameters(commonPrm)

	p.WithAddress(addr)
	p.WithTombstoneAddressTarget(&tombstoneBodyWriter{
		body: respBody,
	})

	return p, nil
}

func (w *tombstoneBodyWriter) SetAddress(addr oid.Address) {
	var addrV2 refs.Address
	addr.WriteToV2(&addrV2)

	w.body.SetTombstone(&addrV2)
}
