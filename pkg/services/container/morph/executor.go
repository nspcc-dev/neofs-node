package container

import (
	"context"
	"fmt"

	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
)

type morphExecutor struct {
	wrapper *wrapper.Wrapper
}

func NewExecutor(w *wrapper.Wrapper) containerSvc.ServiceExecutor {
	return &morphExecutor{
		wrapper: w,
	}
}

func (s *morphExecutor) Put(ctx context.Context, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	cnr, err := containerSDK.NewVerifiedFromV2(body.GetContainer())
	if err != nil {
		return nil, fmt.Errorf("invalid format of the container structure: %w", err)
	}

	sig := body.GetSignature()

	cid, err := s.wrapper.Put(cnr, sig.GetKey(), sig.GetSign())
	if err != nil {
		return nil, err
	}

	res := new(container.PutResponseBody)
	res.SetContainerID(cid.ToV2())

	return res, nil
}

func (s *morphExecutor) Delete(ctx context.Context, body *container.DeleteRequestBody) (*container.DeleteResponseBody, error) {
	cid := containerSDK.NewIDFromV2(body.GetContainerID())

	err := s.wrapper.Delete(cid, body.GetSignature().GetSign())
	if err != nil {
		return nil, err
	}

	return new(container.DeleteResponseBody), nil
}

func (s *morphExecutor) Get(ctx context.Context, body *container.GetRequestBody) (*container.GetResponseBody, error) {
	cid := containerSDK.NewIDFromV2(body.GetContainerID())

	cnr, err := s.wrapper.Get(cid)
	if err != nil {
		return nil, err
	}

	res := new(container.GetResponseBody)
	res.SetContainer(cnr.ToV2())

	return res, nil
}

func (s *morphExecutor) List(ctx context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
	oid := owner.NewIDFromV2(body.GetOwnerID())

	cnrs, err := s.wrapper.List(oid)
	if err != nil {
		return nil, err
	}

	cidList := make([]*refs.ContainerID, 0, len(cnrs))
	for i := range cnrs {
		cidList = append(cidList, cnrs[i].ToV2())
	}

	res := new(container.ListResponseBody)
	res.SetContainerIDs(cidList)

	return res, nil
}

func (s *morphExecutor) SetExtendedACL(ctx context.Context, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	table := eaclSDK.NewTableFromV2(body.GetEACL())

	err := s.wrapper.PutEACL(table, body.GetSignature().GetSign())

	return new(container.SetExtendedACLResponseBody), err
}

func (s *morphExecutor) GetExtendedACL(ctx context.Context, body *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error) {
	cid := containerSDK.NewIDFromV2(body.GetContainerID())

	table, signature, err := s.wrapper.GetEACL(cid)
	if err != nil {
		return nil, err
	}

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(table.ToV2())
	res.SetSignature(signature.ToV2())

	return res, nil
}
