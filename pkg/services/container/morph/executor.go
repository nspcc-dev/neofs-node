package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

type morphExecutor struct {
	wrapper *wrapper.Wrapper

	rdr Reader
}

// Reader is an interface of read-only container storage.
type Reader interface {
	containercore.Source
	eacl.Source

	// List returns a list of container identifiers belonging
	// to the specified owner of NeoFS system. Returns the identifiers
	// of all NeoFS containers if pointer to owner identifier is nil.
	List(*owner.ID) ([]*cid.ID, error)
}

func NewExecutor(w *wrapper.Wrapper, rdr Reader) containerSvc.ServiceExecutor {
	return &morphExecutor{
		wrapper: w,
		rdr:     rdr,
	}
}

func (s *morphExecutor) Put(ctx containerSvc.ContextWithToken, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	cnr := containerSDK.NewContainerFromV2(body.GetContainer())

	cnr.SetSignature(
		pkg.NewSignatureFromV2(body.GetSignature()),
	)

	cnr.SetSessionToken(
		session.NewTokenFromV2(ctx.SessionToken),
	)

	cid, err := wrapper.Put(s.wrapper, cnr)
	if err != nil {
		return nil, err
	}

	res := new(container.PutResponseBody)
	res.SetContainerID(cid.ToV2())

	return res, nil
}

func (s *morphExecutor) Delete(ctx containerSvc.ContextWithToken, body *container.DeleteRequestBody) (*container.DeleteResponseBody, error) {
	id := cid.NewFromV2(body.GetContainerID())
	sig := body.GetSignature().GetSign()
	tok := session.NewTokenFromV2(ctx.SessionToken)

	var rmWitness containercore.RemovalWitness

	rmWitness.SetContainerID(id)
	rmWitness.SetSignature(sig)
	rmWitness.SetSessionToken(tok)

	err := wrapper.Delete(s.wrapper, rmWitness)
	if err != nil {
		return nil, err
	}

	return new(container.DeleteResponseBody), nil
}

func (s *morphExecutor) Get(ctx context.Context, body *container.GetRequestBody) (*container.GetResponseBody, error) {
	id := cid.NewFromV2(body.GetContainerID())

	cnr, err := s.rdr.Get(id)
	if err != nil {
		return nil, err
	}

	res := new(container.GetResponseBody)
	res.SetContainer(cnr.ToV2())
	res.SetSignature(cnr.Signature().ToV2())
	res.SetSessionToken(cnr.SessionToken().ToV2())

	return res, nil
}

func (s *morphExecutor) List(ctx context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
	oid := owner.NewIDFromV2(body.GetOwnerID())

	cnrs, err := s.rdr.List(oid)
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

func (s *morphExecutor) SetExtendedACL(ctx containerSvc.ContextWithToken, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	table := eaclSDK.NewTableFromV2(body.GetEACL())
	sign := pkg.NewSignatureFromV2(body.GetSignature())

	table.SetSignature(sign)

	table.SetSessionToken(
		session.NewTokenFromV2(ctx.SessionToken),
	)

	err := wrapper.PutEACL(s.wrapper, table)

	return new(container.SetExtendedACLResponseBody), err
}

func (s *morphExecutor) GetExtendedACL(ctx context.Context, body *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error) {
	id := cid.NewFromV2(body.GetContainerID())

	table, err := s.rdr.GetEACL(id)
	if err != nil {
		return nil, err
	}

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(table.ToV2())
	res.SetSignature(table.Signature().ToV2())
	res.SetSessionToken(table.SessionToken().ToV2())

	return res, nil
}
