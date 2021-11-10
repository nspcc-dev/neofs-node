package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
)

type morphExecutor struct {
	rdr Reader
	wrt Writer
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

// Writer is an interface of container storage updater.
type Writer interface {
	// Put stores specified container in the side chain.
	Put(*containerSDK.Container) (*cid.ID, error)
	// Delete removes specified container from the side chain.
	Delete(containercore.RemovalWitness) error
	// PutEACL updates extended ACL table of specified container in the side chain.
	PutEACL(*eaclSDK.Table) error
}

func NewExecutor(rdr Reader, wrt Writer) containerSvc.ServiceExecutor {
	return &morphExecutor{
		rdr: rdr,
		wrt: wrt,
	}
}

func (s *morphExecutor) Put(ctx containerSvc.ContextWithToken, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	cnr := containerSDK.NewContainerFromV2(body.GetContainer())

	cnr.SetSignature(
		signature.NewFromV2(body.GetSignature()),
	)

	cnr.SetSessionToken(
		session.NewTokenFromV2(ctx.SessionToken),
	)

	cid, err := s.wrt.Put(cnr)
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

	err := s.wrt.Delete(rmWitness)
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
	sign := signature.NewFromV2(body.GetSignature())

	table.SetSignature(sign)

	table.SetSessionToken(
		session.NewTokenFromV2(ctx.SessionToken),
	)

	err := s.wrt.PutEACL(table)
	if err != nil {
		return nil, err
	}

	return new(container.SetExtendedACLResponseBody), nil
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
