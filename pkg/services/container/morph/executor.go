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

	invalidator Invalidator
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

// Invalidator is an interface of local cache invalidator. It removes cached
// values in order to synchronize updated data in the side chain faster.
type Invalidator interface {
	// InvalidateContainer from the local container cache if it exists.
	InvalidateContainer(*cid.ID)
	// InvalidateEACL from the local eACL cache if it exists.
	InvalidateEACL(*cid.ID)
	// InvalidateContainerList from the local cache of container list results
	// if it exists.
	InvalidateContainerList(*owner.ID)
	// InvalidateContainerListByCID from the local cache of container list
	// results if it exists. Container list source uses owner.ID as a key,
	// so invalidating cache record by the value requires different approach.
	InvalidateContainerListByCID(*cid.ID)
}

func NewExecutor(w *wrapper.Wrapper, rdr Reader, i Invalidator) containerSvc.ServiceExecutor {
	return &morphExecutor{
		wrapper:     w,
		rdr:         rdr,
		invalidator: i,
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

	s.invalidator.InvalidateContainerList(cnr.OwnerID())

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

	s.invalidator.InvalidateContainer(id)
	s.invalidator.InvalidateEACL(id)

	// it is faster to use slower invalidation by CID than making separate
	// network request to fetch owner ID of the container.
	s.invalidator.InvalidateContainerListByCID(id)

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
	if err != nil {
		return nil, err
	}

	s.invalidator.InvalidateEACL(table.CID())

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
