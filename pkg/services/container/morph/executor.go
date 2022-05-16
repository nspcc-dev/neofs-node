package container

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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

// ErrInvalidContext is thrown by morph ServiceExecutor when provided session
// token does not contain expected container context.
var ErrInvalidContext = errors.New("session token does not contain container context")

func NewExecutor(rdr Reader, wrt Writer) containerSvc.ServiceExecutor {
	return &morphExecutor{
		rdr: rdr,
		wrt: wrt,
	}
}

func (s *morphExecutor) Put(ctx containerSvc.ContextWithToken, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	cnr := containerSDK.NewContainerFromV2(body.GetContainer())

	var sig neofscrypto.Signature
	sig.ReadFromV2(*sigV2)

	cnr.SetSignature(&sig)

	tok := session.NewTokenFromV2(ctx.SessionToken)
	if ctx.SessionToken != nil && session.GetContainerContext(tok) == nil {
		return nil, ErrInvalidContext
	}

	cnr.SetSessionToken(tok)

	idCnr, err := s.wrt.Put(cnr)
	if err != nil {
		return nil, err
	}

	var idCnrV2 refs.ContainerID
	idCnr.WriteToV2(&idCnrV2)

	res := new(container.PutResponseBody)
	res.SetContainerID(&idCnrV2)

	return res, nil
}

func (s *morphExecutor) Delete(ctx containerSvc.ContextWithToken, body *container.DeleteRequestBody) (*container.DeleteResponseBody, error) {
	idV2 := body.GetContainerID()
	if idV2 == nil {
		return nil, errors.New("missing container ID")
	}

	var id cid.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	sig := body.GetSignature().GetSign()

	tok := session.NewTokenFromV2(ctx.SessionToken)
	if ctx.SessionToken != nil && session.GetContainerContext(tok) == nil {
		return nil, ErrInvalidContext
	}

	var rmWitness containercore.RemovalWitness

	rmWitness.SetContainerID(&id)
	rmWitness.SetSignature(sig)
	rmWitness.SetSessionToken(tok)

	err = s.wrt.Delete(rmWitness)
	if err != nil {
		return nil, err
	}

	return new(container.DeleteResponseBody), nil
}

func (s *morphExecutor) Get(ctx context.Context, body *container.GetRequestBody) (*container.GetResponseBody, error) {
	idV2 := body.GetContainerID()
	if idV2 == nil {
		return nil, errors.New("missing container ID")
	}

	var id cid.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	cnr, err := s.rdr.Get(&id)
	if err != nil {
		return nil, err
	}

	var sigV2 *refs.Signature

	if sig := cnr.Signature(); sig != nil {
		sigV2 = new(refs.Signature)
		sig.WriteToV2(sigV2)
	}

	res := new(container.GetResponseBody)
	res.SetContainer(cnr.ToV2())
	res.SetSignature(sigV2)
	res.SetSessionToken(cnr.SessionToken().ToV2())

	return res, nil
}

func (s *morphExecutor) List(ctx context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
	oid := owner.NewIDFromV2(body.GetOwnerID())

	cnrs, err := s.rdr.List(oid)
	if err != nil {
		return nil, err
	}

	cidList := make([]refs.ContainerID, len(cnrs))
	for i := range cnrs {
		cnrs[i].WriteToV2(&cidList[i])
	}

	res := new(container.ListResponseBody)
	res.SetContainerIDs(cidList)

	return res, nil
}

func (s *morphExecutor) SetExtendedACL(ctx containerSvc.ContextWithToken, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	table := eaclSDK.NewTableFromV2(body.GetEACL())

	var sig neofscrypto.Signature
	sig.ReadFromV2(*sigV2)

	table.SetSignature(&sig)

	tok := session.NewTokenFromV2(ctx.SessionToken)
	if ctx.SessionToken != nil && session.GetContainerContext(tok) == nil {
		return nil, ErrInvalidContext
	}

	table.SetSessionToken(tok)

	err := s.wrt.PutEACL(table)
	if err != nil {
		return nil, err
	}

	return new(container.SetExtendedACLResponseBody), nil
}

func (s *morphExecutor) GetExtendedACL(ctx context.Context, body *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error) {
	idV2 := body.GetContainerID()
	if idV2 == nil {
		return nil, errors.New("missing container ID")
	}

	var id cid.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	table, err := s.rdr.GetEACL(&id)
	if err != nil {
		return nil, err
	}

	var sigV2 *refs.Signature

	if sig := table.Signature(); sig != nil {
		sigV2 = new(refs.Signature)
		sig.WriteToV2(sigV2)
	}

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(table.ToV2())
	res.SetSignature(sigV2)
	res.SetSessionToken(table.SessionToken().ToV2())

	return res, nil
}
