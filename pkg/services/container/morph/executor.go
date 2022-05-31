package container

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
	// to the specified user of NeoFS system. Returns the identifiers
	// of all NeoFS containers if pointer to owner identifier is nil.
	List(*user.ID) ([]cid.ID, error)
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

func (s *morphExecutor) Put(_ context.Context, tokV2 *sessionV2.Token, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	cnr := containerSDK.NewContainerFromV2(body.GetContainer())

	var sig neofscrypto.Signature
	sig.ReadFromV2(*sigV2)

	cnr.SetSignature(&sig)

	if tokV2 != nil {
		var tok session.Container

		err := tok.ReadFromV2(*tokV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}

		cnr.SetSessionToken(&tok)
	}

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

func (s *morphExecutor) Delete(_ context.Context, tokV2 *sessionV2.Token, body *container.DeleteRequestBody) (*container.DeleteResponseBody, error) {
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

	var tok *session.Container

	if tokV2 != nil {
		tok = new(session.Container)

		err := tok.ReadFromV2(*tokV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}
	}

	var rmWitness containercore.RemovalWitness

	rmWitness.SetContainerID(id)
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

	cnr, err := s.rdr.Get(id)
	if err != nil {
		return nil, err
	}

	var sigV2 *refs.Signature

	if sig := cnr.Signature(); sig != nil {
		sigV2 = new(refs.Signature)
		sig.WriteToV2(sigV2)
	}

	var tokV2 *sessionV2.Token

	if tok := cnr.SessionToken(); tok != nil {
		tokV2 = new(sessionV2.Token)

		tok.WriteToV2(tokV2)
	}

	res := new(container.GetResponseBody)
	res.SetContainer(cnr.ToV2())
	res.SetSignature(sigV2)
	res.SetSessionToken(tokV2)

	return res, nil
}

func (s *morphExecutor) List(ctx context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
	idV2 := body.GetOwnerID()
	if idV2 == nil {
		return nil, fmt.Errorf("missing user ID")
	}

	var id user.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID: %w", err)
	}

	cnrs, err := s.rdr.List(&id)
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

func (s *morphExecutor) SetExtendedACL(ctx context.Context, tokV2 *sessionV2.Token, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	table := eaclSDK.NewTableFromV2(body.GetEACL())

	var sig neofscrypto.Signature
	sig.ReadFromV2(*sigV2)

	table.SetSignature(&sig)

	if tokV2 != nil {
		var tok session.Container

		err := tok.ReadFromV2(*tokV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}

		table.SetSessionToken(&tok)
	}

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

	table, err := s.rdr.GetEACL(id)
	if err != nil {
		return nil, err
	}

	var sigV2 *refs.Signature

	if sig := table.Signature(); sig != nil {
		sigV2 = new(refs.Signature)
		sig.WriteToV2(sigV2)
	}

	var tokV2 *sessionV2.Token

	if tok := table.SessionToken(); tok != nil {
		tokV2 = new(sessionV2.Token)

		tok.WriteToV2(tokV2)
	}

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(table.ToV2())
	res.SetSignature(sigV2)
	res.SetSessionToken(tokV2)

	return res, nil
}
