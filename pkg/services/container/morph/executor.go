package container

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/util/signature"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type morphExecutor struct {
	rdr     Reader
	wrt     Writer
	epocher netmap.State
}

// Reader is an interface of read-only container storage.
type Reader interface {
	containercore.Source
	containercore.EACLSource

	// List returns a list of container identifiers belonging
	// to the specified user of NeoFS system. Returns the identifiers
	// of all NeoFS containers if pointer to owner identifier is nil.
	List(*user.ID) ([]cid.ID, error)
}

// Writer is an interface of container storage updater.
type Writer interface {
	// Put stores specified container in FS chain.
	Put(containercore.Container) (*cid.ID, error)
	// Delete removes specified container from FS chain.
	Delete(containercore.RemovalWitness) error
	// PutEACL updates extended ACL table of specified container in FS chain.
	PutEACL(containercore.EACL) error
}

func NewExecutor(rdr Reader, wrt Writer, epocher netmap.State) containerSvc.ServiceExecutor {
	return &morphExecutor{
		rdr:     rdr,
		wrt:     wrt,
		epocher: epocher,
	}
}

func (s *morphExecutor) Put(_ context.Context, tokV2 *sessionV2.Token, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	cnrV2 := body.GetContainer()
	if cnrV2 == nil {
		return nil, errors.New("missing container field")
	}

	var cnr containercore.Container

	err := cnr.Value.ReadFromV2(*cnrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container: %w", err)
	}

	err = cnr.Signature.ReadFromV2(*sigV2)
	if err != nil {
		return nil, fmt.Errorf("can't read signature: %w", err)
	}

	if tokV2 != nil {
		cnr.Session = new(session.Container)

		err := cnr.Session.ReadFromV2(*tokV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}

		err = s.validateToken(tokV2, nil, sessionV2.ContainerVerbPut)
		if err != nil {
			return nil, fmt.Errorf("session token validation: %w", err)
		}
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

		err = s.validateToken(tokV2, body.GetContainerID(), sessionV2.ContainerVerbDelete)
		if err != nil {
			return nil, fmt.Errorf("session token validation: %w", err)
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

func (s *morphExecutor) Get(_ context.Context, body *container.GetRequestBody) (*container.GetResponseBody, error) {
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

	sigV2 := new(refs.Signature)
	cnr.Signature.WriteToV2(sigV2)

	var tokV2 *sessionV2.Token

	if cnr.Session != nil {
		tokV2 = new(sessionV2.Token)

		cnr.Session.WriteToV2(tokV2)
	}

	var cnrV2 container.Container
	cnr.Value.WriteToV2(&cnrV2)

	res := new(container.GetResponseBody)
	res.SetContainer(&cnrV2)
	res.SetSignature(sigV2)
	res.SetSessionToken(tokV2)

	return res, nil
}

func (s *morphExecutor) List(_ context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
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

func (s *morphExecutor) SetExtendedACL(_ context.Context, tokV2 *sessionV2.Token, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	sigV2 := body.GetSignature()
	if sigV2 == nil {
		// TODO(@cthulhu-rider): #1387 use "const" error
		return nil, errors.New("missing signature")
	}

	var table eaclSDK.Table
	if eacl := body.GetEACL(); eacl != nil {
		if err := table.ReadFromV2(*body.GetEACL()); err != nil {
			return nil, fmt.Errorf("invalid eACL: %w", err)
		}
	}

	eaclInfo := containercore.EACL{
		Value: &table,
	}

	err := eaclInfo.Signature.ReadFromV2(*sigV2)
	if err != nil {
		return nil, fmt.Errorf("can't read signature: %w", err)
	}

	if tokV2 != nil {
		eaclInfo.Session = new(session.Container)

		err := eaclInfo.Session.ReadFromV2(*tokV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}

		err = s.validateToken(tokV2, body.GetEACL().GetContainerID(), sessionV2.ContainerVerbSetEACL)
		if err != nil {
			return nil, fmt.Errorf("session token validation: %w", err)
		}
	}

	err = s.wrt.PutEACL(eaclInfo)
	if err != nil {
		return nil, err
	}

	return new(container.SetExtendedACLResponseBody), nil
}

func (s *morphExecutor) GetExtendedACL(_ context.Context, body *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error) {
	idV2 := body.GetContainerID()
	if idV2 == nil {
		return nil, errors.New("missing container ID")
	}

	var id cid.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	eaclInfo, err := s.rdr.GetEACL(id)
	if err != nil {
		return nil, err
	}

	var sigV2 refs.Signature
	eaclInfo.Signature.WriteToV2(&sigV2)

	var tokV2 *sessionV2.Token

	if eaclInfo.Session != nil {
		tokV2 = new(sessionV2.Token)

		eaclInfo.Session.WriteToV2(tokV2)
	}

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(eaclInfo.Value.ToV2())
	res.SetSignature(&sigV2)
	res.SetSessionToken(tokV2)

	return res, nil
}

type sessionDataSource struct {
	t    *sessionV2.Token
	size int
}

func (d sessionDataSource) ReadSignedData(buff []byte) ([]byte, error) {
	if len(buff) < d.size {
		buff = make([]byte, d.size)
	}

	res := d.t.GetBody().StableMarshal(buff)

	return res[:d.size], nil
}

func (d sessionDataSource) SignedDataSize() int {
	return d.size
}

func newDataSource(t *sessionV2.Token) sessionDataSource {
	return sessionDataSource{
		t:    t,
		size: t.GetBody().StableSize(),
	}
}

func (s *morphExecutor) validateToken(t *sessionV2.Token, cIDV2 *refs.ContainerID, op sessionV2.ContainerSessionVerb) error {
	c := t.GetBody().GetContext()
	cc, ok := c.(*sessionV2.ContainerSessionContext)
	if !ok {
		return errors.New("session is not container-related")
	}

	if verb := cc.Verb(); verb != op {
		return fmt.Errorf("wrong container session operation: %s", verb)
	}

	err := signature.VerifyDataWithSource(newDataSource(t), t.GetSignature)
	if err != nil {
		return fmt.Errorf("incorrect token signature: %w", err)
	}

	lt := t.GetBody().GetLifetime()
	if lt == nil {
		return errors.New("lifetime not set")
	}

	currEpoch := s.epocher.CurrentEpoch()
	exp := lt.GetExp()
	iat := lt.GetIat()
	nbf := lt.GetNbf()

	switch {
	case exp < currEpoch:
		return apistatus.SessionTokenExpired{}
	case iat > currEpoch:
		return fmt.Errorf("token should not be issued yet: IAt: %d, current epoch: %d", iat, currEpoch)
	case nbf > currEpoch:
		return fmt.Errorf("token is not valid yet: NBf: %d, current epoch: %d", nbf, currEpoch)
	}

	if cIDV2 == nil { // can be nil for PUT
		return nil
	}

	var cIDRequested cid.ID

	err = cIDRequested.ReadFromV2(*cIDV2)
	if err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	cnr, err := s.rdr.Get(cIDRequested)
	if err != nil {
		return fmt.Errorf("reading container from the network: %w", err)
	}

	owner := cnr.Value.Owner()
	if issuer := t.GetBody().GetOwnerID().GetValue(); !bytes.Equal(owner[:], issuer) {
		return fmt.Errorf("session was not issued by the container owner, issuer: %s", base58.Encode(issuer))
	}

	var keyFromToken neofsecdsa.PublicKey

	err = keyFromToken.Decode(t.GetSignature().GetKey())
	if err != nil {
		return fmt.Errorf("decoding key from signature: %w", err)
	}

	userFromToken := user.NewFromECDSAPublicKey(ecdsa.PublicKey(keyFromToken))
	if cnr.Value.Owner() != userFromToken {
		return fmt.Errorf("session token signer differs container owner: signer: %s, owner: %s", userFromToken, cnr.Value.Owner())
	}

	if !cc.Wildcard() {
		if sessionCID := cc.ContainerID().GetValue(); !bytes.Equal(sessionCID, cIDV2.GetValue()) {
			return fmt.Errorf("wrong container: %s", base58.Encode(sessionCID))
		}
	}

	return nil
}
