package container

import (
	"context"
	"crypto/sha256"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	aclGRPC "github.com/nspcc-dev/neofs-api-go/v2/acl/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	container2 "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	containerMorph "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	"github.com/pkg/errors"
)

type morphExecutor struct {
	// TODO: use client wrapper
	client *containerMorph.Client
}

func NewExecutor(client *containerMorph.Client) containerSvc.ServiceExecutor {
	return &morphExecutor{
		client: client,
	}
}

func (s *morphExecutor) Put(ctx context.Context, body *container.PutRequestBody) (*container.PutResponseBody, error) {
	cnr := body.GetContainer()

	// marshal the container
	cnrBytes, err := cnr.StableMarshal(nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal the container")
	}

	args := containerMorph.PutArgs{}
	args.SetContainer(cnrBytes)
	args.SetSignature(body.GetSignature().GetSign())
	args.SetPublicKey(body.GetSignature().GetKey())

	if err := s.client.Put(args); err != nil {
		return nil, errors.Wrap(err, "could not call Put method")
	}

	res := new(container.PutResponseBody)

	// FIXME: implement and use CID calculation
	cidBytes := sha256.Sum256(cnrBytes)
	cid := new(refs.ContainerID)
	cid.SetValue(cidBytes[:])
	res.SetContainerID(cid)

	return res, nil
}

func (s *morphExecutor) Delete(ctx context.Context, body *container.DeleteRequestBody) (*container.DeleteResponseBody, error) {
	args := containerMorph.DeleteArgs{}
	args.SetCID(body.GetContainerID().GetValue())
	args.SetSignature(body.GetSignature().GetSign())

	if err := s.client.Delete(args); err != nil {
		return nil, errors.Wrap(err, "could not call Delete method")
	}

	return new(container.DeleteResponseBody), nil
}

func (s *morphExecutor) Get(ctx context.Context, body *container.GetRequestBody) (*container.GetResponseBody, error) {
	args := containerMorph.GetArgs{}
	args.SetCID(body.GetContainerID().GetValue())

	val, err := s.client.Get(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not call Get method")
	}

	// FIXME: implement and use stable unmarshaler
	cnrGRPC := new(container2.Container)
	if err := cnrGRPC.Unmarshal(val.Container()); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal the container")
	}

	res := new(container.GetResponseBody)
	res.SetContainer(container.ContainerFromGRPCMessage(cnrGRPC))

	return res, nil
}

func (s *morphExecutor) List(ctx context.Context, body *container.ListRequestBody) (*container.ListResponseBody, error) {
	args := containerMorph.ListArgs{}
	args.SetOwnerID(body.GetOwnerID().GetValue())

	val, err := s.client.List(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not call List method")
	}

	binCidList := val.CIDList()
	cidList := make([]*refs.ContainerID, 0, len(binCidList))

	for i := range binCidList {
		cid := new(refs.ContainerID)
		cid.SetValue(binCidList[i])

		cidList = append(cidList, cid)
	}

	res := new(container.ListResponseBody)
	res.SetContainerIDs(cidList)

	return res, nil
}

func (s *morphExecutor) SetExtendedACL(ctx context.Context, body *container.SetExtendedACLRequestBody) (*container.SetExtendedACLResponseBody, error) {
	eacl := body.GetEACL()

	eaclBytes, err := eacl.StableMarshal(nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal eACL table")
	}

	args := containerMorph.SetEACLArgs{}
	args.SetEACL(eaclBytes)
	args.SetSignature(body.GetSignature().GetSign())

	if err := s.client.SetEACL(args); err != nil {
		return nil, errors.Wrap(err, "could not call SetEACL method")
	}

	return new(container.SetExtendedACLResponseBody), nil
}

func (s *morphExecutor) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequestBody) (*container.GetExtendedACLResponseBody, error) {
	args := containerMorph.EACLArgs{}
	args.SetCID(req.GetContainerID().GetValue())

	val, err := s.client.EACL(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not call EACL method")
	}

	// FIXME: implement and use stable unmarshaler
	eaclGRPC := new(aclGRPC.EACLTable)
	if err := eaclGRPC.Unmarshal(val.EACL()); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal eACL table")
	}

	eacl := acl.TableFromGRPCMessage(eaclGRPC)

	eaclSignature := new(refs.Signature)
	eaclSignature.SetSign(val.Signature())

	res := new(container.GetExtendedACLResponseBody)
	res.SetEACL(eacl)

	// Public key should be obtained by request sender, so we set up only
	// the signature. Technically, node can make invocation to find container
	// owner public key, but request sender cannot trust this info.
	res.SetSignature(eaclSignature)

	return res, nil
}
