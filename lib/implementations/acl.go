package implementations

import (
	"context"

	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	libacl "github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
	"github.com/nspcc-dev/neofs-node/lib/container"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
)

// Consider moving ACLHelper implementation to the ACL library.

type (
	// ACLHelper is an interface, that provides useful functions
	// for ACL object pre-processor.
	ACLHelper interface {
		BasicACLGetter
		ContainerOwnerChecker
	}

	// BasicACLGetter helper provides function to return basic ACL value.
	BasicACLGetter interface {
		GetBasicACL(context.Context, CID) (uint32, error)
	}

	// ContainerOwnerChecker checks owner of the container.
	ContainerOwnerChecker interface {
		IsContainerOwner(context.Context, CID, refs.OwnerID) (bool, error)
	}

	aclHelper struct {
		cnr container.Storage
	}
)

type binaryEACLSource struct {
	binaryStore acl.BinaryExtendedACLSource
}

// StaticContractClient is a wrapper over Neo:Morph client
// that invokes single smart contract methods with fixed fee.
type StaticContractClient struct {
	// neo-go client instance
	client *goclient.Client

	// contract script-hash
	scScriptHash util.Uint160

	// invocation fee
	fee util.Fixed8
}

// MorphContainerContract is a wrapper over StaticContractClient
// for Container contract calls.
type MorphContainerContract struct {
	// NeoFS Container smart-contract
	containerContract StaticContractClient

	// set EACL method name of container contract
	eaclSetMethodName string

	// get EACL method name of container contract
	eaclGetMethodName string

	// get container method name of container contract
	cnrGetMethodName string

	// put container method name of container contract
	cnrPutMethodName string

	// delete container method name of container contract
	cnrDelMethodName string

	// list containers method name of container contract
	cnrListMethodName string
}

const (
	errNewACLHelper = internal.Error("cannot create ACLHelper instance")
)

// GetBasicACL returns basic ACL of the container.
func (h aclHelper) GetBasicACL(ctx context.Context, cid CID) (uint32, error) {
	gp := container.GetParams{}
	gp.SetContext(ctx)
	gp.SetCID(cid)

	gResp, err := h.cnr.GetContainer(gp)
	if err != nil {
		return 0, err
	}

	return gResp.Container().BasicACL, nil
}

// IsContainerOwner returns true if provided id is an owner container.
func (h aclHelper) IsContainerOwner(ctx context.Context, cid CID, id refs.OwnerID) (bool, error) {
	gp := container.GetParams{}
	gp.SetContext(ctx)
	gp.SetCID(cid)

	gResp, err := h.cnr.GetContainer(gp)
	if err != nil {
		return false, err
	}

	return gResp.Container().OwnerID.Equal(id), nil
}

// NewACLHelper returns implementation of the ACLHelper interface.
func NewACLHelper(cnr container.Storage) (ACLHelper, error) {
	if cnr == nil {
		return nil, errNewACLHelper
	}

	return aclHelper{cnr}, nil
}

// ExtendedACLSourceFromBinary wraps BinaryExtendedACLSource and returns ExtendedACLSource.
//
// If passed BinaryExtendedACLSource is nil, acl.ErrNilBinaryExtendedACLStore returns.
func ExtendedACLSourceFromBinary(v acl.BinaryExtendedACLSource) (acl.ExtendedACLSource, error) {
	if v == nil {
		return nil, acl.ErrNilBinaryExtendedACLStore
	}

	return &binaryEACLSource{
		binaryStore: v,
	}, nil
}

// GetExtendedACLTable receives eACL table in a binary representation from storage,
// unmarshals it and returns ExtendedACLTable interface.
func (s binaryEACLSource) GetExtendedACLTable(ctx context.Context, cid refs.CID) (libacl.ExtendedACLTable, error) {
	key := acl.BinaryEACLKey{}
	key.SetCID(cid)

	val, err := s.binaryStore.GetBinaryEACL(ctx, key)
	if err != nil {
		return nil, err
	}

	eacl := val.EACL()

	// TODO: verify signature

	res := libacl.WrapEACLTable(nil)

	return res, res.UnmarshalBinary(eacl)
}

// NewStaticContractClient initializes a new StaticContractClient.
//
// If passed Client is nil, goclient.ErrNilClient returns.
func NewStaticContractClient(client *goclient.Client, scHash util.Uint160, fee util.Fixed8) (StaticContractClient, error) {
	res := StaticContractClient{
		client:       client,
		scScriptHash: scHash,
		fee:          fee,
	}

	var err error
	if client == nil {
		err = goclient.ErrNilClient
	}

	return res, err
}

// Invoke calls Invoke method of goclient with predefined script hash and fee.
// Supported args types are the same as in goclient.
//
// If Client is not initialized, goclient.ErrNilClient returns.
func (s StaticContractClient) Invoke(method string, args ...interface{}) error {
	if s.client == nil {
		return goclient.ErrNilClient
	}

	return s.client.Invoke(
		s.scScriptHash,
		s.fee,
		method,
		args...,
	)
}

// TestInvoke calls TestInvoke method of goclient with predefined script hash.
//
// If Client is not initialized, goclient.ErrNilClient returns.
func (s StaticContractClient) TestInvoke(method string, args ...interface{}) ([]sc.Parameter, error) {
	if s.client == nil {
		return nil, goclient.ErrNilClient
	}

	return s.client.TestInvoke(
		s.scScriptHash,
		method,
		args...,
	)
}

// SetContainerContractClient is a container contract client setter.
func (s *MorphContainerContract) SetContainerContractClient(v StaticContractClient) {
	s.containerContract = v
}

// SetEACLGetMethodName is a container contract Get EACL method name setter.
func (s *MorphContainerContract) SetEACLGetMethodName(v string) {
	s.eaclGetMethodName = v
}

// SetEACLSetMethodName is a container contract Set EACL method name setter.
func (s *MorphContainerContract) SetEACLSetMethodName(v string) {
	s.eaclSetMethodName = v
}

// SetContainerGetMethodName is a container contract Get method name setter.
func (s *MorphContainerContract) SetContainerGetMethodName(v string) {
	s.cnrGetMethodName = v
}

// SetContainerPutMethodName is a container contract Put method name setter.
func (s *MorphContainerContract) SetContainerPutMethodName(v string) {
	s.cnrPutMethodName = v
}

// SetContainerDeleteMethodName is a container contract Delete method name setter.
func (s *MorphContainerContract) SetContainerDeleteMethodName(v string) {
	s.cnrDelMethodName = v
}

// SetContainerListMethodName is a container contract List method name setter.
func (s *MorphContainerContract) SetContainerListMethodName(v string) {
	s.cnrListMethodName = v
}

// GetBinaryEACL performs the test invocation call of GetEACL method of NeoFS Container contract.
func (s *MorphContainerContract) GetBinaryEACL(_ context.Context, key acl.BinaryEACLKey) (acl.BinaryEACLValue, error) {
	res := acl.BinaryEACLValue{}

	prms, err := s.containerContract.TestInvoke(
		s.eaclGetMethodName,
		key.CID().Bytes(),
	)
	if err != nil {
		return res, err
	} else if ln := len(prms); ln != 1 {
		return res, errors.Errorf("unexpected stack parameter count: %d", ln)
	}

	eacl, err := goclient.BytesFromStackParameter(prms[0])
	if err == nil {
		res.SetEACL(eacl)
	}

	return res, err
}

// PutBinaryEACL invokes the call of SetEACL method of NeoFS Container contract.
func (s *MorphContainerContract) PutBinaryEACL(_ context.Context, key acl.BinaryEACLKey, val acl.BinaryEACLValue) error {
	return s.containerContract.Invoke(
		s.eaclSetMethodName,
		key.CID().Bytes(),
		val.EACL(),
		val.Signature(),
	)
}

// GetContainer performs the test invocation call of Get method of NeoFS Container contract.
func (s *MorphContainerContract) GetContainer(p container.GetParams) (*container.GetResult, error) {
	prms, err := s.containerContract.TestInvoke(
		s.cnrGetMethodName,
		p.CID().Bytes(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count: %d", ln)
	}

	cnrBytes, err := goclient.BytesFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get byte array from stack item")
	}

	cnr := new(container.Container)
	if err := cnr.Unmarshal(cnrBytes); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal container from bytes")
	}

	res := new(container.GetResult)
	res.SetContainer(cnr)

	return res, nil
}

// PutContainer invokes the call of Put method of NeoFS Container contract.
func (s *MorphContainerContract) PutContainer(p container.PutParams) (*container.PutResult, error) {
	cnr := p.Container()

	cid, err := cnr.ID()
	if err != nil {
		return nil, errors.Wrap(err, "could not calculate container ID")
	}

	cnrBytes, err := cnr.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal container")
	}

	if err := s.containerContract.Invoke(
		s.cnrPutMethodName,
		cnr.OwnerID.Bytes(),
		cnrBytes,
		[]byte{},
	); err != nil {
		return nil, errors.Wrap(err, "could not invoke contract method")
	}

	res := new(container.PutResult)
	res.SetCID(cid)

	return res, nil
}

// DeleteContainer invokes the call of Delete method of NeoFS Container contract.
func (s *MorphContainerContract) DeleteContainer(p container.DeleteParams) (*container.DeleteResult, error) {
	if err := s.containerContract.Invoke(
		s.cnrDelMethodName,
		p.CID().Bytes(),
		p.OwnerID().Bytes(),
		[]byte{},
	); err != nil {
		return nil, errors.Wrap(err, "could not invoke contract method")
	}

	return new(container.DeleteResult), nil
}

// ListContainers performs the test invocation call of Get method of NeoFS Container contract.
//
// If owner ID list in parameters is non-empty, bytes of first owner are attached to call.
func (s *MorphContainerContract) ListContainers(p container.ListParams) (*container.ListResult, error) {
	args := make([]interface{}, 0, 1)

	if ownerIDList := p.OwnerIDList(); len(ownerIDList) > 0 {
		args = append(args, ownerIDList[0].Bytes())
	}

	prms, err := s.containerContract.TestInvoke(
		s.cnrListMethodName,
		args...,
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not perform test invocation")
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count: %d", ln)
	}

	prms, err = goclient.ArrayFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get stack item array from stack item")
	}

	cidList := make([]CID, 0, len(prms))

	for i := range prms {
		cidBytes, err := goclient.BytesFromStackParameter(prms[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get byte array from stack item")
		}

		cid, err := refs.CIDFromBytes(cidBytes)
		if err != nil {
			return nil, errors.Wrap(err, "could not get container ID from bytes")
		}

		cidList = append(cidList, cid)
	}

	res := new(container.ListResult)
	res.SetCIDList(cidList)

	return res, nil
}
