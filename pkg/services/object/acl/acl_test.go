package acl

import (
	"bytes"
	"testing"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	// for args testing

	tb testing.TB

	clientUsr    user.ID
	clientPubKey []byte

	requestedCnr cid.ID
	requestedObj *oid.ID

	// dynamic state

	containerInfoErr error
	containerInfo    ContainerInfo

	curEpochErr error
	curEpoch    uint64

	eACLErr error
	eACL    eacl.Table

	usrErr    error
	usr       user.ID
	usrPubKey []byte

	innerRingErr error
	innerRingKey []byte

	containerNodeErr error
	containerNodeKey []byte

	localStorageErr    error
	localStorageHeader object.Object
}

func newTestNode(tb testing.TB, requestedContainer cid.ID, clientUsr user.ID, clientPubKey []byte) *testNode {
	return &testNode{
		tb:           tb,
		clientUsr:    clientUsr,
		clientPubKey: clientPubKey,
		requestedCnr: requestedContainer,
	}
}

func (x *testNode) setContainerInfo(info ContainerInfo) {
	x.containerInfo = info
	x.containerInfoErr = nil
}

func (x *testNode) setContainerInfoError(err error) {
	x.containerInfoErr = err
}

func (x *testNode) GetContainerInfo(cnr cid.ID) (ContainerInfo, error) {
	require.True(x.tb, cnr.Equals(x.requestedCnr), "only requested container's info should be read")
	return x.containerInfo, x.containerInfoErr
}

func (x *testNode) setEACL(eACL eacl.Table) {
	x.eACL = eACL
	x.eACLErr = nil
}

func (x *testNode) setEACLError(err error) {
	x.eACLErr = err
}

func (x *testNode) GetExtendedACL(cnr cid.ID) (eacl.Table, error) {
	require.True(x.tb, cnr.Equals(x.requestedCnr), "only requested container's eACL should be read")
	return x.eACL, x.eACLErr
}

func (x *testNode) setCurrentEpoch(epoch uint64) {
	x.curEpoch = epoch
	x.curEpochErr = nil
}

func (x *testNode) setCurrentEpochError(err error) {
	x.curEpochErr = err
}

func (x *testNode) CurrentEpoch() (uint64, error) {
	if x.curEpochErr != nil {
		return 0, x.curEpochErr
	}

	return x.curEpoch, nil
}

func (x *testNode) setUserPublicKey(usr user.ID, bPublicKey []byte) {
	x.usr = usr
	x.usrPubKey = bPublicKey
	x.usrErr = nil
}

func (x *testNode) ResolveUserByPublicKey(bPublicKey []byte) (user.ID, error) {
	require.Equal(x.tb, x.clientPubKey, bPublicKey, "only client public key must be processed")
	return x.usr, x.usrErr
}

func (x *testNode) IsUserPublicKey(usr user.ID, bPublicKey []byte) (bool, error) {
	require.Equal(x.tb, x.clientUsr, usr, "only client user must be processed")
	require.Equal(x.tb, x.clientPubKey, bPublicKey, "only client public key must be processed")
	return bytes.Equal(x.usrPubKey, bPublicKey), x.usrErr
}

func (x *testNode) IsInnerRingPublicKey(bPublicKey []byte) (bool, error) {
	require.Equal(x.tb, x.clientPubKey, bPublicKey, "only client public key must be checked as Inner Ring")
	return bytes.Equal(x.innerRingKey, bPublicKey), x.innerRingErr
}

func (x *testNode) IsContainerNodePublicKey(cnr cid.ID, bPublicKey []byte) (bool, error) {
	require.True(x.tb, cnr.Equals(x.requestedCnr), "only requested container's node keys should be read")
	require.Equal(x.tb, x.clientPubKey, bPublicKey, "only client public key must be checked as container node")
	return bytes.Equal(x.containerNodeKey, bPublicKey), x.containerNodeErr
}

func (x *testNode) setLocalObjectHeader(hdr object.Object) {
	x.localStorageHeader = hdr
	x.localStorageErr = nil
}

func (x *testNode) setLocalStorageError(err error) {
	x.localStorageErr = err
}

func (x *testNode) ReadLocalObjectHeaders(cnr cid.ID, id oid.ID) (object.Object, error) {
	require.True(x.tb, cnr.Equals(x.requestedCnr), "only requested container should be read from local storage")
	require.NotNil(x.tb, x.requestedObj, "only operations with single object context should touch local storage")
	require.Equal(x.tb, *x.requestedObj, id, "only requested object should be read form local storage")
	return x.localStorageHeader, x.localStorageErr
}

func TestNeoFSFailures(t *testing.T) {
}
