package getsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"io"
	"sync/atomic"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

func newCommonParameters(local bool, sTok *session.Object, xs []string) (*util.CommonPrm, error) {
	// TODO: see pkg/services/object/put/service_test.go:494
	var mxs []*protosession.XHeader
	for i := 0; i < len(xs); i += 2 {
		mxs = append(mxs, &protosession.XHeader{
			Key:   xs[i],
			Value: xs[i+1],
		})
	}

	req := &protoobject.GetRequest{
		MetaHeader: &protosession.RequestMetaHeader{
			Ttl:      2,
			XHeaders: mxs,
		},
	}
	if sTok != nil {
		req.MetaHeader.SessionToken = sTok.ProtoMessage()
	}
	if local {
		req.MetaHeader.Ttl = 1
	}

	return util.CommonPrmFromRequest(req)
}

// TODO: share.
type ioReadCloser struct {
	io.Reader
	io.Closer
}

// TODO: share.
type mockIOCloser struct {
	count atomic.Uint32
}

func (x *mockIOCloser) Close() error {
	x.count.Add(1)
	return nil
}

type mockObjectWriter struct {
	writeHeaderErr error
	writeChunkErr  error

	hdr object.Object
	buf bytes.Buffer
}

func (x *mockObjectWriter) WriteHeader(hdr *object.Object) error {
	if x.writeHeaderErr != nil {
		return x.writeHeaderErr
	}
	x.hdr = *hdr.CutPayload()
	return nil
}

func (x *mockObjectWriter) WriteChunk(data []byte) error {
	if x.writeChunkErr != nil {
		return x.writeChunkErr
	}
	_, err := x.buf.Write(data)
	return err
}

type getNodesForObjectValue struct {
	nodeSets [][]netmap.NodeInfo
	repRules []uint
	ecRules  []iec.Rule
	err      error
}

type mockNeoFSNet struct {
	getNodesForObject map[oid.Address]getNodesForObjectValue
	localPubKey       []byte
}

func (x *mockNeoFSNet) GetNodesForObject(addr oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	v, ok := x.getNodesForObject[addr]
	if !ok {
		return nil, nil, nil, errors.New("[test] unexpected object requested")
	}
	return v.nodeSets, v.repRules, v.ecRules, v.err
}

func (x *mockNeoFSNet) IsLocalNodePublicKey(pub []byte) bool {
	return bytes.Equal(pub, x.localPubKey)
}

type getECPartKey struct {
	cnr    cid.ID
	parent oid.ID
	pi     iec.PartInfo
}

type getECPartValue struct {
	hdr object.Object
	rdr io.ReadCloser
	err error
}

type mockLocalObjects struct {
	getECPart map[getECPartKey]getECPartValue
}

func (x *mockLocalObjects) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error) {
	v, ok := x.getECPart[getECPartKey{
		cnr:    cnr,
		parent: parent,
		pi:     pi,
	}]
	if !ok {
		return object.Object{}, nil, errors.New("[test] unexpected object requested")
	}
	return v.hdr, v.rdr, v.err
}

type mockKeyStorage struct {
	privKey ecdsa.PrivateKey
}

func (x *mockKeyStorage) GetKey(*util.SessionInfo) (*ecdsa.PrivateKey, error) {
	return &x.privKey, nil
}

type unimplementedNeoFSNet struct{}

func (x unimplementedNeoFSNet) GetNodesForObject(oid.Address) ([][]netmap.NodeInfo, []uint, []iec.Rule, error) {
	panic("unimplemented")
}

func (x unimplementedNeoFSNet) IsLocalNodePublicKey([]byte) bool {
	panic("unimplemented")
}

type unimplementedObjectWriter struct{}

func (unimplementedObjectWriter) WriteHeader(*object.Object) error {
	panic("unimplemented")
}

func (unimplementedObjectWriter) WriteChunk([]byte) error {
	panic("unimplemented")
}

type unimplementedServiceConns struct{}

func (x unimplementedServiceConns) InitGetObjectRangeStream(context.Context, netmap.NodeInfo, ecdsa.PrivateKey, cid.ID, oid.ID,
	uint64, uint64, *session.Object, []string) (io.ReadCloser, error) {
	panic("unimplemented")
}

func (x unimplementedServiceConns) Head(context.Context, netmap.NodeInfo, ecdsa.PrivateKey, cid.ID, oid.ID, *session.Object) (object.Object, error) {
	panic("unimplemented")
}
