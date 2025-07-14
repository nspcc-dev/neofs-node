package internal

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type commonPrm struct {
	cli coreclient.Client

	ctx context.Context

	signer user.Signer

	tokenSession *session.Object

	tokenBearer *bearer.Token

	local bool

	xHeaders []string
}

// SetClient sets base client for NeoFS API communication.
//
// Required parameter.
func (x *commonPrm) SetClient(cli coreclient.Client) {
	x.cli = cli
}

// SetContext sets context.Context for network communication.
//
// Required parameter.
func (x *commonPrm) SetContext(ctx context.Context) {
	x.ctx = ctx
}

// SetPrivateKey sets private key to sign the request(s).
//
// Required parameter.
func (x *commonPrm) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.signer = user.NewAutoIDSigner(*key)
}

// SetSessionToken sets token of the session within which request should be sent.
//
// By default the request will be sent outside the session.
func (x *commonPrm) SetSessionToken(tok *session.Object) {
	x.tokenSession = tok
}

// SetBearerToken sets bearer token to be attached to the request.
//
// By default token is not attached to the request.
func (x *commonPrm) SetBearerToken(tok *bearer.Token) {
	x.tokenBearer = tok
}

// SetTTL sets time-to-live call option.
func (x *commonPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// SetXHeaders sets request X-Headers.
//
// By default X-Headers will  not be attached to the request.
func (x *commonPrm) SetXHeaders(hs []string) {
	x.xHeaders = hs
}

type readPrmCommon struct {
	commonPrm
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	readPrmCommon

	cliPrm client.PrmObjectGet

	obj oid.ID
	cnr cid.ID
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *GetObjectPrm) SetRawFlag() {
	x.cliPrm.MarkRaw()
}

// SetAddress sets object address.
//
// Required parameter.
func (x *GetObjectPrm) SetAddress(addr oid.Address) {
	x.obj = addr.Object()
	x.cnr = addr.Container()
}

// GetObjectRes groups the resulting values of GetObject operation.
type GetObjectRes struct {
	obj *object.Object
}

// Object returns requested object.
func (x GetObjectRes) Object() *object.Object {
	return x.obj
}

// GetObject reads the object by address.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns:
//   - error of type *object.SplitInfoError if object raw flag is set and requested object is virtual
//   - [apistatus.ErrObjectAlreadyRemoved] error if the requested object is marked to be removed
//
// GetObject ignores the provided session if it is not related to the requested object.
func GetObject(prm GetObjectPrm) (*GetObjectRes, error) {
	// here we ignore session if it is opened for other object since such
	// request will almost definitely fail. The case can occur, for example,
	// when session is bound to the parent object and child object is requested.
	if prm.tokenSession != nil && prm.tokenSession.AssertObject(prm.obj) {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	prm.cliPrm.WithXHeaders(prm.xHeaders...)

	obj, rdr, err := prm.cli.ObjectGetInit(prm.ctx, prm.cnr, prm.obj, prm.signer, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object reading: %w", err)
	}

	buf := make([]byte, obj.PayloadSize())

	_, err = rdr.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	obj.SetPayload(buf)

	return &GetObjectRes{
		obj: &obj,
	}, nil
}

// HeadObjectPrm groups parameters of HeadObject operation.
type HeadObjectPrm struct {
	readPrmCommon

	cliPrm client.PrmObjectHead

	obj oid.ID
	cnr cid.ID
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *HeadObjectPrm) SetRawFlag() {
	x.cliPrm.MarkRaw()
}

// SetAddress sets object address.
//
// Required parameter.
func (x *HeadObjectPrm) SetAddress(addr oid.Address) {
	x.obj = addr.Object()
	x.cnr = addr.Container()
}

// HeadObjectRes groups the resulting values of GetObject operation.
type HeadObjectRes struct {
	hdr *object.Object
}

// Header returns requested object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.hdr
}

// HeadObject reads object header by address.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns:
//   - error of type *object.SplitInfoError if object raw flag is set and requested object is virtual
//   - [apistatus.ErrObjectAlreadyRemoved] error if the requested object is marked to be removed
//   - [apistatus.ErrNodeUnderMaintenance] error if remote node is currently under maintenance
//
// HeadObject ignores the provided session if it is not related to the requested object.
func HeadObject(prm HeadObjectPrm) (*HeadObjectRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	// see details in same statement of GetObject
	if prm.tokenSession != nil && prm.tokenSession.AssertObject(prm.obj) {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.WithXHeaders(prm.xHeaders...)

	hdr, err := prm.cli.ObjectHead(prm.ctx, prm.cnr, prm.obj, prm.signer, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read object header from NeoFS: %w", err)
	}

	return &HeadObjectRes{
		hdr: hdr,
	}, nil
}

// PayloadRangePrm groups parameters of PayloadRange operation.
type PayloadRangePrm struct {
	readPrmCommon

	offset, ln uint64

	cliPrm client.PrmObjectRange

	obj oid.ID
	cnr cid.ID
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *PayloadRangePrm) SetRawFlag() {
	x.cliPrm.MarkRaw()
}

// SetAddress sets object address.
//
// Required parameter.
func (x *PayloadRangePrm) SetAddress(addr oid.Address) {
	x.obj = addr.Object()
	x.cnr = addr.Container()
}

// SetRange range of the object payload to be read.
//
// Required parameter.
func (x *PayloadRangePrm) SetRange(rng *object.Range) {
	x.offset = rng.GetOffset()
	x.ln = rng.GetLength()
}

// PayloadRangeRes groups the resulting values of GetObject operation.
type PayloadRangeRes struct {
	data []byte
}

// PayloadRange returns data of the requested payload range.
func (x PayloadRangeRes) PayloadRange() []byte {
	return x.data
}

// maxInitialBufferSize is the maximum initial buffer size for PayloadRange result.
// We don't want to allocate a lot of space in advance because a query can
// fail with apistatus.ObjectOutOfRange status.
const maxInitialBufferSize = 1024 * 1024 // 1 MiB

// PayloadRange reads object payload range by address.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns:
//   - error of type *object.SplitInfoError if object raw flag is set and requested object is virtual
//   - [apistatus.ErrObjectAlreadyRemoved] error if the requested object is marked to be removed
//   - [apistatus.ErrObjectOutOfRange] error if the requested range is too big
//   - [apistatus.ErrObjectAccessDenied] error if access to the requested object is denied
//
// PayloadRange ignores the provided session if it is not related to the requested object.
func PayloadRange(prm PayloadRangePrm) (*PayloadRangeRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	// see details in same statement of GetObject
	if prm.tokenSession != nil && prm.tokenSession.AssertObject(prm.obj) {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectRangeInit(prm.ctx, prm.cnr, prm.obj, prm.offset, prm.ln, prm.signer, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init payload reading: %w", err)
	}

	if int64(prm.ln) < 0 {
		// `CopyN` expects `int64`, this check ensures that the result is positive.
		// On practice this means that we can return incorrect results for objects
		// with size > 8_388 Petabytes, this will be fixed later with support for streaming.
		return nil, new(apistatus.ObjectOutOfRange)
	}

	ln := min(prm.ln, maxInitialBufferSize)

	w := bytes.NewBuffer(make([]byte, ln))
	_, err = io.CopyN(w, rdr, int64(prm.ln))
	if err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	return &PayloadRangeRes{
		data: w.Bytes(),
	}, nil
}

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonPrm

	obj *object.Object
}

// SetObject sets object to be stored.
//
// Required parameter.
func (x *PutObjectPrm) SetObject(obj *object.Object) {
	x.obj = obj
}

// PutObjectRes groups the resulting values of PutObject operation.
type PutObjectRes struct {
	id oid.ID
}

// ID returns identifier of the stored object.
func (x PutObjectRes) ID() oid.ID {
	return x.id
}

// PutObject saves the object in local storage of the remote node.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(prm PutObjectPrm) (*PutObjectRes, error) {
	var prmCli client.PrmObjectPutInit

	prmCli.MarkLocal()

	if prm.tokenSession != nil {
		prmCli.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prmCli.WithBearerToken(*prm.tokenBearer)
	}

	prmCli.WithXHeaders(prm.xHeaders...)

	w, err := prm.cli.ObjectPutInit(prm.ctx, *prm.obj, prm.signer, prmCli)
	if err != nil {
		return nil, fmt.Errorf("init object writing on client: %w", err)
	}

	_, err = w.Write(prm.obj.Payload())
	if err != nil {
		return nil, fmt.Errorf("write object payload into stream: %w", err)
	}

	err = w.Close()
	if err != nil {
		ReportError(prm.cli, err)
		return nil, fmt.Errorf("finish object stream: %w", err)
	}

	return &PutObjectRes{
		id: w.GetResult().StoredObjectID(),
	}, nil
}

// SearchObjectsPrm groups parameters of SearchObjects operation.
type SearchObjectsPrm struct {
	readPrmCommon

	cid    cid.ID
	cliPrm client.PrmObjectSearch
}

// SetContainerID sets identifier of the container to search the objects.
//
// Required parameter.
func (x *SearchObjectsPrm) SetContainerID(id cid.ID) {
	x.cid = id
}

// SetFilters sets search filters.
func (x *SearchObjectsPrm) SetFilters(fs object.SearchFilters) {
	x.cliPrm.SetFilters(fs)
}

// SearchObjectsRes groups the resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	ids []oid.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []oid.ID {
	return x.ids
}

// SearchObjects selects objects from container which match the filters.
//
// Returns any error which prevented the operation from completing correctly in error return.
func SearchObjects(prm SearchObjectsPrm) (*SearchObjectsRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	if prm.tokenSession != nil {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectSearchInit(prm.ctx, prm.cid, prm.signer, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object searching in client: %w", err)
	}

	var ids []oid.ID

	err = rdr.Iterate(func(id oid.ID) bool {
		ids = append(ids, id)
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("search objects using NeoFS API: %w", err)
	}

	return &SearchObjectsRes{
		ids: ids,
	}, nil
}
