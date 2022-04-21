package internal

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/token"
)

type commonPrm struct {
	cli coreclient.Client

	ctx context.Context

	key *ecdsa.PrivateKey

	tokenSession *session.Token

	tokenBearer *token.BearerToken

	local bool

	xHeaders []*session.XHeader
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
	x.key = key
}

// SetSessionToken sets token of the session within which request should be sent.
//
// By default the request will be sent outside the session.
func (x *commonPrm) SetSessionToken(tok *session.Token) {
	x.tokenSession = tok
}

// SetBearerToken sets bearer token to be attached to the request.
//
// By default token is not attached to the request.
func (x *commonPrm) SetBearerToken(tok *token.BearerToken) {
	x.tokenBearer = tok
}

// SetTTL sets time-to-live call option.
func (x *commonPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// SetXHeaders sets request X-Headers.
//
// By default X-Headers will  not be attached to the request.
func (x *commonPrm) SetXHeaders(hs []*session.XHeader) {
	x.xHeaders = hs
}

func (x commonPrm) xHeadersPrm() (res []string) {
	if len(x.xHeaders) > 0 {
		res = make([]string, len(x.xHeaders)*2)

		for i := range x.xHeaders {
			res[2*i] = x.xHeaders[i].Key()
			res[2*i+1] = x.xHeaders[i].Value()
		}
	}

	return
}

type readPrmCommon struct {
	commonPrm
}

// SetNetmapEpoch sets the epoch number to be used to locate the object.
//
// By default current epoch on the server will be used.
func (x *readPrmCommon) SetNetmapEpoch(_ uint64) {
	// FIXME: (neofs-node#1194) not supported by client
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	readPrmCommon

	cliPrm client.PrmObjectGet
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
func (x *GetObjectPrm) SetAddress(addr *addressSDK.Address) {
	if id := addr.ContainerID(); id != nil {
		x.cliPrm.FromContainer(*id)
	}

	if id := addr.ObjectID(); id != nil {
		x.cliPrm.ByID(*id)
	}
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
//  error of type *object.SplitInfoError if object raw flag is set and requested object is virtual;
//  error of type *apistatus.ObjectAlreadyRemoved if the requested object is marked to be removed.
func GetObject(prm GetObjectPrm) (*GetObjectRes, error) {
	if prm.tokenSession != nil {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	prm.cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectGetInit(prm.ctx, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object reading: %w", err)
	}

	if prm.key != nil {
		rdr.UseKey(*prm.key)
	}

	var obj object.Object

	if !rdr.ReadHeader(&obj) {
		res, err := rdr.Close()
		if err == nil {
			// pull out an error from status
			err = apistatus.ErrFromStatus(res.Status())
		}

		return nil, fmt.Errorf("read object header: %w", err)
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
func (x *HeadObjectPrm) SetAddress(addr *addressSDK.Address) {
	if id := addr.ContainerID(); id != nil {
		x.cliPrm.FromContainer(*id)
	}

	if id := addr.ObjectID(); id != nil {
		x.cliPrm.ByID(*id)
	}
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
//  error of type *object.SplitInfoError if object raw flag is set and requested object is virtual;
//  error of type *apistatus.ObjectAlreadyRemoved if the requested object is marked to be removed.
func HeadObject(prm HeadObjectPrm) (*HeadObjectRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	if prm.tokenSession != nil {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	cliRes, err := prm.cli.ObjectHead(prm.ctx, prm.cliPrm)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(cliRes.Status())
	}

	if err != nil {
		return nil, fmt.Errorf("read object header from NeoFS: %w", err)
	}

	var hdr object.Object

	if !cliRes.ReadHeader(&hdr) {
		return nil, errors.New("missing object header in the response")
	}

	return &HeadObjectRes{
		hdr: &hdr,
	}, nil
}

// PayloadRangePrm groups parameters of PayloadRange operation.
type PayloadRangePrm struct {
	readPrmCommon

	ln uint64

	cliPrm client.PrmObjectRange
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
func (x *PayloadRangePrm) SetAddress(addr *addressSDK.Address) {
	if id := addr.ContainerID(); id != nil {
		x.cliPrm.FromContainer(*id)
	}

	if id := addr.ObjectID(); id != nil {
		x.cliPrm.ByID(*id)
	}
}

// SetRange range of the object payload to be read.
//
// Required parameter.
func (x *PayloadRangePrm) SetRange(rng *object.Range) {
	x.cliPrm.SetOffset(rng.GetOffset())
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

// PayloadRange reads object payload range by address.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns:
//  error of type *object.SplitInfoError if object raw flag is set and requested object is virtual;
//  error of type *apistatus.ObjectAlreadyRemoved if the requested object is marked to be removed.
func PayloadRange(prm PayloadRangePrm) (*PayloadRangeRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	if prm.tokenSession != nil {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.SetLength(prm.ln)
	prm.cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectRangeInit(prm.ctx, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init payload reading: %w", err)
	}

	data := make([]byte, prm.ln)

	_, err = io.ReadFull(rdr, data)
	if err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	return &PayloadRangeRes{
		data: data,
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
	id *oidSDK.ID
}

// ID returns identifier of the stored object.
func (x PutObjectRes) ID() *oidSDK.ID {
	return x.id
}

// PutObject saves the object in local storage of the remote node.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(prm PutObjectPrm) (*PutObjectRes, error) {
	var prmCli client.PrmObjectPutInit

	w, err := prm.cli.ObjectPutInit(prm.ctx, prmCli)
	if err != nil {
		return nil, fmt.Errorf("init object writing on client: %w", err)
	}

	w.MarkLocal()

	if prm.key != nil {
		w.UseKey(*prm.key)
	}

	if prm.tokenSession != nil {
		w.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		w.WithBearerToken(*prm.tokenBearer)
	}

	w.WithXHeaders(prm.xHeadersPrm()...)

	if w.WriteHeader(*prm.obj) {
		w.WritePayloadChunk(prm.obj.Payload())
	}

	res, err := w.Close()
	if err == nil {
		err = apistatus.ErrFromStatus(res.Status())
	}

	if err != nil {
		return nil, fmt.Errorf("write object via client: %w", err)
	}

	var id oidSDK.ID
	if !res.ReadStoredObjectID(&id) {
		return nil, errors.New("missing identifier in the response")
	}

	return &PutObjectRes{
		id: &id,
	}, nil
}

// SearchObjectsPrm groups parameters of SearchObjects operation.
type SearchObjectsPrm struct {
	readPrmCommon

	cliPrm client.PrmObjectSearch
}

// SetContainerID sets identifier of the container to search the objects.
//
// Required parameter.
func (x *SearchObjectsPrm) SetContainerID(id *cid.ID) {
	if id != nil {
		x.cliPrm.InContainer(*id)
	}
}

// SetFilters sets search filters.
func (x *SearchObjectsPrm) SetFilters(fs object.SearchFilters) {
	x.cliPrm.SetFilters(fs)
}

// SearchObjectsRes groups the resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	ids []oidSDK.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []oidSDK.ID {
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

	prm.cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectSearchInit(prm.ctx, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object searching in client: %w", err)
	}

	if prm.key != nil {
		rdr.UseKey(*prm.key)
	}

	buf := make([]oidSDK.ID, 10)
	var ids []oidSDK.ID
	var n int
	var ok bool

	for {
		n, ok = rdr.Read(buf)
		if n > 0 {
			for i := range buf[:n] {
				v := buf[i]
				ids = append(ids, v)
			}
		}

		if !ok {
			break
		}
	}

	res, err := rdr.Close()
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.Status())
	}

	if err != nil {
		return nil, fmt.Errorf("read object list: %w", err)
	}

	return &SearchObjectsRes{
		ids: ids,
	}, nil
}
