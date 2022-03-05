package internal

import (
	"context"
	"crypto/ecdsa"
	"strconv"

	session2 "github.com/nspcc-dev/neofs-api-go/v2/session"
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

	opts []client.CallOption
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
	x.opts = append(x.opts, client.WithKey(key))
}

// SetSessionToken sets token of the session within which request should be sent.
//
// By default the request will be sent outside the session.
func (x *commonPrm) SetSessionToken(tok *session.Token) {
	x.opts = append(x.opts, client.WithSession(tok))
}

// SetBearerToken sets bearer token to be attached to the request.
//
// By default token is not attached to the request.
func (x *commonPrm) SetBearerToken(tok *token.BearerToken) {
	x.opts = append(x.opts, client.WithBearer(tok))
}

// SetTTL sets time-to-live call option.
func (x *commonPrm) SetTTL(ttl uint32) {
	x.opts = append(x.opts, client.WithTTL(ttl))
}

// SetXHeaders sets request X-Headers.
//
// By default X-Headers will  not be attached to the request.
func (x *commonPrm) SetXHeaders(xhdrs []*session.XHeader) {
	for _, xhdr := range xhdrs {
		x.opts = append(x.opts, client.WithXHeader(xhdr))
	}
}

type readPrmCommon struct {
	commonPrm
}

// SetNetmapEpoch sets the epoch number to be used to locate the object.
//
// By default current epoch on the server will be used.
func (x *readPrmCommon) SetNetmapEpoch(epoch uint64) {
	xNetmapEpoch := session.NewXHeader()
	xNetmapEpoch.SetKey(session2.XHeaderNetmapEpoch)
	xNetmapEpoch.SetValue(strconv.FormatUint(epoch, 10))

	x.opts = append(x.opts, client.WithXHeader(xNetmapEpoch))
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	readPrmCommon

	cliPrm client.GetObjectParams
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *GetObjectPrm) SetRawFlag() {
	x.cliPrm.WithRawFlag(true)
}

// SetAddress sets object address.
//
// Required parameter.
func (x *GetObjectPrm) SetAddress(addr *addressSDK.Address) {
	x.cliPrm.WithAddress(addr)
}

// GetObjectRes groups resulting values of GetObject operation.
type GetObjectRes struct {
	cliRes *client.ObjectGetRes
}

// Object returns requested object.
func (x GetObjectRes) Object() *object.Object {
	return x.cliRes.Object()
}

// GetObject reads the object by address.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
// Returns:
//  error of type *object.SplitInfoError if object if raw flag is set and requested object is virtual;
//  object.ErrAlreadyRemoved error if requested object is marked to be removed.
func GetObject(prm GetObjectPrm) (res GetObjectRes, err error) {
	res.cliRes, err = prm.cli.GetObject(prm.ctx, &prm.cliPrm, prm.opts...)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	// FIXME: object.ErrAlreadyRemoved never returns

	return
}

// HeadObjectPrm groups parameters of HeadObject operation.
type HeadObjectPrm struct {
	readPrmCommon

	cliPrm client.ObjectHeaderParams
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *HeadObjectPrm) SetRawFlag() {
	x.cliPrm.WithRawFlag(true)
}

// SetAddress sets object address.
//
// Required parameter.
func (x *HeadObjectPrm) SetAddress(addr *addressSDK.Address) {
	x.cliPrm.WithAddress(addr)
}

// GetObjectRes groups resulting values of GetObject operation.
type HeadObjectRes struct {
	cliRes *client.ObjectHeadRes
}

// Header returns requested object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.cliRes.Object()
}

// HeadObject reads object header by address.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
// Returns:
//  error of type *object.SplitInfoError if object if raw flag is set and requested object is virtual;
//  object.ErrAlreadyRemoved error if requested object is marked to be removed.
func HeadObject(prm HeadObjectPrm) (res HeadObjectRes, err error) {
	res.cliRes, err = prm.cli.HeadObject(prm.ctx, &prm.cliPrm, prm.opts...)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	// FIXME: object.ErrAlreadyRemoved never returns

	return
}

// PayloadRangePrm groups parameters of PayloadRange operation.
type PayloadRangePrm struct {
	readPrmCommon

	cliPrm client.RangeDataParams
}

// SetRawFlag sets raw flag of the request.
//
// By default request will not be raw.
func (x *PayloadRangePrm) SetRawFlag() {
	x.cliPrm.WithRaw(true)
}

// SetAddress sets object address.
//
// Required parameter.
func (x *PayloadRangePrm) SetAddress(addr *addressSDK.Address) {
	x.cliPrm.WithAddress(addr)
}

// SetRange range of the object payload to be read.
//
// Required parameter.
func (x *PayloadRangePrm) SetRange(rng *object.Range) {
	x.cliPrm.WithRange(rng)
}

// PayloadRangeRes groups resulting values of GetObject operation.
type PayloadRangeRes struct {
	cliRes *client.ObjectRangeRes
}

// PayloadRange returns data of the requested payload range.
func (x PayloadRangeRes) PayloadRange() []byte {
	return x.cliRes.Data()
}

// PayloadRange reads object payload range by address.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
// Returns:
//  error of type *object.SplitInfoError if object if raw flag is set and requested object is virtual;
//  object.ErrAlreadyRemoved error if requested object is marked to be removed.
func PayloadRange(prm PayloadRangePrm) (res PayloadRangeRes, err error) {
	res.cliRes, err = prm.cli.ObjectPayloadRangeData(prm.ctx, &prm.cliPrm, prm.opts...)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	// FIXME: object.ErrAlreadyRemoved never returns

	return
}

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonPrm

	cliPrm client.PutObjectParams
}

// SetObject sets object to be stored.
//
// Required parameter.
func (x *PutObjectPrm) SetObject(obj *object.Object) {
	x.cliPrm.WithObject(obj)
}

// PutObjectRes groups resulting values of PutObject operation.
type PutObjectRes struct {
	cliRes *client.ObjectPutRes
}

// ID returns identifier of the stored object.
func (x PutObjectRes) ID() *oidSDK.ID {
	return x.cliRes.ID()
}

// PutObject saves the object in local storage of the remote node.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
func PutObject(prm PutObjectPrm) (res PutObjectRes, err error) {
	res.cliRes, err = prm.cli.PutObject(prm.ctx, &prm.cliPrm,
		append(prm.opts, client.WithTTL(1))...,
	)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	return
}

// SearchObjectsPrm groups parameters of SearchObjects operation.
type SearchObjectsPrm struct {
	readPrmCommon

	cliPrm client.SearchObjectParams
}

// SetContainerID sets identifier of the container to search the objects.
//
// Required parameter.
func (x *SearchObjectsPrm) SetContainerID(id *cid.ID) {
	x.cliPrm.WithContainerID(id)
}

// SetFilters sets search filters.
func (x *SearchObjectsPrm) SetFilters(fs object.SearchFilters) {
	x.cliPrm.WithSearchFilters(fs)
}

// SearchObjectsRes groups resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	cliRes *client.ObjectSearchRes
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []*oidSDK.ID {
	return x.cliRes.IDList()
}

// SearchObjects selects objects from container which match the filters.
//
// Returns any error prevented the operation from completing correctly in error return.
func SearchObjects(prm SearchObjectsPrm) (res SearchObjectsRes, err error) {
	res.cliRes, err = prm.cli.SearchObjects(prm.ctx, &prm.cliPrm, prm.opts...)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	return
}
