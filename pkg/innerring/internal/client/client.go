package neofsapiclient

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// Client represents NeoFS API client cut down to the needs of a purely IR application.
type Client struct {
	key *ecdsa.PrivateKey

	c client.Client
}

// WrapBasicClient wraps client.Client instance to use it for NeoFS API RPC.
func (x *Client) WrapBasicClient(c client.Client) {
	x.c = c
}

// SetPrivateKey sets private key to sign RPC requests.
func (x *Client) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.key = key
}

// SearchSGPrm groups parameters of SearchSG operation.
type SearchSGPrm struct {
	contextPrm

	cnrID *cid.ID
}

// SetContainerID sets ID of the container to search for storage groups.
func (x *SearchSGPrm) SetContainerID(id *cid.ID) {
	x.cnrID = id
}

// SearchSGRes groups resulting values of SearchSG operation.
type SearchSGRes struct {
	cliRes *client.ObjectSearchRes
}

// IDList returns list of IDs of storage groups in container.
func (x SearchSGRes) IDList() []*object.ID {
	return x.cliRes.IDList()
}

var sgFilter = storagegroup.SearchQuery()

// SearchSG lists objects of storage group type in the container.
//
// Returns any error prevented the operation from completing correctly in error return.
func (x Client) SearchSG(prm SearchSGPrm) (res SearchSGRes, err error) {
	var cliPrm client.SearchObjectParams

	cliPrm.WithContainerID(prm.cnrID)
	cliPrm.WithSearchFilters(sgFilter)

	res.cliRes, err = x.c.SearchObjects(prm.ctx, &cliPrm,
		client.WithKey(x.key),
	)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	return
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	getObjectPrm
}

// GetObjectRes groups resulting values of GetObject operation.
type GetObjectRes struct {
	cliRes *client.ObjectGetRes
}

// Object returns received object.
func (x GetObjectRes) Object() *object.Object {
	return x.cliRes.Object()
}

// GetObject reads the object by address.
//
// Returns any error prevented the operation from completing correctly in error return.
func (x Client) GetObject(prm GetObjectPrm) (res GetObjectRes, err error) {
	var cliPrm client.GetObjectParams

	cliPrm.WithAddress(prm.objAddr)

	res.cliRes, err = x.c.GetObject(prm.ctx, &cliPrm,
		client.WithKey(x.key),
	)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	return
}

// HeadObjectPrm groups parameters of HeadObject operation.
type HeadObjectPrm struct {
	getObjectPrm

	raw bool

	ttl uint32
}

// SetRawFlag sets flag of raw request.
func (x *HeadObjectPrm) SetRawFlag() {
	x.raw = true
}

// SetTTL sets request TTL value.
func (x *HeadObjectPrm) SetTTL(ttl uint32) {
	x.ttl = ttl
}

// HeadObjectRes groups resulting values of HeadObject operation.
type HeadObjectRes struct {
	cliRes *client.ObjectHeadRes
}

// Header returns received object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.cliRes.Object()
}

// HeadObject reads short object header by address.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw requests, returns *object.SplitInfoError error if requested object is virtual.
func (x Client) HeadObject(prm HeadObjectPrm) (res HeadObjectRes, err error) {
	var cliPrm client.ObjectHeaderParams

	cliPrm.WithAddress(prm.objAddr)
	cliPrm.WithRawFlag(prm.raw)
	cliPrm.WithMainFields()

	res.cliRes, err = x.c.HeadObject(prm.ctx, &cliPrm,
		client.WithKey(x.key),
		client.WithTTL(prm.ttl),
	)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(res.cliRes.Status())
	}

	return
}

// GetObjectPayload reads object by address from NeoFS via Client and returns its payload.
//
// Returns any error prevented the operation from completing correctly in error return.
func GetObjectPayload(ctx context.Context, c Client, addr *object.Address) ([]byte, error) {
	var prm GetObjectPrm

	prm.SetContext(ctx)
	prm.SetAddress(addr)

	obj, err := c.GetObject(prm)
	if err != nil {
		return nil, err
	}

	return obj.Object().Payload(), nil
}

func headObject(ctx context.Context, c Client, addr *object.Address, raw bool, ttl uint32) (*object.Object, error) {
	var prm HeadObjectPrm

	prm.SetContext(ctx)
	prm.SetAddress(addr)
	prm.SetTTL(ttl)

	if raw {
		prm.SetRawFlag()
	}

	obj, err := c.HeadObject(prm)
	if err != nil {
		return nil, err
	}

	return obj.Header(), nil
}

// GetRawObjectHeaderLocally reads raw short object header from server's local storage by address via Client.
func GetRawObjectHeaderLocally(ctx context.Context, c Client, addr *object.Address) (*object.Object, error) {
	return headObject(ctx, c, addr, true, 1)
}

// GetObjectHeaderFromContainer reads short object header by address via Client with TTL = 10
// for deep traversal of the container.
func GetObjectHeaderFromContainer(ctx context.Context, c Client, addr *object.Address) (*object.Object, error) {
	return headObject(ctx, c, addr, false, 10)
}

// HashPayloadRangePrm groups parameters of HashPayloadRange operation.
type HashPayloadRangePrm struct {
	getObjectPrm

	rng *object.Range
}

// SetRange sets payload range to calculate the hash.
func (x *HashPayloadRangePrm) SetRange(rng *object.Range) {
	x.rng = rng
}

// HashPayloadRangeRes groups resulting values of HashPayloadRange operation.
type HashPayloadRangeRes struct {
	h []byte
}

// Hash returns hash of the object payload range.
func (x HashPayloadRangeRes) Hash() []byte {
	return x.h
}

// HashObjectRange requests to calculate Tillich-Zemor hash of the payload range of the object
// from the remote server's local storage.
//
// Returns any error prevented the operation from completing correctly in error return.
func (x Client) HashPayloadRange(prm HashPayloadRangePrm) (res HashPayloadRangeRes, err error) {
	var cliPrm client.RangeChecksumParams

	cliPrm.WithAddress(prm.objAddr)
	cliPrm.WithRangeList(prm.rng)

	cliRes, err := x.c.HashObjectPayloadRanges(prm.ctx, &cliPrm,
		client.WithKey(x.key),
		client.WithTTL(1),
	)
	if err == nil {
		// pull out an error from status
		err = apistatus.ErrFromStatus(cliRes.Status())
		if err != nil {
			return
		}

		hs := cliRes.Hashes()
		if ln := len(hs); ln != 1 {
			err = fmt.Errorf("wrong number of hashes %d", ln)
		} else {
			res.h = hs[0]
		}
	}

	return
}

// HashObjectRange reads Tillich-Zemor hash of the object payload range by address
// from the remote server's local storage via Client.
//
// Returns any error prevented the operation from completing correctly in error return.
func HashObjectRange(ctx context.Context, c Client, addr *object.Address, rng *object.Range) ([]byte, error) {
	var prm HashPayloadRangePrm

	prm.SetContext(ctx)
	prm.SetAddress(addr)
	prm.SetRange(rng)

	res, err := c.HashPayloadRange(prm)
	if err != nil {
		return nil, err
	}

	return res.Hash(), nil
}
