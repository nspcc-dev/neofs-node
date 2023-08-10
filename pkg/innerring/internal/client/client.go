package neofsapiclient

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// Client represents NeoFS API client cut down to the needs of a purely IR application.
type Client struct {
	signer user.Signer

	c clientcore.Client
}

// WrapBasicClient wraps a client.Client instance to use it for NeoFS API RPC.
func (x *Client) WrapBasicClient(c clientcore.Client) {
	x.c = c
}

// SetPrivateKey sets a private key to sign RPC requests.
func (x *Client) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.signer = user.NewAutoIDSigner(*key)
}

// SearchSGPrm groups parameters of SearchSG operation.
type SearchSGPrm struct {
	contextPrm

	cnrID cid.ID
}

// SetContainerID sets the ID of the container to search for storage groups.
func (x *SearchSGPrm) SetContainerID(id cid.ID) {
	x.cnrID = id
}

// SearchSGRes groups the resulting values of SearchSG operation.
type SearchSGRes struct {
	cliRes []oid.ID
}

// IDList returns a list of IDs of storage groups in the container.
func (x SearchSGRes) IDList() []oid.ID {
	return x.cliRes
}

var sgFilter = storagegroup.SearchQuery()

// SearchSG lists objects of storage group type in the container.
//
// Returns any error which prevented the operation from completing correctly in error return.
func (x Client) SearchSG(prm SearchSGPrm) (*SearchSGRes, error) {
	var cliPrm client.PrmObjectSearch
	cliPrm.SetFilters(sgFilter)

	rdr, err := x.c.ObjectSearchInit(prm.ctx, prm.cnrID, x.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object search: %w", err)
	}

	var list []oid.ID

	err = rdr.Iterate(func(id oid.ID) bool {
		list = append(list, id)
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("search objects using NeoFS API: %w", err)
	}

	return &SearchSGRes{
		cliRes: list,
	}, nil
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	getObjectPrm
}

// GetObjectRes groups the resulting values of GetObject operation.
type GetObjectRes struct {
	obj *object.Object
}

// Object returns the received object.
func (x GetObjectRes) Object() *object.Object {
	return x.obj
}

// GetObject reads the object by address.
//
// Returns any error which prevented the operation from completing correctly in error return.
func (x Client) GetObject(prm GetObjectPrm) (*GetObjectRes, error) {
	var cliPrm client.PrmObjectGet

	obj, rdr, err := x.c.ObjectGetInit(prm.ctx, prm.objAddr.Container(), prm.objAddr.Object(), x.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object search: %w", err)
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
	getObjectPrm

	raw bool

	local bool
}

// SetRawFlag sets flag of raw request.
func (x *HeadObjectPrm) SetRawFlag() {
	x.raw = true
}

// SetTTL sets request TTL value.
func (x *HeadObjectPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// HeadObjectRes groups the resulting values of HeadObject operation.
type HeadObjectRes struct {
	hdr *object.Object
}

// Header returns the received object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.hdr
}

// HeadObject reads short object header by address.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw requests, returns *object.SplitInfoError error if the requested object is virtual.
func (x Client) HeadObject(prm HeadObjectPrm) (*HeadObjectRes, error) {
	var cliPrm client.PrmObjectHead

	if prm.raw {
		cliPrm.MarkRaw()
	}

	if prm.local {
		cliPrm.MarkLocal()
	}

	hdr, err := x.c.ObjectHead(prm.ctx, prm.objAddr.Container(), prm.objAddr.Object(), x.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read object header from NeoFS: %w", err)
	}

	return &HeadObjectRes{
		hdr: hdr,
	}, nil
}

// GetObjectPayload reads an object by address from NeoFS via Client and returns its payload.
//
// Returns any error which prevented the operation from completing correctly in error return.
func GetObjectPayload(ctx context.Context, c Client, addr oid.Address) ([]byte, error) {
	var prm GetObjectPrm

	prm.SetContext(ctx)
	prm.SetAddress(addr)

	obj, err := c.GetObject(prm)
	if err != nil {
		return nil, err
	}

	return obj.Object().Payload(), nil
}

func headObject(ctx context.Context, c Client, addr oid.Address, raw bool, ttl uint32) (*object.Object, error) {
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

// GetRawObjectHeaderLocally reads the raw short object header from the server's local storage by address via Client.
func GetRawObjectHeaderLocally(ctx context.Context, c Client, addr oid.Address) (*object.Object, error) {
	return headObject(ctx, c, addr, true, 1)
}

// GetObjectHeaderFromContainer reads the short object header by address via Client with TTL = 10
// for deep traversal of the container.
func GetObjectHeaderFromContainer(ctx context.Context, c Client, addr oid.Address) (*object.Object, error) {
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

// HashPayloadRangeRes groups the resulting values of HashPayloadRange operation.
type HashPayloadRangeRes struct {
	h []byte
}

// Hash returns the hash of the object payload range.
func (x HashPayloadRangeRes) Hash() []byte {
	return x.h
}

// HashPayloadRange requests to calculate Tillich-Zemor hash of the payload range of the object
// from the remote server's local storage.
//
// Returns any error which prevented the operation from completing correctly in error return.
func (x Client) HashPayloadRange(prm HashPayloadRangePrm) (res HashPayloadRangeRes, err error) {
	var cliPrm client.PrmObjectHash
	cliPrm.SetRangeList(prm.rng.GetOffset(), prm.rng.GetLength())
	cliPrm.TillichZemorAlgo()

	hs, err := x.c.ObjectHash(prm.ctx, prm.objAddr.Container(), prm.objAddr.Object(), x.signer, cliPrm)
	if err == nil {
		if ln := len(hs); ln != 1 {
			err = fmt.Errorf("wrong number of checksums %d", ln)
		} else {
			res.h = hs[0]
		}
	}

	return
}

// HashObjectRange reads Tillich-Zemor hash of the object payload range by address
// from the remote server's local storage via Client.
//
// Returns any error which prevented the operation from completing correctly in error return.
func HashObjectRange(ctx context.Context, c Client, addr oid.Address, rng *object.Range) ([]byte, error) {
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
