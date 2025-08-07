package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// NodeInfoPrm groups parameters of NodeInfo operation.
type NodeInfoPrm struct {
	commonPrm
	client.PrmEndpointInfo
}

// NodeInfoRes groups the resulting values of NodeInfo operation.
type NodeInfoRes struct {
	cliRes *client.ResEndpointInfo
}

// NodeInfo returns information about the node from netmap.
func (x NodeInfoRes) NodeInfo() netmap.NodeInfo {
	return x.cliRes.NodeInfo()
}

// LatestVersion returns the latest NeoFS API version in use.
func (x NodeInfoRes) LatestVersion() version.Version {
	return x.cliRes.LatestVersion()
}

// NodeInfo requests information about the remote server from NeoFS netmap.
//
// Returns any error which prevented the operation from completing correctly in error return.
func NodeInfo(ctx context.Context, prm NodeInfoPrm) (res NodeInfoRes, err error) {
	res.cliRes, err = prm.cli.EndpointInfo(ctx, prm.PrmEndpointInfo)

	return
}

// NetMapSnapshotPrm groups parameters of NetMapSnapshot operation.
type NetMapSnapshotPrm struct {
	commonPrm
}

// NetMapSnapshotRes groups the resulting values of NetMapSnapshot operation.
type NetMapSnapshotRes struct {
	cliRes netmap.NetMap
}

// NetMap returns current local snapshot of the NeoFS network map.
func (x NetMapSnapshotRes) NetMap() netmap.NetMap {
	return x.cliRes
}

// NetMapSnapshot requests current network view of the remote server.
//
// Returns any error which prevented the operation from completing correctly in error return.
func NetMapSnapshot(ctx context.Context, prm NetMapSnapshotPrm) (res NetMapSnapshotRes, err error) {
	res.cliRes, err = prm.cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
	return
}

// CreateSessionPrm groups parameters of CreateSession operation.
type CreateSessionPrm struct {
	commonPrm
	signerPrm
	client.PrmSessionCreate
}

// CreateSessionRes groups the resulting values of CreateSession operation.
type CreateSessionRes struct {
	cliRes *client.ResSessionCreate
}

// ID returns session identifier.
func (x CreateSessionRes) ID() []byte {
	return x.cliRes.ID()
}

// SessionKey returns public session key in a binary format.
func (x CreateSessionRes) SessionKey() []byte {
	return x.cliRes.PublicKey()
}

// CreateSession opens a new unlimited session with the remote node.
//
// Returns any error which prevented the operation from completing correctly in error return.
func CreateSession(ctx context.Context, prm CreateSessionPrm) (res CreateSessionRes, err error) {
	res.cliRes, err = prm.cli.SessionCreate(ctx, prm.signer, prm.PrmSessionCreate)

	return
}

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonObjectPrm

	hdr *object.Object

	rdr io.Reader

	headerCallback func(*object.Object)
}

// SetHeader sets object header.
func (x *PutObjectPrm) SetHeader(hdr *object.Object) {
	x.hdr = hdr
}

// SetPayloadReader sets reader of the object payload.
func (x *PutObjectPrm) SetPayloadReader(rdr io.Reader) {
	x.rdr = rdr
}

// SetHeaderCallback sets callback which is called on the object after the header is received
// but before the payload is written.
func (x *PutObjectPrm) SetHeaderCallback(f func(*object.Object)) {
	x.headerCallback = f
}

// PutObjectRes groups the resulting values of PutObject operation.
type PutObjectRes struct {
	id oid.ID
}

// ID returns identifier of the created object.
func (x PutObjectRes) ID() oid.ID {
	return x.id
}

// PutObject saves the object in NeoFS network.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(ctx context.Context, prm PutObjectPrm) (*PutObjectRes, error) {
	var putPrm client.PrmObjectPutInit

	if prm.sessionToken != nil {
		putPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		putPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.local {
		putPrm.MarkLocal()
	}

	putPrm.WithXHeaders(prm.xHeaders...)

	wrt, err := prm.cli.ObjectPutInit(ctx, *prm.hdr, prm.signer, putPrm)
	if err != nil {
		return nil, fmt.Errorf("init object writing: %w", err)
	}

	if prm.headerCallback != nil {
		prm.headerCallback(prm.hdr)
	}

	sz := prm.hdr.PayloadSize()

	if data := prm.hdr.Payload(); len(data) > 0 {
		if prm.rdr != nil {
			prm.rdr = io.MultiReader(bytes.NewReader(data), prm.rdr)
		} else {
			prm.rdr = bytes.NewReader(data)
			sz = uint64(len(data))
		}
	}

	if prm.rdr != nil {
		const defaultBufferSizePut = 3 << 20 // Maximum chunk size is 3 MiB in the SDK.

		if sz == 0 || sz > defaultBufferSizePut {
			sz = defaultBufferSizePut
		}

		buf := make([]byte, sz)

		_, err = io.CopyBuffer(wrt, prm.rdr, buf)
		if err != nil {
			return nil, fmt.Errorf("copy data into object stream: %w", err)
		}
	}

	err = wrt.Close()
	if err != nil {
		return nil, fmt.Errorf("finish object stream: %w", err)
	}

	return &PutObjectRes{
		id: wrt.GetResult().StoredObjectID(),
	}, nil
}

// DeleteObjectPrm groups parameters of DeleteObject operation.
type DeleteObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
}

// DeleteObjectRes groups the resulting values of DeleteObject operation.
type DeleteObjectRes struct {
	tomb oid.ID
}

// Tombstone returns the ID of the created object with tombstone.
func (x DeleteObjectRes) Tombstone() oid.ID {
	return x.tomb
}

// DeleteObject marks an object to be removed from NeoFS through tombstone placement.
//
// Returns any error which prevented the operation from completing correctly in error return.
func DeleteObject(ctx context.Context, prm DeleteObjectPrm) (*DeleteObjectRes, error) {
	var delPrm client.PrmObjectDelete

	if prm.sessionToken != nil {
		delPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		delPrm.WithBearerToken(*prm.bearerToken)
	}

	delPrm.WithXHeaders(prm.xHeaders...)

	cliRes, err := prm.cli.ObjectDelete(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, delPrm)
	if err != nil {
		return nil, fmt.Errorf("remove object via client: %w", err)
	}

	return &DeleteObjectRes{
		tomb: cliRes,
	}, nil
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm
	payloadWriterPrm
	headerCallback func(*object.Object)
}

// SetHeaderCallback sets callback which is called on the object after the header is received
// but before the payload is written.
func (p *GetObjectPrm) SetHeaderCallback(f func(*object.Object)) {
	p.headerCallback = f
}

// GetObjectRes groups the resulting values of GetObject operation.
type GetObjectRes struct {
	hdr *object.Object
}

// Header returns the header of the request object.
func (x GetObjectRes) Header() *object.Object {
	return x.hdr
}

// GetObject reads an object by address.
//
// Interrupts on any writer error. If successful, payload is written to the writer.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func GetObject(ctx context.Context, prm GetObjectPrm) (*GetObjectRes, error) {
	var getPrm client.PrmObjectGet

	if prm.sessionToken != nil {
		getPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		getPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.raw {
		getPrm.MarkRaw()
	}

	if prm.local {
		getPrm.MarkLocal()
	}

	getPrm.WithXHeaders(prm.xHeaders...)

	hdr, rdr, err := prm.cli.ObjectGetInit(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, getPrm)
	if err != nil {
		return nil, fmt.Errorf("init object reading on client: %w", err)
	}

	if prm.headerCallback != nil {
		prm.headerCallback(&hdr)
	}

	_, err = io.Copy(prm.wrt, rdr)
	if err != nil {
		return nil, fmt.Errorf("copy payload: %w", err)
	}

	return &GetObjectRes{
		hdr: &hdr,
	}, nil
}

// HeadObjectPrm groups parameters of HeadObject operation.
type HeadObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm

	mainOnly bool
}

// SetMainOnlyFlag sets flag to get only main fields of an object header in terms of NeoFS API.
func (x *HeadObjectPrm) SetMainOnlyFlag(v bool) {
	x.mainOnly = v
}

// HeadObjectRes groups the resulting values of HeadObject operation.
type HeadObjectRes struct {
	hdr *object.Object
}

// Header returns the requested object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.hdr
}

// HeadObject reads an object header by address.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func HeadObject(ctx context.Context, prm HeadObjectPrm) (*HeadObjectRes, error) {
	var cliPrm client.PrmObjectHead

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.raw {
		cliPrm.MarkRaw()
	}

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	hdr, err := prm.cli.ObjectHead(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read object header via client: %w", err)
	}

	return &HeadObjectRes{
		hdr: hdr,
	}, nil
}

// SearchObjectsPrm groups parameters of SearchObjects operation.
type SearchObjectsPrm struct {
	commonObjectPrm
	containerIDPrm

	filters object.SearchFilters
}

// SetFilters sets search filters.
func (x *SearchObjectsPrm) SetFilters(filters object.SearchFilters) {
	x.filters = filters
}

// SearchObjectsRes groups the resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	ids []oid.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []oid.ID {
	return x.ids
}

// SearchObjects selects objects from the container which match the filters.
//
// Returns any error which prevented the operation from completing correctly in error return.
func SearchObjects(ctx context.Context, prm SearchObjectsPrm) (*SearchObjectsRes, error) {
	var cliPrm client.PrmObjectSearch
	cliPrm.SetFilters(prm.filters)

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectSearchInit(ctx, prm.cnrID, prm.signer, cliPrm)
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

	return &SearchObjectsRes{
		ids: list,
	}, nil
}

// HashPayloadRangesPrm groups parameters of HashPayloadRanges operation.
type HashPayloadRangesPrm struct {
	commonObjectPrm
	objectAddressPrm

	tz bool

	rngs []*object.Range

	salt []byte
}

// TZ sets flag to request Tillich-Zemor hashes.
func (x *HashPayloadRangesPrm) TZ() {
	x.tz = true
}

// SetRanges sets a list of payload ranges to hash.
func (x *HashPayloadRangesPrm) SetRanges(rngs []*object.Range) {
	x.rngs = rngs
}

// SetSalt sets data for each range to be XOR'ed with.
func (x *HashPayloadRangesPrm) SetSalt(salt []byte) {
	x.salt = salt
}

// HashPayloadRangesRes groups the resulting values of HashPayloadRanges operation.
type HashPayloadRangesRes struct {
	cliRes [][]byte
}

// HashList returns a list of hashes of the payload ranges keeping order.
func (x HashPayloadRangesRes) HashList() [][]byte {
	return x.cliRes
}

// HashPayloadRanges requests hashes (by default SHA256) of the object payload ranges.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns an error if number of received hashes differs with the number of requested ranges.
func HashPayloadRanges(ctx context.Context, prm HashPayloadRangesPrm) (*HashPayloadRangesRes, error) {
	var cliPrm client.PrmObjectHash

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.UseSalt(prm.salt)

	rngs := make([]uint64, 2*len(prm.rngs))

	for i := range prm.rngs {
		rngs[2*i] = prm.rngs[i].GetOffset()
		rngs[2*i+1] = prm.rngs[i].GetLength()
	}

	cliPrm.SetRangeList(rngs...)

	if prm.tz {
		cliPrm.TillichZemorAlgo()
	}

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	res, err := prm.cli.ObjectHash(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read payload hashes via client: %w", err)
	}

	return &HashPayloadRangesRes{
		cliRes: res,
	}, nil
}

// PayloadRangePrm groups parameters of PayloadRange operation.
type PayloadRangePrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm
	payloadWriterPrm

	rng *object.Range
}

// SetRange sets payload range to read.
func (x *PayloadRangePrm) SetRange(rng *object.Range) {
	x.rng = rng
}

// PayloadRangeRes groups the resulting values of PayloadRange operation.
type PayloadRangeRes struct{}

// PayloadRange reads object payload range from NeoFS and writes it to the specified writer.
//
// Interrupts on any writer error.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func PayloadRange(ctx context.Context, prm PayloadRangePrm) (*PayloadRangeRes, error) {
	var cliPrm client.PrmObjectRange

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.raw {
		cliPrm.MarkRaw()
	}

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectRangeInit(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.rng.GetOffset(), prm.rng.GetLength(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init payload reading: %w", err)
	}

	_, err = io.Copy(prm.wrt, rdr)
	if err != nil {
		return nil, fmt.Errorf("copy payload: %w", err)
	}

	return new(PayloadRangeRes), nil
}
