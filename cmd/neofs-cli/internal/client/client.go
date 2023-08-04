package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// BalanceOfPrm groups parameters of BalanceOf operation.
type BalanceOfPrm struct {
	commonPrm
	client.PrmBalanceGet
}

// BalanceOfRes groups the resulting values of BalanceOf operation.
type BalanceOfRes struct {
	cliRes accounting.Decimal
}

// Balance returns the current balance.
func (x BalanceOfRes) Balance() accounting.Decimal {
	return x.cliRes
}

// BalanceOf requests the current balance of a NeoFS user.
//
// Returns any error which prevented the operation from completing correctly in error return.
func BalanceOf(ctx context.Context, prm BalanceOfPrm) (res BalanceOfRes, err error) {
	res.cliRes, err = prm.cli.BalanceGet(ctx, prm.PrmBalanceGet)

	return
}

// ListContainersPrm groups parameters of ListContainers operation.
type ListContainersPrm struct {
	commonPrm

	owner user.ID
	client.PrmContainerList
}

// SetAccount sets containers' owner.
func (l *ListContainersPrm) SetAccount(owner user.ID) {
	l.owner = owner
}

// ListContainersRes groups the resulting values of ListContainers operation.
type ListContainersRes struct {
	cliRes []cid.ID
}

// IDList returns list of identifiers of user's containers.
func (x ListContainersRes) IDList() []cid.ID {
	return x.cliRes
}

// ListContainers requests a list of NeoFS user's containers.
//
// Returns any error which prevented the operation from completing correctly in error return.
func ListContainers(ctx context.Context, prm ListContainersPrm) (res ListContainersRes, err error) {
	res.cliRes, err = prm.cli.ContainerList(ctx, prm.owner, prm.PrmContainerList)

	return
}

// PutContainerPrm groups parameters of PutContainer operation.
type PutContainerPrm struct {
	commonPrm
	signerRFC6979Prm

	cnr containerSDK.Container
	client.PrmContainerPut
}

// SetContainer sets container.
func (p *PutContainerPrm) SetContainer(cnr containerSDK.Container) {
	p.cnr = cnr
}

// PutContainerRes groups the resulting values of PutContainer operation.
type PutContainerRes struct {
	cnr cid.ID
}

// ID returns identifier of the created container.
func (x PutContainerRes) ID() cid.ID {
	return x.cnr
}

// PutContainer sends a request to save the container in NeoFS.
//
// Operation is asynchronous and not guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by identifier.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutContainer(ctx context.Context, prm PutContainerPrm) (res PutContainerRes, err error) {
	cliRes, err := prm.cli.ContainerPut(ctx, prm.cnr, prm.signer, prm.PrmContainerPut)
	if err == nil {
		res.cnr = cliRes
	}

	return
}

// GetContainerPrm groups parameters of GetContainer operation.
type GetContainerPrm struct {
	commonPrm

	cid    cid.ID
	cliPrm client.PrmContainerGet
}

// SetContainer sets identifier of the container to be read.
func (x *GetContainerPrm) SetContainer(id cid.ID) {
	x.cid = id
}

// GetContainerRes groups the resulting values of GetContainer operation.
type GetContainerRes struct {
	cliRes containerSDK.Container
}

// Container returns structured of the requested container.
func (x GetContainerRes) Container() containerSDK.Container {
	return x.cliRes
}

// GetContainer reads a container from NeoFS by ID.
//
// Returns any error which prevented the operation from completing correctly in error return.
func GetContainer(ctx context.Context, prm GetContainerPrm) (res GetContainerRes, err error) {
	res.cliRes, err = prm.cli.ContainerGet(ctx, prm.cid, prm.cliPrm)

	return
}

// IsACLExtendable checks if ACL of the container referenced by the given identifier
// can be extended. Client connection MUST BE correctly established in advance.
func IsACLExtendable(ctx context.Context, c *client.Client, cnr cid.ID) (bool, error) {
	var prm GetContainerPrm
	prm.SetClient(c)
	prm.SetContainer(cnr)

	res, err := GetContainer(ctx, prm)
	if err != nil {
		return false, fmt.Errorf("get container from the NeoFS: %w", err)
	}

	return res.Container().BasicACL().Extendable(), nil
}

// DeleteContainerPrm groups parameters of DeleteContainerPrm operation.
type DeleteContainerPrm struct {
	commonPrm
	signerRFC6979Prm

	cid cid.ID
	client.PrmContainerDelete
}

// SetContainer sets an ID of a container to be removed.
func (d *DeleteContainerPrm) SetContainer(cid cid.ID) {
	d.cid = cid
}

// DeleteContainerRes groups the resulting values of DeleteContainer operation.
type DeleteContainerRes struct{}

// DeleteContainer sends a request to remove a container from NeoFS by ID.
//
// Operation is asynchronous and not guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by identifier.
//
// Returns any error which prevented the operation from completing correctly in error return.
func DeleteContainer(ctx context.Context, prm DeleteContainerPrm) (res DeleteContainerRes, err error) {
	err = prm.cli.ContainerDelete(ctx, prm.cid, prm.signer, prm.PrmContainerDelete)

	return
}

// EACLPrm groups parameters of EACL operation.
type EACLPrm struct {
	commonPrm

	cid cid.ID
	client.PrmContainerEACL
}

// SetContainer sets container ID to be requested
// for its eACL.
func (E *EACLPrm) SetContainer(cid cid.ID) {
	E.cid = cid
}

// EACLRes groups the resulting values of EACL operation.
type EACLRes struct {
	cliRes eacl.Table
}

// EACL returns requested eACL table.
func (x EACLRes) EACL() eacl.Table {
	return x.cliRes
}

// EACL reads eACL table from NeoFS by container ID.
//
// Returns any error which prevented the operation from completing correctly in error return.
func EACL(ctx context.Context, prm EACLPrm) (res EACLRes, err error) {
	res.cliRes, err = prm.cli.ContainerEACL(ctx, prm.cid, prm.PrmContainerEACL)

	return
}

// SetEACLPrm groups parameters of SetEACL operation.
type SetEACLPrm struct {
	commonPrm
	signerRFC6979Prm

	table eacl.Table
	client.PrmContainerSetEACL
}

// SetTable sets extended Access Control List table to be applied.
func (s *SetEACLPrm) SetTable(table eacl.Table) {
	s.table = table
}

// SetEACLRes groups the resulting values of SetEACL operation.
type SetEACLRes struct{}

// SetEACL requests to save an eACL table in NeoFS.
//
// Operation is asynchronous and no guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by container identifier.
//
// Returns any error which prevented the operation from completing correctly in error return.
func SetEACL(ctx context.Context, prm SetEACLPrm) (res SetEACLRes, err error) {
	err = prm.cli.ContainerSetEACL(ctx, prm.table, prm.signer, prm.PrmContainerSetEACL)

	return
}

// NetworkInfoPrm groups parameters of NetworkInfo operation.
type NetworkInfoPrm struct {
	commonPrm
	client.PrmNetworkInfo
}

// NetworkInfoRes groups the resulting values of NetworkInfo operation.
type NetworkInfoRes struct {
	cliRes netmap.NetworkInfo
}

// NetworkInfo returns structured information about the NeoFS network.
func (x NetworkInfoRes) NetworkInfo() netmap.NetworkInfo {
	return x.cliRes
}

// NetworkInfo reads information about the NeoFS network.
//
// Returns any error which prevented the operation from completing correctly in error return.
func NetworkInfo(ctx context.Context, prm NetworkInfoPrm) (res NetworkInfoRes, err error) {
	res.cliRes, err = prm.cli.NetworkInfo(ctx, prm.PrmNetworkInfo)

	return
}

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

	res, err := prm.cli.ObjectHead(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read object header via client: %w", err)
	}

	var hdr object.Object

	if !res.ReadHeader(&hdr) {
		return nil, fmt.Errorf("missing header in response")
	}

	return &HeadObjectRes{
		hdr: &hdr,
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

	buf := make([]oid.ID, 10)
	var list []oid.ID
	var n int
	var ok bool

	for {
		n, ok = rdr.Read(buf)
		for i := 0; i < n; i++ {
			list = append(list, buf[i])
		}
		if !ok {
			break
		}
	}

	err = rdr.Close()
	if err != nil {
		return nil, fmt.Errorf("read object list: %w", err)
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

// SyncContainerPrm groups parameters of SyncContainerSettings operation.
type SyncContainerPrm struct {
	commonPrm
	c *containerSDK.Container
}

// SetContainer sets a container that is required to be synced.
func (s *SyncContainerPrm) SetContainer(c *containerSDK.Container) {
	s.c = c
}

// SyncContainerRes groups resulting values of SyncContainerSettings
// operation.
type SyncContainerRes struct{}

// SyncContainerSettings reads global network config from NeoFS and
// syncs container settings with it.
//
// Interrupts on any writer error.
//
// Panics if a container passed as a parameter is nil.
func SyncContainerSettings(ctx context.Context, prm SyncContainerPrm) (*SyncContainerRes, error) {
	if prm.c == nil {
		panic("sync container settings with the network: nil container")
	}

	err := client.SyncContainerWithNetwork(ctx, prm.c, prm.cli)
	if err != nil {
		return nil, err
	}

	return new(SyncContainerRes), nil
}
