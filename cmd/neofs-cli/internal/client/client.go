package internal

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// BalanceOfPrm groups parameters of BalanceOf operation.
type BalanceOfPrm struct {
	commonPrm
	client.PrmBalanceGet
}

// BalanceOfRes groups the resulting values of BalanceOf operation.
type BalanceOfRes struct {
	cliRes *client.ResBalanceGet
}

// Balance returns the current balance.
func (x BalanceOfRes) Balance() *accounting.Decimal {
	return x.cliRes.Amount()
}

// BalanceOf requests the current balance of a NeoFS user.
//
// Returns any error which prevented the operation from completing correctly in error return.
func BalanceOf(prm BalanceOfPrm) (res BalanceOfRes, err error) {
	res.cliRes, err = prm.cli.BalanceGet(context.Background(), prm.PrmBalanceGet)

	return
}

// ListContainersPrm groups parameters of ListContainers operation.
type ListContainersPrm struct {
	commonPrm
	client.PrmContainerList
}

// ListContainersRes groups the resulting values of ListContainers operation.
type ListContainersRes struct {
	cliRes *client.ResContainerList
}

// IDList returns list of identifiers of user's containers.
func (x ListContainersRes) IDList() []cid.ID {
	return x.cliRes.Containers()
}

// ListContainers requests a list of NeoFS user's containers.
//
// Returns any error which prevented the operation from completing correctly in error return.
func ListContainers(prm ListContainersPrm) (res ListContainersRes, err error) {
	res.cliRes, err = prm.cli.ContainerList(context.Background(), prm.PrmContainerList)

	return
}

// PutContainerPrm groups parameters of PutContainer operation.
type PutContainerPrm struct {
	commonPrm
	client.PrmContainerPut
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
func PutContainer(prm PutContainerPrm) (res PutContainerRes, err error) {
	cliRes, err := prm.cli.ContainerPut(context.Background(), prm.PrmContainerPut)
	if err == nil {
		cnr := cliRes.ID()
		if cnr == nil {
			err = errors.New("missing container ID in response")
		} else {
			res.cnr = *cnr
		}
	}

	return
}

// GetContainerPrm groups parameters of GetContainer operation.
type GetContainerPrm struct {
	commonPrm
	cliPrm client.PrmContainerGet
}

// SetContainer sets identifier of the container to be read.
func (x *GetContainerPrm) SetContainer(id cid.ID) {
	x.cliPrm.SetContainer(id)
}

// GetContainerRes groups the resulting values of GetContainer operation.
type GetContainerRes struct {
	cliRes *client.ResContainerGet
}

// Container returns structured of the requested container.
func (x GetContainerRes) Container() containerSDK.Container {
	return x.cliRes.Container()
}

// GetContainer reads a container from NeoFS by ID.
//
// Returns any error which prevented the operation from completing correctly in error return.
func GetContainer(prm GetContainerPrm) (res GetContainerRes, err error) {
	res.cliRes, err = prm.cli.ContainerGet(context.Background(), prm.cliPrm)

	return
}

// DeleteContainerPrm groups parameters of DeleteContainerPrm operation.
type DeleteContainerPrm struct {
	commonPrm
	client.PrmContainerDelete
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
func DeleteContainer(prm DeleteContainerPrm) (res DeleteContainerRes, err error) {
	_, err = prm.cli.ContainerDelete(context.Background(), prm.PrmContainerDelete)

	return
}

// EACLPrm groups parameters of EACL operation.
type EACLPrm struct {
	commonPrm
	client.PrmContainerEACL
}

// EACLRes groups the resulting values of EACL operation.
type EACLRes struct {
	cliRes *client.ResContainerEACL
}

// EACL returns requested eACL table.
func (x EACLRes) EACL() *eacl.Table {
	return x.cliRes.Table()
}

// EACL reads eACL table from NeoFS by container ID.
//
// Returns any error which prevented the operation from completing correctly in error return.
func EACL(prm EACLPrm) (res EACLRes, err error) {
	res.cliRes, err = prm.cli.ContainerEACL(context.Background(), prm.PrmContainerEACL)

	return
}

// SetEACLPrm groups parameters of SetEACL operation.
type SetEACLPrm struct {
	commonPrm
	client.PrmContainerSetEACL
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
func SetEACL(prm SetEACLPrm) (res SetEACLRes, err error) {
	_, err = prm.cli.ContainerSetEACL(context.Background(), prm.PrmContainerSetEACL)

	return
}

// NetworkInfoPrm groups parameters of NetworkInfo operation.
type NetworkInfoPrm struct {
	commonPrm
	client.PrmNetworkInfo
}

// NetworkInfoRes groups the resulting values of NetworkInfo operation.
type NetworkInfoRes struct {
	cliRes *client.ResNetworkInfo
}

// NetworkInfo returns structured information about the NeoFS network.
func (x NetworkInfoRes) NetworkInfo() *netmap.NetworkInfo {
	return x.cliRes.Info()
}

// NetworkInfo reads information about the NeoFS network.
//
// Returns any error which prevented the operation from completing correctly in error return.
func NetworkInfo(prm NetworkInfoPrm) (res NetworkInfoRes, err error) {
	res.cliRes, err = prm.cli.NetworkInfo(context.Background(), prm.PrmNetworkInfo)

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
	return *x.cliRes.NodeInfo()
}

// LatestVersion returns the latest NeoFS API version in use.
func (x NodeInfoRes) LatestVersion() *version.Version {
	return x.cliRes.LatestVersion()
}

// NodeInfo requests information about the remote server from NeoFS netmap.
//
// Returns any error which prevented the operation from completing correctly in error return.
func NodeInfo(prm NodeInfoPrm) (res NodeInfoRes, err error) {
	res.cliRes, err = prm.cli.EndpointInfo(context.Background(), prm.PrmEndpointInfo)

	return
}

// CreateSessionPrm groups parameters of CreateSession operation.
type CreateSessionPrm struct {
	commonPrm
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
func CreateSession(prm CreateSessionPrm) (res CreateSessionRes, err error) {
	res.cliRes, err = prm.cli.SessionCreate(context.Background(), prm.PrmSessionCreate)

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
func PutObject(prm PutObjectPrm) (*PutObjectRes, error) {
	var putPrm client.PrmObjectPutInit

	wrt, err := prm.cli.ObjectPutInit(context.Background(), putPrm)
	if err != nil {
		return nil, fmt.Errorf("init object writing: %w", err)
	}

	if prm.sessionToken != nil {
		wrt.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		wrt.WithBearerToken(*prm.bearerToken)
	}

	if prm.local {
		wrt.MarkLocal()
	}

	wrt.WithXHeaders(prm.xHeaders...)

	if wrt.WriteHeader(*prm.hdr) {
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
			// TODO: (neofs-node#1198) explore better values or configure it
			const defaultBufferSizePut = 4096

			if sz == 0 || sz > defaultBufferSizePut {
				sz = defaultBufferSizePut
			}

			buf := make([]byte, sz)

			var n int

			for {
				n, err = prm.rdr.Read(buf)
				if n > 0 {
					if !wrt.WritePayloadChunk(buf[:n]) {
						break
					}

					continue
				}

				if errors.Is(err, io.EOF) {
					break
				}

				return nil, fmt.Errorf("read payload: %w", err)
			}
		}
	}

	cliRes, err := wrt.Close()
	if err != nil { // here err already carries both status and client errors
		return nil, fmt.Errorf("client failure: %w", err)
	}

	var res PutObjectRes

	if !cliRes.ReadStoredObjectID(&res.id) {
		return nil, errors.New("missing ID of the stored object")
	}

	return &res, nil
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
func DeleteObject(prm DeleteObjectPrm) (*DeleteObjectRes, error) {
	var delPrm client.PrmObjectDelete
	delPrm.FromContainer(prm.objAddr.Container())
	delPrm.ByID(prm.objAddr.Object())

	if prm.sessionToken != nil {
		delPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		delPrm.WithBearerToken(*prm.bearerToken)
	}

	delPrm.WithXHeaders(prm.xHeaders...)

	cliRes, err := prm.cli.ObjectDelete(context.Background(), delPrm)
	if err != nil {
		return nil, fmt.Errorf("remove object via client: %w", err)
	}

	var id oid.ID

	if !cliRes.ReadTombstoneID(&id) {
		return nil, errors.New("object removed but tombstone ID is missing")
	}

	return &DeleteObjectRes{
		tomb: id,
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
func GetObject(prm GetObjectPrm) (*GetObjectRes, error) {
	var getPrm client.PrmObjectGet
	getPrm.FromContainer(prm.objAddr.Container())
	getPrm.ByID(prm.objAddr.Object())

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

	rdr, err := prm.cli.ObjectGetInit(context.Background(), getPrm)
	if err != nil {
		return nil, fmt.Errorf("init object reading on client: %w", err)
	}

	var hdr object.Object

	if !rdr.ReadHeader(&hdr) {
		_, err = rdr.Close()
		return nil, fmt.Errorf("read object header: %w", err)
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
func HeadObject(prm HeadObjectPrm) (*HeadObjectRes, error) {
	var cliPrm client.PrmObjectHead
	cliPrm.FromContainer(prm.objAddr.Container())
	cliPrm.ByID(prm.objAddr.Object())

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

	res, err := prm.cli.ObjectHead(context.Background(), cliPrm)
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
func SearchObjects(prm SearchObjectsPrm) (*SearchObjectsRes, error) {
	var cliPrm client.PrmObjectSearch
	cliPrm.InContainer(prm.cnrID)
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

	rdr, err := prm.cli.ObjectSearchInit(context.Background(), cliPrm)
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

	_, err = rdr.Close()
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
	cliRes *client.ResObjectHash
}

// HashList returns a list of hashes of the payload ranges keeping order.
func (x HashPayloadRangesRes) HashList() [][]byte {
	return x.cliRes.Checksums()
}

// HashPayloadRanges requests hashes (by default SHA256) of the object payload ranges.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns an error if number of received hashes differs with the number of requested ranges.
func HashPayloadRanges(prm HashPayloadRangesPrm) (*HashPayloadRangesRes, error) {
	var cliPrm client.PrmObjectHash
	cliPrm.FromContainer(prm.objAddr.Container())
	cliPrm.ByID(prm.objAddr.Object())

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

	res, err := prm.cli.ObjectHash(context.Background(), cliPrm)
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
func PayloadRange(prm PayloadRangePrm) (*PayloadRangeRes, error) {
	var cliPrm client.PrmObjectRange
	cliPrm.FromContainer(prm.objAddr.Container())
	cliPrm.ByID(prm.objAddr.Object())

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

	cliPrm.SetOffset(prm.rng.GetOffset())
	cliPrm.SetLength(prm.rng.GetLength())

	cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectRangeInit(context.Background(), cliPrm)
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
func SyncContainerSettings(prm SyncContainerPrm) (*SyncContainerRes, error) {
	if prm.c == nil {
		panic("sync container settings with the network: nil container")
	}

	err := client.SyncContainerWithNetwork(context.Background(), prm.c, prm.cli)
	if err != nil {
		return nil, err
	}

	return new(SyncContainerRes), nil
}
