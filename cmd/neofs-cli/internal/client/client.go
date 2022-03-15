package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// BalanceOfPrm groups parameters of BalanceOf operation.
type BalanceOfPrm struct {
	commonPrm
	client.PrmBalanceGet
}

// BalanceOfRes groups resulting values of BalanceOf operation.
type BalanceOfRes struct {
	cliRes *client.ResBalanceGet
}

// Balance returns current balance.
func (x BalanceOfRes) Balance() *accounting.Decimal {
	return x.cliRes.Amount()
}

// BalanceOf requests current balance of NeoFS user.
//
// Returns any error prevented the operation from completing correctly in error return.
func BalanceOf(prm BalanceOfPrm) (res BalanceOfRes, err error) {
	res.cliRes, err = prm.cli.BalanceGet(context.Background(), prm.PrmBalanceGet)

	return
}

// ListContainersPrm groups parameters of ListContainers operation.
type ListContainersPrm struct {
	commonPrm
	client.PrmContainerList
}

// ListContainersRes groups resulting values of ListContainers operation.
type ListContainersRes struct {
	cliRes *client.ResContainerList
}

// IDList returns list of identifiers of user's containers.
func (x ListContainersRes) IDList() []cid.ID {
	return x.cliRes.Containers()
}

// ListContainers requests list of NeoFS user's containers.
//
// Returns any error prevented the operation from completing correctly in error return.
func ListContainers(prm ListContainersPrm) (res ListContainersRes, err error) {
	res.cliRes, err = prm.cli.ContainerList(context.Background(), prm.PrmContainerList)

	return
}

// PutContainerPrm groups parameters of PutContainer operation.
type PutContainerPrm struct {
	commonPrm
	client.PrmContainerPut
}

// PutContainerRes groups resulting values of PutContainer operation.
type PutContainerRes struct {
	cliRes *client.ResContainerPut
}

// ID returns identifier of the created container.
func (x PutContainerRes) ID() *cid.ID {
	return x.cliRes.ID()
}

// PutContainer sends request to save container in NeoFS.
//
// Operation is asynchronous and no guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by identifier.
//
// Returns any error prevented the operation from completing correctly in error return.
func PutContainer(prm PutContainerPrm) (res PutContainerRes, err error) {
	res.cliRes, err = prm.cli.ContainerPut(context.Background(), prm.PrmContainerPut)

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

// GetContainerRes groups resulting values of GetContainer operation.
type GetContainerRes struct {
	cliRes *client.ResContainerGet
}

// Container returns structured of the requested container.
func (x GetContainerRes) Container() *container.Container {
	return x.cliRes.Container()
}

// GetContainer reads container from NeoFS by ID.
//
// Returns any error prevented the operation from completing correctly in error return.
func GetContainer(prm GetContainerPrm) (res GetContainerRes, err error) {
	res.cliRes, err = prm.cli.ContainerGet(context.Background(), prm.cliPrm)

	return
}

// DeleteContainerPrm groups parameters of DeleteContainerPrm operation.
type DeleteContainerPrm struct {
	commonPrm
	client.PrmContainerDelete
}

// DeleteContainerRes groups resulting values of DeleteContainer operation.
type DeleteContainerRes struct{}

// DeleteContainer sends request to remove container from NeoFS by ID.
//
// Operation is asynchronous and no guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by identifier.
//
// Returns any error prevented the operation from completing correctly in error return.
func DeleteContainer(prm DeleteContainerPrm) (res DeleteContainerRes, err error) {
	_, err = prm.cli.ContainerDelete(context.Background(), prm.PrmContainerDelete)

	return
}

// EACLPrm groups parameters of EACL operation.
type EACLPrm struct {
	commonPrm
	client.PrmContainerEACL
}

// EACLRes groups resulting values of EACL operation.
type EACLRes struct {
	cliRes *client.ResContainerEACL
}

// EACL returns requested eACL table.
func (x EACLRes) EACL() *eacl.Table {
	return x.cliRes.Table()
}

// EACL reads eACL table from NeoFS by container ID.
//
// Returns any error prevented the operation from completing correctly in error return.
func EACL(prm EACLPrm) (res EACLRes, err error) {
	res.cliRes, err = prm.cli.ContainerEACL(context.Background(), prm.PrmContainerEACL)

	return
}

// SetEACLPrm groups parameters of SetEACL operation.
type SetEACLPrm struct {
	commonPrm
	client.PrmContainerSetEACL
}

// SetEACLRes groups resulting values of SetEACL operation.
type SetEACLRes struct{}

// SetEACL requests to save eACL table in NeoFS.
//
// Operation is asynchronous and no guaranteed even in the absence of errors.
// The required time is also not predictable.
//
// Success can be verified by reading by container identifier.
//
// Returns any error prevented the operation from completing correctly in error return.
func SetEACL(prm SetEACLPrm) (res SetEACLRes, err error) {
	_, err = prm.cli.ContainerSetEACL(context.Background(), prm.PrmContainerSetEACL)

	return
}

// NetworkInfoPrm groups parameters of NetworkInfo operation.
type NetworkInfoPrm struct {
	commonPrm
	client.PrmNetworkInfo
}

// NetworkInfoRes groups resulting values of NetworkInfo operation.
type NetworkInfoRes struct {
	cliRes *client.ResNetworkInfo
}

// NetworkInfo returns structured information about the NeoFS network.
func (x NetworkInfoRes) NetworkInfo() *netmap.NetworkInfo {
	return x.cliRes.Info()
}

// NetworkInfo reads information about the NeoFS network.
//
// Returns any error prevented the operation from completing correctly in error return.
func NetworkInfo(prm NetworkInfoPrm) (res NetworkInfoRes, err error) {
	res.cliRes, err = prm.cli.NetworkInfo(context.Background(), prm.PrmNetworkInfo)

	return
}

// NodeInfoPrm groups parameters of NodeInfo operation.
type NodeInfoPrm struct {
	commonPrm
	client.PrmEndpointInfo
}

// NodeInfoRes groups resulting values of NodeInfo operation.
type NodeInfoRes struct {
	cliRes *client.ResEndpointInfo
}

// NodeInfo returns information about the node from netmap.
func (x NodeInfoRes) NodeInfo() *netmap.NodeInfo {
	return x.cliRes.NodeInfo()
}

// LatestVersion returns latest NeoFS API version in use.
func (x NodeInfoRes) LatestVersion() *version.Version {
	return x.cliRes.LatestVersion()
}

// NodeInfo requests information about the remote server from NeoFS netmap.
//
// Returns any error prevented the operation from completing correctly in error return.
func NodeInfo(prm NodeInfoPrm) (res NodeInfoRes, err error) {
	res.cliRes, err = prm.cli.EndpointInfo(context.Background(), prm.PrmEndpointInfo)

	return
}

// CreateSessionPrm groups parameters of CreateSession operation.
type CreateSessionPrm struct {
	commonPrm
	client.PrmSessionCreate
}

// CreateSessionRes groups resulting values of CreateSession operation.
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

// CreateSession opens new unlimited session with the remote node.
//
// Returns any error prevented the operation from completing correctly in error return.
func CreateSession(prm CreateSessionPrm) (res CreateSessionRes, err error) {
	res.cliRes, err = prm.cli.SessionCreate(context.Background(), prm.PrmSessionCreate)

	return
}

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonObjectPrm

	hdr *object.Object

	rdr io.Reader
}

// SetHeader sets object header.
func (x *PutObjectPrm) SetHeader(hdr *object.Object) {
	x.hdr = hdr
}

// SetPayloadReader sets reader of the object payload.
func (x *PutObjectPrm) SetPayloadReader(rdr io.Reader) {
	x.rdr = rdr
}

// PutObjectRes groups resulting values of PutObject operation.
type PutObjectRes struct {
	id *oidSDK.ID
}

// ID returns identifier of the created object.
func (x PutObjectRes) ID() *oidSDK.ID {
	return x.id
}

// PutObject saves the object in NeoFS network.
//
// Returns any error prevented the operation from completing correctly in error return.
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

	wrt.WithXHeaders(prm.xHeadersPrm()...)

	if wrt.WriteHeader(*prm.hdr) {
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

	res, err := wrt.Close()
	if err != nil { // here err already carries both status and client errors
		return nil, fmt.Errorf("client failure: %w", err)
	}

	var id oidSDK.ID

	if !res.ReadStoredObjectID(&id) {
		return nil, errors.New("missing ID of the stored object")
	}

	return &PutObjectRes{
		id: &id,
	}, nil
}

// DeleteObjectPrm groups parameters of DeleteObject operation.
type DeleteObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
}

// DeleteObjectRes groups resulting values of DeleteObject operation.
type DeleteObjectRes struct {
	addrTombstone *addressSDK.Address
}

// TombstoneAddress returns address of the created object with tombstone.
func (x DeleteObjectRes) TombstoneAddress() *addressSDK.Address {
	return x.addrTombstone
}

// DeleteObject marks object to be removed from NeoFS through tombstone placement.
//
// Returns any error prevented the operation from completing correctly in error return.
func DeleteObject(prm DeleteObjectPrm) (*DeleteObjectRes, error) {
	var delPrm client.PrmObjectDelete

	if id := prm.objAddr.ContainerID(); id != nil {
		delPrm.FromContainer(*id)
	}

	if id := prm.objAddr.ObjectID(); id != nil {
		delPrm.ByID(*id)
	}

	if prm.sessionToken != nil {
		delPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		delPrm.WithBearerToken(*prm.bearerToken)
	}

	delPrm.WithXHeaders(prm.xHeadersPrm()...)

	cliRes, err := prm.cli.ObjectDelete(context.Background(), delPrm)
	if err != nil {
		return nil, fmt.Errorf("remove object via client: %w", err)
	}

	var id oidSDK.ID

	if !cliRes.ReadTombstoneID(&id) {
		return nil, errors.New("object removed but tombstone ID is missing")
	}

	var addr addressSDK.Address
	addr.SetObjectID(&id)
	addr.SetContainerID(prm.objAddr.ContainerID())

	return &DeleteObjectRes{
		addrTombstone: &addr,
	}, nil
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm
	payloadWriterPrm
}

// GetObjectRes groups resulting values of GetObject operation.
type GetObjectRes struct {
	hdr *object.Object
}

// Header returns header of the request object.
func (x GetObjectRes) Header() *object.Object {
	return x.hdr
}

// maximum size of the buffer use for io.Copy*.
// Chosen small due to the expected low volume of NeoFS CLI process resources.
// TODO: (neofs-node#1198) explore better values or configure it
const maxPayloadBufferSize = 1024

// GetObject reads the object by address.
//
// Interrupts on any writer error. If successful, payload is written to writer.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func GetObject(prm GetObjectPrm) (*GetObjectRes, error) {
	var getPrm client.PrmObjectGet

	if id := prm.objAddr.ContainerID(); id != nil {
		getPrm.FromContainer(*id)
	}

	if id := prm.objAddr.ObjectID(); id != nil {
		getPrm.ByID(*id)
	}

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

	getPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectGetInit(context.Background(), getPrm)
	if err != nil {
		return nil, fmt.Errorf("init object reading on client: %w", err)
	}

	var hdr object.Object

	if !rdr.ReadHeader(&hdr) {
		_, err = rdr.Close()
		return nil, fmt.Errorf("read object header: %w", err)
	}

	sz := hdr.PayloadSize()
	if sz > maxPayloadBufferSize {
		sz = maxPayloadBufferSize
	}

	buf := make([]byte, sz)

	_, err = io.CopyBuffer(prm.wrt, rdr, buf)
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

// SetMainOnlyFlag sets flag to get only main fields of object header in terms of NeoFS API.
func (x *HeadObjectPrm) SetMainOnlyFlag(v bool) {
	x.mainOnly = v
}

// HeadObjectRes groups resulting values of HeadObject operation.
type HeadObjectRes struct {
	hdr *object.Object
}

// Header returns requested object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.hdr
}

// HeadObject reads object header by address.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func HeadObject(prm HeadObjectPrm) (*HeadObjectRes, error) {
	var cliPrm client.PrmObjectHead

	if id := prm.objAddr.ContainerID(); id != nil {
		cliPrm.FromContainer(*id)
	}

	if id := prm.objAddr.ObjectID(); id != nil {
		cliPrm.ByID(*id)
	}

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

	cliPrm.WithXHeaders(prm.xHeadersPrm()...)

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

// SearchObjectsRes groups resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	ids []*oidSDK.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []*oidSDK.ID {
	return x.ids
}

// SearchObjects selects objects from container which match the filters.
//
// Returns any error prevented the operation from completing correctly in error return.
func SearchObjects(prm SearchObjectsPrm) (*SearchObjectsRes, error) {
	var cliPrm client.PrmObjectSearch

	if prm.cnrID != nil {
		cliPrm.InContainer(*prm.cnrID)
	}

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

	cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectSearchInit(context.Background(), cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object search: %w", err)
	}

	buf := make([]oidSDK.ID, 10)
	var list []*oidSDK.ID
	var n int
	var ok bool

	for {
		n, ok = rdr.Read(buf)
		for i := 0; i < n; i++ {
			v := buf[i]
			list = append(list, &v)
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

// SetRanges sets list of payload ranges to hash.
func (x *HashPayloadRangesPrm) SetRanges(rngs []*object.Range) {
	x.rngs = rngs
}

// SetSalt sets data for each range to be XOR'ed with.
func (x *HashPayloadRangesPrm) SetSalt(salt []byte) {
	x.salt = salt
}

// HashPayloadRangesRes groups resulting values of HashPayloadRanges operation.
type HashPayloadRangesRes struct {
	cliRes *client.ResObjectHash
}

// HashList returns list of hashes of the payload ranges keeping order.
func (x HashPayloadRangesRes) HashList() [][]byte {
	return x.cliRes.Checksums()
}

// HashPayloadRanges requests hashes (by default SHA256) of the object payload ranges.
//
// Returns any error prevented the operation from completing correctly in error return.
// Returns an error if number of received hashes differs with the number of requested ranges.
func HashPayloadRanges(prm HashPayloadRangesPrm) (*HashPayloadRangesRes, error) {
	var cliPrm client.PrmObjectHash

	if id := prm.objAddr.ContainerID(); id != nil {
		cliPrm.FromContainer(*id)
	}

	if id := prm.objAddr.ObjectID(); id != nil {
		cliPrm.ByID(*id)
	}

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

	cliPrm.WithXHeaders(prm.xHeadersPrm()...)

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

// PayloadRangeRes groups resulting values of PayloadRange operation.
type PayloadRangeRes struct{}

// PayloadRange reads object payload range from NeoFS and writes it to specified writer.
//
// Interrupts on any writer error.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func PayloadRange(prm PayloadRangePrm) (*PayloadRangeRes, error) {
	var cliPrm client.PrmObjectRange

	if id := prm.objAddr.ContainerID(); id != nil {
		cliPrm.FromContainer(*id)
	}

	if id := prm.objAddr.ObjectID(); id != nil {
		cliPrm.ByID(*id)
	}

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

	cliPrm.WithXHeaders(prm.xHeadersPrm()...)

	rdr, err := prm.cli.ObjectRangeInit(context.Background(), cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init payload reading: %w", err)
	}

	sz := prm.rng.GetLength()
	if sz > maxPayloadBufferSize {
		sz = maxPayloadBufferSize
	}

	buf := make([]byte, sz)

	_, err = io.CopyBuffer(prm.wrt, rdr, buf)
	if err != nil {
		return nil, fmt.Errorf("copy payload: %w", err)
	}

	return new(PayloadRangeRes), nil
}
