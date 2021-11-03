package internal

import (
	"context"
	"crypto/sha256"
	"io"
	"math"

	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// BalanceOfPrm groups parameters of BalanceOf operation.
type BalanceOfPrm struct {
	commonPrm
	ownerIDPrm
}

// BalanceOfRes groups resulting values of BalanceOf operation.
type BalanceOfRes struct {
	cliRes *accounting.Decimal
}

// Balance returns current balance.
func (x BalanceOfRes) Balance() *accounting.Decimal {
	return x.cliRes
}

// BalanceOf requests current balance of NeoFS user.
//
// Returns any error prevented the operation from completing correctly in error return.
func BalanceOf(prm BalanceOfPrm) (res BalanceOfRes, err error) {
	res.cliRes, err = prm.cli.GetBalance(context.Background(), prm.ownerID,
		client.WithKey(prm.privKey),
	)

	return
}

// ListContainersPrm groups parameters of ListContainers operation.
type ListContainersPrm struct {
	commonPrm
	ownerIDPrm
}

// ListContainersRes groups resulting values of ListContainers operation.
type ListContainersRes struct {
	cliRes []*cid.ID
}

// IDList returns list of identifiers of user's containers.
func (x ListContainersRes) IDList() []*cid.ID {
	return x.cliRes
}

// ListContainers requests list of NeoFS user's containers.
//
// Returns any error prevented the operation from completing correctly in error return.
func ListContainers(prm ListContainersPrm) (res ListContainersRes, err error) {
	res.cliRes, err = prm.cli.ListContainers(context.Background(), prm.ownerID,
		client.WithKey(prm.privKey),
	)

	return
}

// PutContainerPrm groups parameters of PutContainer operation.
type PutContainerPrm struct {
	commonPrm
	sessionTokenPrm

	cnr *container.Container
}

// SetContainer sets container structure.
func (x *PutContainerPrm) SetContainer(cnr *container.Container) {
	x.cnr = cnr
}

// PutContainerRes groups resulting values of PutContainer operation.
type PutContainerRes struct {
	cliRes *cid.ID
}

// ID returns identifier of the created container.
func (x PutContainerRes) ID() *cid.ID {
	return x.cliRes
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
	res.cliRes, err = prm.cli.PutContainer(context.Background(), prm.cnr,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
	)

	return
}

// GetContainerPrm groups parameters of GetContainer operation.
type GetContainerPrm struct {
	commonPrm
	containerIDPrm
}

// GetContainerRes groups resulting values of GetContainer operation.
type GetContainerRes struct {
	cliRes *container.Container
}

// Container returns structured of the requested container.
func (x GetContainerRes) Container() *container.Container {
	return x.cliRes
}

// GetContainer reads container from NeoFS by ID.
//
// Returns any error prevented the operation from completing correctly in error return.
func GetContainer(prm GetContainerPrm) (res GetContainerRes, err error) {
	res.cliRes, err = prm.cli.GetContainer(context.Background(), prm.cnrID,
		client.WithKey(prm.privKey),
	)

	return
}

// DeleteContainerPrm groups parameters of DeleteContainerPrm operation.
type DeleteContainerPrm struct {
	commonPrm
	sessionTokenPrm
	containerIDPrm
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
	err = prm.cli.DeleteContainer(context.Background(), prm.cnrID,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
	)

	return
}

// EACLPrm groups parameters of EACL operation.
type EACLPrm struct {
	commonPrm
	containerIDPrm
}

// EACLRes groups resulting values of EACL operation.
type EACLRes struct {
	cliRes *client.EACLWithSignature
}

// EACL returns requested eACL table.
func (x EACLRes) EACL() *eacl.Table {
	return x.cliRes.EACL()
}

// EACL reads eACL table from NeoFS by container ID.
//
// Returns any error prevented the operation from completing correctly in error return.
func EACL(prm EACLPrm) (res EACLRes, err error) {
	res.cliRes, err = prm.cli.GetEACL(context.Background(), prm.cnrID,
		client.WithKey(prm.privKey),
	)

	return
}

// SetEACLPrm groups parameters of SetEACL operation.
type SetEACLPrm struct {
	commonPrm
	sessionTokenPrm

	eaclTable *eacl.Table
}

// SetEACLTable sets eACL table structure.
func (x *SetEACLPrm) SetEACLTable(table *eacl.Table) {
	x.eaclTable = table
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
	err = prm.cli.SetEACL(context.Background(), prm.eaclTable,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
	)

	return
}

// NetworkInfoPrm groups parameters of NetworkInfo operation.
type NetworkInfoPrm struct {
	commonPrm
}

// NetworkInfoRes groups resulting values of NetworkInfo operation.
type NetworkInfoRes struct {
	cliRes *netmap.NetworkInfo
}

// NetworkInfo returns structured information about the NeoFS network.
func (x NetworkInfoRes) NetworkInfo() *netmap.NetworkInfo {
	return x.cliRes
}

// NetworkInfo reads information about the NeoFS network.
//
// Returns any error prevented the operation from completing correctly in error return.
func NetworkInfo(prm NetworkInfoPrm) (res NetworkInfoRes, err error) {
	res.cliRes, err = prm.cli.NetworkInfo(context.Background(),
		client.WithKey(prm.privKey),
	)

	return
}

// NodeInfoPrm groups parameters of NodeInfo operation.
type NodeInfoPrm struct {
	commonPrm
}

// NodeInfoRes groups resulting values of NodeInfo operation.
type NodeInfoRes struct {
	cliRes *client.EndpointInfo
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
	res.cliRes, err = prm.cli.EndpointInfo(context.Background(),
		client.WithKey(prm.privKey),
	)

	return
}

// CreateSessionPrm groups parameters of CreateSession operation.
type CreateSessionPrm struct {
	commonPrm
}

// CreateSessionRes groups resulting values of CreateSession operation.
type CreateSessionRes struct {
	cliRes *session.Token
}

// ID returns session identifier.
func (x CreateSessionRes) ID() []byte {
	return x.cliRes.ID()
}

// SessionKey returns public session key in a binary format.
func (x CreateSessionRes) SessionKey() []byte {
	return x.cliRes.SessionKey()
}

// CreateSession opens new unlimited session with the remote node.
//
// Returns any error prevented the operation from completing correctly in error return.
func CreateSession(prm CreateSessionPrm) (res CreateSessionRes, err error) {
	res.cliRes, err = prm.cli.CreateSession(context.Background(), math.MaxUint64,
		client.WithKey(prm.privKey),
	)

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
	cliRes *object.ID
}

// ID returns identifier of the created object.
func (x PutObjectRes) ID() *object.ID {
	return x.cliRes
}

// PutObject saves the object in NeoFS network.
//
// Returns any error prevented the operation from completing correctly in error return.
func PutObject(prm PutObjectPrm) (res PutObjectRes, err error) {
	var putPrm client.PutObjectParams

	putPrm.WithObject(prm.hdr)
	putPrm.WithPayloadReader(prm.rdr)

	res.cliRes, err = prm.cli.PutObject(context.Background(), &putPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
}

// DeleteObjectPrm groups parameters of DeleteObject operation.
type DeleteObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
}

// DeleteObjectRes groups resulting values of DeleteObject operation.
type DeleteObjectRes struct {
	cliRes *object.Address
}

// TombstoneAddress returns address of the created object with tombstone.
func (x DeleteObjectRes) TombstoneAddress() *object.Address {
	return x.cliRes
}

// DeleteObject marks object to be removed from NeoFS through tombstone placement.
//
// Returns any error prevented the operation from completing correctly in error return.
func DeleteObject(prm DeleteObjectPrm) (res DeleteObjectRes, err error) {
	var delPrm client.DeleteObjectParams

	delPrm.WithAddress(prm.objAddr)

	res.cliRes, err = client.DeleteObject(context.Background(), prm.cli, &delPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
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
	cliRes *object.Object
}

// Object returns header of the request object.
func (x GetObjectRes) Header() *object.Object {
	return x.cliRes
}

// GetObject reads the object by address.
//
// Interrupts on any writer error. If successful, payload is written to writer.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func GetObject(prm GetObjectPrm) (res GetObjectRes, err error) {
	var getPrm client.GetObjectParams

	getPrm.WithAddress(prm.objAddr)
	getPrm.WithPayloadWriter(prm.wrt)
	getPrm.WithRawFlag(prm.raw)

	res.cliRes, err = prm.cli.GetObject(context.Background(), &getPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
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
	cliRes *object.Object
}

// Header returns requested object header.
func (x HeadObjectRes) Header() *object.Object {
	return x.cliRes
}

// HeadObject reads object header by address.
//
// Returns any error prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func HeadObject(prm HeadObjectPrm) (res HeadObjectRes, err error) {
	var cliPrm client.ObjectHeaderParams

	cliPrm.WithAddress(prm.objAddr)
	cliPrm.WithRawFlag(prm.raw)

	if prm.mainOnly {
		cliPrm.WithMainFields()
	} else {
		cliPrm.WithAllFields()
	}

	res.cliRes, err = prm.cli.GetObjectHeader(context.Background(), &cliPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
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
	cliRes []*object.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []*object.ID {
	return x.cliRes
}

// SearchObjects selects objects from container which match the filters.
//
// Returns any error prevented the operation from completing correctly in error return.
func SearchObjects(prm SearchObjectsPrm) (res SearchObjectsRes, err error) {
	var cliPrm client.SearchObjectParams

	cliPrm.WithSearchFilters(prm.filters)
	cliPrm.WithContainerID(prm.cnrID)

	res.cliRes, err = prm.cli.SearchObject(context.Background(), &cliPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
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
	cliRes [][]byte
}

// HashList returns list of hashes of the payload ranges keeping order.
func (x HashPayloadRangesRes) HashList() [][]byte {
	return x.cliRes
}

// HashPayloadRanges requests hashes (by default SHA256) of the object payload ranges.
//
// Returns any error prevented the operation from completing correctly in error return.
// Returns an error if number of received hashes differs with the number of requested ranges.
func HashPayloadRanges(prm HashPayloadRangesPrm) (res HashPayloadRangesRes, err error) {
	var cliPrm client.RangeChecksumParams

	cliPrm.WithAddress(prm.objAddr)
	cliPrm.WithSalt(prm.salt)
	cliPrm.WithRangeList(prm.rngs...)

	if prm.tz {
		var hs [][sha256.Size]byte

		hs, err = prm.cli.ObjectPayloadRangeSHA256(context.Background(), &cliPrm, append(prm.opts,
			client.WithKey(prm.privKey),
			client.WithSession(prm.sessionToken),
			client.WithBearer(prm.bearerToken),
		)...)
		if err == nil {
			res.cliRes = make([][]byte, 0, len(hs))

			for i := range hs {
				res.cliRes = append(res.cliRes, hs[i][:])
			}
		}
	} else {
		var hs [][client.TZSize]byte

		hs, err = prm.cli.ObjectPayloadRangeTZ(context.Background(), &cliPrm, append(prm.opts,
			client.WithKey(prm.privKey),
			client.WithSession(prm.sessionToken),
			client.WithBearer(prm.bearerToken),
		)...)
		if err == nil {
			res.cliRes = make([][]byte, 0, len(hs))

			for i := range hs {
				res.cliRes = append(res.cliRes, hs[i][:])
			}
		}
	}

	return
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
func PayloadRange(prm PayloadRangePrm) (res PayloadRangeRes, err error) {
	var cliPrm client.RangeDataParams

	cliPrm.WithRaw(prm.raw)
	cliPrm.WithAddress(prm.objAddr)
	cliPrm.WithDataWriter(prm.wrt)
	cliPrm.WithRange(prm.rng)

	_, err = prm.cli.ObjectPayloadRangeData(context.Background(), &cliPrm, append(prm.opts,
		client.WithKey(prm.privKey),
		client.WithSession(prm.sessionToken),
		client.WithBearer(prm.bearerToken),
	)...)

	return
}
