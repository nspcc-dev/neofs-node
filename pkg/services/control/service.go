package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/util/proto"
)

// StableMarshal reads binary representation of health check request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *HealthCheckRequest_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of health check request body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *HealthCheckRequest_Body) StableSize() int {
	return 0
}

// SetBody sets health check request body.
func (x *HealthCheckRequest) SetBody(v *HealthCheckRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the health check request body.
func (x *HealthCheckRequest) SetSignature(body *Signature) {
	if x != nil {
		x.Signature = body
	}
}

// ReadSignedData reads signed data of health check request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *HealthCheckRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of health check request.
//
// Structures with the same field values have the same signed data size.
func (x *HealthCheckRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetNetmapStatus sets status of the storage node in NeoFS network map.
func (x *HealthCheckResponse_Body) SetNetmapStatus(v NetmapStatus) {
	if x != nil {
		x.NetmapStatus = v
	}
}

// SetHealthStatus sets health status of the storage node application.
func (x *HealthCheckResponse_Body) SetHealthStatus(v HealthStatus) {
	if x != nil {
		x.HealthStatus = v
	}
}

const (
	_ = iota
	healthRespBodyStatusFNum
	healthRespBodyHealthStatusFNum
)

// StableMarshal reads binary representation of health check response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *HealthCheckResponse_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var offset int

	offset += proto.EnumMarshal(healthRespBodyStatusFNum, buf[offset:], int32(x.NetmapStatus))
	proto.EnumMarshal(healthRespBodyHealthStatusFNum, buf[offset:], int32(x.HealthStatus))

	return buf
}

// StableSize returns binary size of health check response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *HealthCheckResponse_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.EnumSize(healthRespBodyStatusFNum, int32(x.NetmapStatus))
	size += proto.EnumSize(healthRespBodyHealthStatusFNum, int32(x.HealthStatus))

	return size
}

// SetBody sets health check response body.
func (x *HealthCheckResponse) SetBody(v *HealthCheckResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the health check response body.
func (x *HealthCheckResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of health check response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *HealthCheckResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of health check response.
//
// Structures with the same field values have the same signed data size.
func (x *HealthCheckResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of netmap snapshot request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NetmapSnapshotRequest_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of netmap snapshot request body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NetmapSnapshotRequest_Body) StableSize() int {
	return 0
}

// SetBody sets get netmap snapshot request body.
func (x *NetmapSnapshotRequest) SetBody(v *NetmapSnapshotRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the netmap snapshot request body.
func (x *NetmapSnapshotRequest) SetSignature(body *Signature) {
	if x != nil {
		x.Signature = body
	}
}

// ReadSignedData reads signed data of netmap snapshot request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *NetmapSnapshotRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of netmap snapshot request.
//
// Structures with the same field values have the same signed data size.
func (x *NetmapSnapshotRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetNetmap sets structure of the current network map.
func (x *NetmapSnapshotResponse_Body) SetNetmap(v *Netmap) {
	if x != nil {
		x.Netmap = v
	}
}

const (
	_ = iota
	snapshotRespBodyNetmapFNum
)

// StableMarshal reads binary representation of netmap snapshot response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NetmapSnapshotResponse_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	proto.NestedStructureMarshal(snapshotRespBodyNetmapFNum, buf, x.Netmap)

	return buf
}

// StableSize returns binary size of netmap snapshot response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NetmapSnapshotResponse_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.NestedStructureSize(snapshotRespBodyNetmapFNum, x.Netmap)

	return size
}

// SetBody sets get netmap snapshot response body.
func (x *NetmapSnapshotResponse) SetBody(v *NetmapSnapshotResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the get netmap snapshot response body.
func (x *NetmapSnapshotResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of netmap snapshot response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *NetmapSnapshotResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of netmap snapshot response.
//
// Structures with the same field values have the same signed data size.
func (x *NetmapSnapshotResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetStatus sets new storage node status in NeoFS network map.
func (x *SetNetmapStatusRequest_Body) SetStatus(v NetmapStatus) {
	if x != nil {
		x.Status = v
	}
}

const (
	_ = iota
	setNetmapStatusReqBodyStatusFNum
)

// StableMarshal reads binary representation of set netmap status request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *SetNetmapStatusRequest_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	proto.EnumMarshal(setNetmapStatusReqBodyStatusFNum, buf, int32(x.Status))

	return buf
}

// StableSize returns binary size of netmap status response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *SetNetmapStatusRequest_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.EnumSize(setNetmapStatusReqBodyStatusFNum, int32(x.Status))

	return size
}

// SetBody sets body of the set netmap status request .
func (x *SetNetmapStatusRequest) SetBody(v *SetNetmapStatusRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the set netmap status request body.
func (x *SetNetmapStatusRequest) SetSignature(body *Signature) {
	if x != nil {
		x.Signature = body
	}
}

// ReadSignedData reads signed data of set netmap status request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *SetNetmapStatusRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of set netmap status request.
//
// Structures with the same field values have the same signed data size.
func (x *SetNetmapStatusRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of set netmap status response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *SetNetmapStatusResponse_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of set netmap status response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *SetNetmapStatusResponse_Body) StableSize() int {
	return 0
}

// SetBody sets set body of the netmap status response.
func (x *SetNetmapStatusResponse) SetBody(v *SetNetmapStatusResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the set netmap status response body.
func (x *SetNetmapStatusResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of set netmap status response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *SetNetmapStatusResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of set netmap status response.
//
// Structures with the same field values have the same signed data size.
func (x *SetNetmapStatusResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetAddressList sets list of objects to be removed in NeoFS API binary format.
func (x *DropObjectsRequest_Body) SetAddressList(v [][]byte) {
	if x != nil {
		x.AddressList = v
	}
}

const (
	_ = iota
	addrListReqBodyStatusFNum
)

// StableMarshal reads binary representation of "Drop objects" request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *DropObjectsRequest_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	proto.RepeatedBytesMarshal(addrListReqBodyStatusFNum, buf, x.AddressList)

	return buf
}

// StableSize returns binary size of "Drop objects" response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *DropObjectsRequest_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.RepeatedBytesSize(addrListReqBodyStatusFNum, x.AddressList)

	return size
}

// SetBody sets body of the set "Drop objects" request.
func (x *DropObjectsRequest) SetBody(v *DropObjectsRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the "Drop objects" request body.
func (x *DropObjectsRequest) SetSignature(body *Signature) {
	if x != nil {
		x.Signature = body
	}
}

// ReadSignedData reads signed data of "Drop objects" request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *DropObjectsRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data of "Drop objects" request.
//
// Structures with the same field values have the same signed data size.
func (x *DropObjectsRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of "Drop objects" response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *DropObjectsResponse_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of "Drop objects" response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *DropObjectsResponse_Body) StableSize() int {
	return 0
}

// SetBody sets set body of the "Drop objects" response.
func (x *DropObjectsResponse) SetBody(v *DropObjectsResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the "Drop objects" response body.
func (x *DropObjectsResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of "Drop objects" response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *DropObjectsResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data of "Drop objects" response.
//
// Structures with the same field values have the same signed data size.
func (x *DropObjectsResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of list shards request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *ListShardsRequest_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of list shards request body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *ListShardsRequest_Body) StableSize() int {
	return 0
}

// SetBody sets list shards request body.
func (x *ListShardsRequest) SetBody(v *ListShardsRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the list shards request body.
func (x *ListShardsRequest) SetSignature(body *Signature) {
	if x != nil {
		x.Signature = body
	}
}

// ReadSignedData reads signed data of list shards request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *ListShardsRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of list shards request.
//
// Structures with the same field values have the same signed data size.
func (x *ListShardsRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetShards sets shards of the storage node.
func (x *ListShardsResponse_Body) SetShards(v []*ShardInfo) {
	if x != nil {
		x.Shards = v
	}
}

const (
	_ = iota
	listShardsRespBodyShardsFNum
)

// StableMarshal reads binary representation of list shards response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *ListShardsResponse_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var offset int

	for i := range x.Shards {
		offset += proto.NestedStructureMarshal(listShardsRespBodyShardsFNum, buf[offset:], x.Shards[i])
	}

	return buf
}

// StableSize returns binary size of list shards response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *ListShardsResponse_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	for i := range x.Shards {
		size += proto.NestedStructureSize(listShardsRespBodyShardsFNum, x.Shards[i])
	}

	return size
}

// SetBody sets list shards response body.
func (x *ListShardsResponse) SetBody(v *ListShardsResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the list shards response body.
func (x *ListShardsResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of list shards response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *ListShardsResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of list shards response.
//
// Structures with the same field values have the same signed data size.
func (x *ListShardsResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetShardID sets shard ID whose mode is requested to be set.
func (x *SetShardModeRequest_Body) SetShardID(v []byte) {
	if v != nil {
		x.Shard_ID = v
	}
}

// SetMode sets mode of the shard.
func (x *SetShardModeRequest_Body) SetMode(v ShardMode) {
	x.Mode = v
}

// ClearErrorCounter sets flag signifying whether error counter for shard should be cleared.
func (x *SetShardModeRequest_Body) ClearErrorCounter(reset bool) {
	x.ResetErrorCounter = reset
}

const (
	_ = iota
	setShardModeReqBodyShardIDFNum
	setShardModeReqBodyModeFNum
	setShardModeReqBodyResetCounterFNum
)

// StableMarshal reads binary representation of set shard mode request body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *SetShardModeRequest_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var offset int

	offset += proto.BytesMarshal(setShardModeReqBodyShardIDFNum, buf, x.Shard_ID)
	offset += proto.EnumMarshal(setShardModeReqBodyModeFNum, buf[offset:], int32(x.Mode))
	proto.BoolMarshal(setShardModeReqBodyResetCounterFNum, buf[offset:], x.ResetErrorCounter)

	return buf
}

// StableSize returns binary size of set shard mode response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *SetShardModeRequest_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(setShardModeReqBodyShardIDFNum, x.Shard_ID)
	size += proto.EnumSize(setShardModeReqBodyModeFNum, int32(x.Mode))
	size += proto.BoolSize(setShardModeReqBodyResetCounterFNum, x.ResetErrorCounter)

	return size
}

// SetBody sets request body.
func (x *SetShardModeRequest) SetBody(v *SetShardModeRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the request body.
func (x *SetShardModeRequest) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of the set shard mode request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *SetShardModeRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of the set shard mode request.
//
// Structures with the same field values have the same signed data size.
func (x *SetShardModeRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of the set shard mode response body
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *SetShardModeResponse_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of the set shard mode response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *SetShardModeResponse_Body) StableSize() int {
	return 0
}

// SetBody sets body of the set shard mode response.
func (x *SetShardModeResponse) SetBody(v *SetShardModeResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets signature of the set shard mode response body.
func (x *SetShardModeResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data of the set shard mode response to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *SetShardModeResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data
// of the set shard mode response.
//
// Structures with the same field values have the same signed data size.
func (x *SetShardModeResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetShardID sets shard ID for the dump shard request.
func (x *DumpShardRequest_Body) SetShardID(id []byte) {
	x.Shard_ID = id
}

// SetFilepath sets filepath for the dump shard request.
func (x *DumpShardRequest_Body) SetFilepath(p string) {
	x.Filepath = p
}

// SetIgnoreErrors sets ignore errors flag for the dump shard request.
func (x *DumpShardRequest_Body) SetIgnoreErrors(ignore bool) {
	x.IgnoreErrors = ignore
}

const (
	_ = iota
	dumpShardReqBodyShardIDFNum
	dumpShardReqBodyFilepathFNum
	dumpShardReqBodyIgnoreErrorsFNum
)

// StableMarshal reads binary representation of request body binary format.
//
// If buffer length is less than StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *DumpShardRequest_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var offset int

	offset += proto.BytesMarshal(dumpShardReqBodyShardIDFNum, buf, x.Shard_ID)
	offset += proto.StringMarshal(dumpShardReqBodyFilepathFNum, buf[offset:], x.Filepath)
	proto.BoolMarshal(dumpShardReqBodyIgnoreErrorsFNum, buf[offset:], x.IgnoreErrors)

	return buf
}

// StableSize returns binary size of the request body in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *DumpShardRequest_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(dumpShardReqBodyShardIDFNum, x.Shard_ID)
	size += proto.StringSize(dumpShardReqBodyFilepathFNum, x.Filepath)
	size += proto.BoolSize(dumpShardReqBodyIgnoreErrorsFNum, x.IgnoreErrors)

	return size
}

// SetBody sets request body.
func (x *DumpShardRequest) SetBody(v *DumpShardRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets body signature of the request.
func (x *DumpShardRequest) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data from request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *DumpShardRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns size of the request signed data in bytes.
//
// Structures with the same field values have the same signed data size.
func (x *DumpShardRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of the response body in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *DumpShardResponse_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of the response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *DumpShardResponse_Body) StableSize() int {
	return 0
}

// SetBody sets response body.
func (x *DumpShardResponse) SetBody(v *DumpShardResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets response body signature.
func (x *DumpShardResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data from response to buf.
//
// If buffer length is less than SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *DumpShardResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data.
//
// Structures with the same field values have the same signed data size.
func (x *DumpShardResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// SetShardID sets shard ID for the restore shard request.
func (x *RestoreShardRequest_Body) SetShardID(id []byte) {
	x.Shard_ID = id
}

// SetFilepath sets filepath for the restore shard request.
func (x *RestoreShardRequest_Body) SetFilepath(p string) {
	x.Filepath = p
}

// SetIgnoreErrors sets ignore errors flag for the restore shard request.
func (x *RestoreShardRequest_Body) SetIgnoreErrors(ignore bool) {
	x.IgnoreErrors = ignore
}

const (
	_ = iota
	restoreShardReqBodyShardIDFNum
	restoreShardReqBodyFilepathFNum
	restoreShardReqBodyIgnoreErrorsFNum
)

// StableMarshal reads binary representation of request body binary format.
//
// If buffer length is less than StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *RestoreShardRequest_Body) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	var offset int

	offset += proto.BytesMarshal(restoreShardReqBodyShardIDFNum, buf, x.Shard_ID)
	offset += proto.StringMarshal(restoreShardReqBodyFilepathFNum, buf[offset:], x.Filepath)
	proto.BoolMarshal(restoreShardReqBodyIgnoreErrorsFNum, buf[offset:], x.IgnoreErrors)

	return buf
}

// StableSize returns binary size of the request body in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *RestoreShardRequest_Body) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(restoreShardReqBodyShardIDFNum, x.Shard_ID)
	size += proto.StringSize(restoreShardReqBodyFilepathFNum, x.Filepath)
	size += proto.BoolSize(restoreShardReqBodyIgnoreErrorsFNum, x.IgnoreErrors)

	return size
}

// SetBody sets request body.
func (x *RestoreShardRequest) SetBody(v *RestoreShardRequest_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets body signature of the request.
func (x *RestoreShardRequest) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data from request to buf.
//
// If buffer length is less than x.SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *RestoreShardRequest) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns size of the request signed data in bytes.
//
// Structures with the same field values have the same signed data size.
func (x *RestoreShardRequest) SignedDataSize() int {
	return x.GetBody().StableSize()
}

// StableMarshal reads binary representation of the response body in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *RestoreShardResponse_Body) StableMarshal(buf []byte) []byte {
	return buf
}

// StableSize returns binary size of the response body
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *RestoreShardResponse_Body) StableSize() int {
	return 0
}

// SetBody sets response body.
func (x *RestoreShardResponse) SetBody(v *RestoreShardResponse_Body) {
	if x != nil {
		x.Body = v
	}
}

// SetSignature sets response body signature.
func (x *RestoreShardResponse) SetSignature(v *Signature) {
	if x != nil {
		x.Signature = v
	}
}

// ReadSignedData reads signed data from response to buf.
//
// If buffer length is less than SignedDataSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same signed data.
func (x *RestoreShardResponse) ReadSignedData(buf []byte) ([]byte, error) {
	return x.GetBody().StableMarshal(buf), nil
}

// SignedDataSize returns binary size of the signed data.
//
// Structures with the same field values have the same signed data size.
func (x *RestoreShardResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}
