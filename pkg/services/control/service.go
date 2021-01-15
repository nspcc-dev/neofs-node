package control

import (
	"github.com/nspcc-dev/neofs-api-go/util/proto"
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
func (x *HealthCheckRequest_Body) StableMarshal(buf []byte) ([]byte, error) {
	return buf, nil
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
	return x.GetBody().StableMarshal(buf)
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

const (
	_ = iota
	healthRespBodyStatusFNum
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
func (x *HealthCheckResponse_Body) StableMarshal(buf []byte) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	_, err := proto.EnumMarshal(healthRespBodyStatusFNum, buf, int32(x.NetmapStatus))
	if err != nil {
		return nil, err
	}

	return buf, nil
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
	return x.GetBody().StableMarshal(buf)
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
func (x *NetmapSnapshotRequest_Body) StableMarshal(buf []byte) ([]byte, error) {
	return buf, nil
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
	return x.GetBody().StableMarshal(buf)
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
func (x *NetmapSnapshotResponse_Body) StableMarshal(buf []byte) ([]byte, error) {
	if x == nil {
		return []byte{}, nil
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	_, err := proto.NestedStructureMarshal(snapshotRespBodyNetmapFNum, buf, x.Netmap)
	if err != nil {
		return nil, err
	}

	return buf, nil
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
	return x.GetBody().StableMarshal(buf)
}

// SignedDataSize returns binary size of the signed data
// of netmap snapshot response.
//
// Structures with the same field values have the same signed data size.
func (x *NetmapSnapshotResponse) SignedDataSize() int {
	return x.GetBody().StableSize()
}
