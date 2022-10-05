// Code generated by protoc-gen-go-neofs. DO NOT EDIT.

package control

import "github.com/nspcc-dev/neofs-api-go/v2/util/proto"

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *Signature) StableSize() (size int) {
	size += proto.BytesSize(1, x.Key)
	size += proto.BytesSize(2, x.Sign)
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *Signature) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.BytesMarshal(1, buf[offset:], x.Key)
	offset += proto.BytesMarshal(2, buf[offset:], x.Sign)
	return buf
}

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo_Attribute) StableSize() (size int) {
	size += proto.StringSize(1, x.Key)
	size += proto.StringSize(2, x.Value)
	size += proto.RepeatedStringSize(3, x.Parents)
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo_Attribute) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.StringMarshal(1, buf[offset:], x.Key)
	offset += proto.StringMarshal(2, buf[offset:], x.Value)
	offset += proto.RepeatedStringMarshal(3, buf[offset:], x.Parents)
	return buf
}

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo) StableSize() (size int) {
	size += proto.BytesSize(1, x.PublicKey)
	size += proto.RepeatedStringSize(2, x.Addresses)
	for i := range x.Attributes {
		size += proto.NestedStructureSize(3, x.Attributes[i])
	}
	size += proto.EnumSize(4, int32(x.State))
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.BytesMarshal(1, buf[offset:], x.PublicKey)
	offset += proto.RepeatedStringMarshal(2, buf[offset:], x.Addresses)
	for i := range x.Attributes {
		offset += proto.NestedStructureMarshal(3, buf[offset:], x.Attributes[i])
	}
	offset += proto.EnumMarshal(4, buf[offset:], int32(x.State))
	return buf
}

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *Netmap) StableSize() (size int) {
	size += proto.UInt64Size(1, x.Epoch)
	for i := range x.Nodes {
		size += proto.NestedStructureSize(2, x.Nodes[i])
	}
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *Netmap) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.UInt64Marshal(1, buf[offset:], x.Epoch)
	for i := range x.Nodes {
		offset += proto.NestedStructureMarshal(2, buf[offset:], x.Nodes[i])
	}
	return buf
}

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *ShardInfo) StableSize() (size int) {
	size += proto.BytesSize(1, x.Shard_ID)
	size += proto.StringSize(2, x.MetabasePath)
	for i := range x.Blobstor {
		size += proto.NestedStructureSize(3, x.Blobstor[i])
	}
	size += proto.StringSize(4, x.WritecachePath)
	size += proto.EnumSize(5, int32(x.Mode))
	size += proto.UInt32Size(6, x.ErrorCount)
	size += proto.StringSize(7, x.PiloramaPath)
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *ShardInfo) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.BytesMarshal(1, buf[offset:], x.Shard_ID)
	offset += proto.StringMarshal(2, buf[offset:], x.MetabasePath)
	for i := range x.Blobstor {
		offset += proto.NestedStructureMarshal(3, buf[offset:], x.Blobstor[i])
	}
	offset += proto.StringMarshal(4, buf[offset:], x.WritecachePath)
	offset += proto.EnumMarshal(5, buf[offset:], int32(x.Mode))
	offset += proto.UInt32Marshal(6, buf[offset:], x.ErrorCount)
	offset += proto.StringMarshal(7, buf[offset:], x.PiloramaPath)
	return buf
}

// StableSize returns the size of x in protobuf format.
//
// Structures with the same field values have the same binary size.
func (x *BlobstorInfo) StableSize() (size int) {
	size += proto.StringSize(1, x.Path)
	size += proto.StringSize(2, x.Type)
	return size
}

// StableMarshal marshals x in protobuf binary format with stable field order.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns any error encountered which did not allow writing the data completely.
// Otherwise, returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *BlobstorInfo) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}
	if buf == nil {
		buf = make([]byte, x.StableSize())
	}
	var offset int
	offset += proto.StringMarshal(1, buf[offset:], x.Path)
	offset += proto.StringMarshal(2, buf[offset:], x.Type)
	return buf
}
