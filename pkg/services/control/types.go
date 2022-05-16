package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/util/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// SetKey sets public key used for signing.
func (x *Signature) SetKey(v []byte) {
	if x != nil {
		x.Key = v
	}
}

// SetSign sets binary signature.
func (x *Signature) SetSign(v []byte) {
	if x != nil {
		x.Sign = v
	}
}

// SetKey sets key of the node attribute.
func (x *NodeInfo_Attribute) SetKey(v string) {
	if x != nil {
		x.Key = v
	}
}

// SetValue sets value of the node attribute.
func (x *NodeInfo_Attribute) SetValue(v string) {
	if x != nil {
		x.Value = v
	}
}

// SetParents sets parent keys.
func (x *NodeInfo_Attribute) SetParents(v []string) {
	if x != nil {
		x.Parents = v
	}
}

const (
	_ = iota
	nodeAttrKeyFNum
	nodeAttrValueFNum
	nodeAttrParentsFNum
)

// StableMarshal reads binary representation of node attribute
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo_Attribute) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	offset := proto.StringMarshal(nodeAttrKeyFNum, buf, x.Key)
	offset += proto.StringMarshal(nodeAttrValueFNum, buf[offset:], x.Value)

	for i := range x.Parents {
		offset += proto.StringMarshal(nodeAttrParentsFNum, buf[offset:], x.Parents[i])
	}

	return buf
}

// StableSize returns binary size of node attribute
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo_Attribute) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.StringSize(nodeAttrKeyFNum, x.Key)
	size += proto.StringSize(nodeAttrValueFNum, x.Value)

	parents := x.GetParents()
	for i := range parents {
		size += proto.StringSize(nodeAttrParentsFNum, parents[i])
	}

	return size
}

// SetPublicKey sets public key of the NeoFS node in a binary format.
func (x *NodeInfo) SetPublicKey(v []byte) {
	if x != nil {
		x.PublicKey = v
	}
}

// SetAddresses sets ways to connect to a node.
func (x *NodeInfo) SetAddresses(v []string) {
	if x != nil {
		x.Addresses = v
	}
}

// SetAttributes sets attributes of the NeoFS Storage Node.
func (x *NodeInfo) SetAttributes(v []*NodeInfo_Attribute) {
	if x != nil {
		x.Attributes = v
	}
}

// SetState sets state of the NeoFS node.
func (x *NodeInfo) SetState(v NetmapStatus) {
	if x != nil {
		x.State = v
	}
}

const (
	_ = iota
	nodePubKeyFNum
	nodeAddrFNum
	nodeAttrsFNum
	nodeStateFNum
)

// StableMarshal reads binary representation of node information
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *NodeInfo) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	offset := proto.BytesMarshal(nodePubKeyFNum, buf, x.PublicKey)
	offset += proto.RepeatedStringMarshal(nodeAddrFNum, buf[offset:], x.Addresses)

	for i := range x.Attributes {
		offset += proto.NestedStructureMarshal(nodeAttrsFNum, buf[offset:], x.Attributes[i])
	}

	proto.EnumMarshal(nodeStateFNum, buf[offset:], int32(x.State))

	return buf
}

// StableSize returns binary size of node information
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *NodeInfo) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(nodePubKeyFNum, x.PublicKey)
	size += proto.RepeatedStringSize(nodeAddrFNum, x.Addresses)

	for i := range x.Attributes {
		size += proto.NestedStructureSize(nodeAttrsFNum, x.Attributes[i])
	}

	size += proto.EnumSize(nodeStateFNum, int32(x.State))

	return size
}

// SetEpoch sets revision number of the network map.
func (x *Netmap) SetEpoch(v uint64) {
	if x != nil {
		x.Epoch = v
	}
}

// SetNodes sets nodes presented in network.
func (x *Netmap) SetNodes(v []*NodeInfo) {
	if x != nil {
		x.Nodes = v
	}
}

const (
	_ = iota
	netmapEpochFNum
	netmapNodesFNum
)

// StableMarshal reads binary representation of netmap  in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *Netmap) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	offset := proto.UInt64Marshal(netmapEpochFNum, buf, x.Epoch)

	for i := range x.Nodes {
		offset += proto.NestedStructureMarshal(netmapNodesFNum, buf[offset:], x.Nodes[i])
	}

	return buf
}

// StableSize returns binary size of netmap  in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *Netmap) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.UInt64Size(netmapEpochFNum, x.Epoch)

	for i := range x.Nodes {
		size += proto.NestedStructureSize(netmapNodesFNum, x.Nodes[i])
	}

	return size
}

func (x *Netmap) MarshalJSON() ([]byte, error) {
	return protojson.MarshalOptions{
		EmitUnpopulated: true,
	}.Marshal(x)
}

// SetID sets identificator of the shard.
func (x *ShardInfo) SetID(v []byte) {
	x.Shard_ID = v
}

// SetMetabasePath sets path to shard's metabase.
func (x *ShardInfo) SetMetabasePath(v string) {
	x.MetabasePath = v
}

// SetBlobstorPath sets path to shard's blobstor.
func (x *ShardInfo) SetBlobstorPath(v string) {
	x.BlobstorPath = v
}

// SetWriteCachePath sets path to shard's write-cache.
func (x *ShardInfo) SetWriteCachePath(v string) {
	x.WritecachePath = v
}

// SetPiloramaPath sets path to shard's pilorama.
func (x *ShardInfo) SetPiloramaPath(v string) {
	x.PiloramaPath = v
}

// SetMode sets path to shard's work mode.
func (x *ShardInfo) SetMode(v ShardMode) {
	x.Mode = v
}

// SetErrorCount sets shard's error counter.
func (x *ShardInfo) SetErrorCount(count uint32) {
	x.ErrorCount = count
}

const (
	_ = iota
	shardInfoIDFNum
	shardInfoMetabaseFNum
	shardInfoBlobstorFNum
	shardInfoWriteCacheFNum
	shardInfoModeFNum
	shardInfoErrorCountFNum
	shardInfoPiloramaFNum
)

// StableSize returns binary size of shard information
// in protobuf binary format.
//
// Structures with the same field values have the same binary size.
func (x *ShardInfo) StableSize() int {
	if x == nil {
		return 0
	}

	size := 0

	size += proto.BytesSize(shardInfoIDFNum, x.Shard_ID)
	size += proto.StringSize(shardInfoMetabaseFNum, x.MetabasePath)
	size += proto.StringSize(shardInfoBlobstorFNum, x.BlobstorPath)
	size += proto.StringSize(shardInfoWriteCacheFNum, x.WritecachePath)
	size += proto.EnumSize(shardInfoModeFNum, int32(x.Mode))
	size += proto.UInt32Size(shardInfoErrorCountFNum, x.ErrorCount)
	size += proto.StringSize(shardInfoPiloramaFNum, x.PiloramaPath)

	return size
}

// StableMarshal reads binary representation of shard information
// in protobuf binary format.
//
// If buffer length is less than x.StableSize(), new buffer is allocated.
//
// Returns the buffer in which the data is written.
//
// Structures with the same field values have the same binary format.
func (x *ShardInfo) StableMarshal(buf []byte) []byte {
	if x == nil {
		return []byte{}
	}

	if sz := x.StableSize(); len(buf) < sz {
		buf = make([]byte, sz)
	}

	offset := proto.BytesMarshal(shardInfoIDFNum, buf, x.Shard_ID)
	offset += proto.StringMarshal(shardInfoMetabaseFNum, buf[offset:], x.MetabasePath)
	offset += proto.StringMarshal(shardInfoBlobstorFNum, buf[offset:], x.BlobstorPath)
	offset += proto.StringMarshal(shardInfoWriteCacheFNum, buf[offset:], x.WritecachePath)
	offset += proto.EnumMarshal(shardInfoModeFNum, buf[offset:], int32(x.Mode))
	offset += proto.EnumMarshal(shardInfoErrorCountFNum, buf[offset:], int32(x.ErrorCount))
	proto.StringMarshal(shardInfoPiloramaFNum, buf[offset:], x.PiloramaPath)

	return buf
}
