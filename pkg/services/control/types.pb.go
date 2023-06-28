// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.21.9
// source: pkg/services/control/types.proto

package control

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Status of the storage node in the NeoFS network map.
type NetmapStatus int32

const (
	// Undefined status, default value.
	NetmapStatus_STATUS_UNDEFINED NetmapStatus = 0
	// Node is online.
	NetmapStatus_ONLINE NetmapStatus = 1
	// Node is offline.
	NetmapStatus_OFFLINE NetmapStatus = 2
	// Node is maintained by the owner.
	NetmapStatus_MAINTENANCE NetmapStatus = 3
)

// Enum value maps for NetmapStatus.
var (
	NetmapStatus_name = map[int32]string{
		0: "STATUS_UNDEFINED",
		1: "ONLINE",
		2: "OFFLINE",
		3: "MAINTENANCE",
	}
	NetmapStatus_value = map[string]int32{
		"STATUS_UNDEFINED": 0,
		"ONLINE":           1,
		"OFFLINE":          2,
		"MAINTENANCE":      3,
	}
)

func (x NetmapStatus) Enum() *NetmapStatus {
	p := new(NetmapStatus)
	*p = x
	return p
}

func (x NetmapStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (NetmapStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_services_control_types_proto_enumTypes[0].Descriptor()
}

func (NetmapStatus) Type() protoreflect.EnumType {
	return &file_pkg_services_control_types_proto_enumTypes[0]
}

func (x NetmapStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use NetmapStatus.Descriptor instead.
func (NetmapStatus) EnumDescriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{0}
}

// Health status of the storage node application.
type HealthStatus int32

const (
	// Undefined status, default value.
	HealthStatus_HEALTH_STATUS_UNDEFINED HealthStatus = 0
	// Storage node application is starting.
	HealthStatus_STARTING HealthStatus = 1
	// Storage node application is started and serves all services.
	HealthStatus_READY HealthStatus = 2
	// Storage node application is shutting down.
	HealthStatus_SHUTTING_DOWN HealthStatus = 3
)

// Enum value maps for HealthStatus.
var (
	HealthStatus_name = map[int32]string{
		0: "HEALTH_STATUS_UNDEFINED",
		1: "STARTING",
		2: "READY",
		3: "SHUTTING_DOWN",
	}
	HealthStatus_value = map[string]int32{
		"HEALTH_STATUS_UNDEFINED": 0,
		"STARTING":                1,
		"READY":                   2,
		"SHUTTING_DOWN":           3,
	}
)

func (x HealthStatus) Enum() *HealthStatus {
	p := new(HealthStatus)
	*p = x
	return p
}

func (x HealthStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (HealthStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_services_control_types_proto_enumTypes[1].Descriptor()
}

func (HealthStatus) Type() protoreflect.EnumType {
	return &file_pkg_services_control_types_proto_enumTypes[1]
}

func (x HealthStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use HealthStatus.Descriptor instead.
func (HealthStatus) EnumDescriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{1}
}

// Work mode of the shard.
type ShardMode int32

const (
	// Undefined mode, default value.
	ShardMode_SHARD_MODE_UNDEFINED ShardMode = 0
	// Read-write.
	ShardMode_READ_WRITE ShardMode = 1
	// Read-only.
	ShardMode_READ_ONLY ShardMode = 2
	// Degraded.
	ShardMode_DEGRADED ShardMode = 3
	// DegradedReadOnly.
	ShardMode_DEGRADED_READ_ONLY ShardMode = 4
)

// Enum value maps for ShardMode.
var (
	ShardMode_name = map[int32]string{
		0: "SHARD_MODE_UNDEFINED",
		1: "READ_WRITE",
		2: "READ_ONLY",
		3: "DEGRADED",
		4: "DEGRADED_READ_ONLY",
	}
	ShardMode_value = map[string]int32{
		"SHARD_MODE_UNDEFINED": 0,
		"READ_WRITE":           1,
		"READ_ONLY":            2,
		"DEGRADED":             3,
		"DEGRADED_READ_ONLY":   4,
	}
)

func (x ShardMode) Enum() *ShardMode {
	p := new(ShardMode)
	*p = x
	return p
}

func (x ShardMode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ShardMode) Descriptor() protoreflect.EnumDescriptor {
	return file_pkg_services_control_types_proto_enumTypes[2].Descriptor()
}

func (ShardMode) Type() protoreflect.EnumType {
	return &file_pkg_services_control_types_proto_enumTypes[2]
}

func (x ShardMode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ShardMode.Descriptor instead.
func (ShardMode) EnumDescriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{2}
}

// Signature of some message.
type Signature struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Public key used for signing.
	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// Binary signature.
	Sign []byte `protobuf:"bytes,2,opt,name=sign,json=signature,proto3" json:"sign,omitempty"`
}

func (x *Signature) Reset() {
	*x = Signature{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Signature) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Signature) ProtoMessage() {}

func (x *Signature) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Signature.ProtoReflect.Descriptor instead.
func (*Signature) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{0}
}

func (x *Signature) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Signature) GetSign() []byte {
	if x != nil {
		return x.Sign
	}
	return nil
}

// NeoFS node description.
type NodeInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Public key of the NeoFS node in a binary format.
	PublicKey []byte `protobuf:"bytes,1,opt,name=public_key,json=publicKey,proto3" json:"public_key,omitempty"`
	// Ways to connect to a node.
	Addresses []string `protobuf:"bytes,2,rep,name=addresses,proto3" json:"addresses,omitempty"`
	// Carries list of the NeoFS node attributes in a key-value form. Key name
	// must be a node-unique valid UTF-8 string. Value can't be empty. NodeInfo
	// structures with duplicated attribute names or attributes with empty values
	// will be considered invalid.
	Attributes []*NodeInfo_Attribute `protobuf:"bytes,3,rep,name=attributes,proto3" json:"attributes,omitempty"`
	// Carries state of the NeoFS node.
	State NetmapStatus `protobuf:"varint,4,opt,name=state,proto3,enum=control.NetmapStatus" json:"state,omitempty"`
}

func (x *NodeInfo) Reset() {
	*x = NodeInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeInfo) ProtoMessage() {}

func (x *NodeInfo) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeInfo.ProtoReflect.Descriptor instead.
func (*NodeInfo) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{1}
}

func (x *NodeInfo) GetPublicKey() []byte {
	if x != nil {
		return x.PublicKey
	}
	return nil
}

func (x *NodeInfo) GetAddresses() []string {
	if x != nil {
		return x.Addresses
	}
	return nil
}

func (x *NodeInfo) GetAttributes() []*NodeInfo_Attribute {
	if x != nil {
		return x.Attributes
	}
	return nil
}

func (x *NodeInfo) GetState() NetmapStatus {
	if x != nil {
		return x.State
	}
	return NetmapStatus_STATUS_UNDEFINED
}

// Network map structure.
type Netmap struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Network map revision number.
	Epoch uint64 `protobuf:"varint,1,opt,name=epoch,proto3" json:"epoch,omitempty"`
	// Nodes presented in network.
	Nodes []*NodeInfo `protobuf:"bytes,2,rep,name=nodes,proto3" json:"nodes,omitempty"`
}

func (x *Netmap) Reset() {
	*x = Netmap{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Netmap) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Netmap) ProtoMessage() {}

func (x *Netmap) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Netmap.ProtoReflect.Descriptor instead.
func (*Netmap) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{2}
}

func (x *Netmap) GetEpoch() uint64 {
	if x != nil {
		return x.Epoch
	}
	return 0
}

func (x *Netmap) GetNodes() []*NodeInfo {
	if x != nil {
		return x.Nodes
	}
	return nil
}

// Shard description.
type ShardInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// ID of the shard.
	Shard_ID []byte `protobuf:"bytes,1,opt,name=shard_ID,json=shardID,proto3" json:"shard_ID,omitempty"`
	// Path to shard's metabase.
	MetabasePath string `protobuf:"bytes,2,opt,name=metabase_path,json=metabasePath,proto3" json:"metabase_path,omitempty"`
	// Shard's blobstor info.
	Blobstor []*BlobstorInfo `protobuf:"bytes,3,rep,name=blobstor,proto3" json:"blobstor,omitempty"`
	// Path to shard's write-cache, empty if disabled.
	WritecachePath string `protobuf:"bytes,4,opt,name=writecache_path,json=writecachePath,proto3" json:"writecache_path,omitempty"`
	// Work mode of the shard.
	Mode ShardMode `protobuf:"varint,5,opt,name=mode,proto3,enum=control.ShardMode" json:"mode,omitempty"`
	// Amount of errors occured.
	ErrorCount uint32 `protobuf:"varint,6,opt,name=errorCount,proto3" json:"errorCount,omitempty"`
	// Path to shard's pilorama storage.
	PiloramaPath string `protobuf:"bytes,7,opt,name=pilorama_path,json=piloramaPath,proto3" json:"pilorama_path,omitempty"`
}

func (x *ShardInfo) Reset() {
	*x = ShardInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ShardInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ShardInfo) ProtoMessage() {}

func (x *ShardInfo) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ShardInfo.ProtoReflect.Descriptor instead.
func (*ShardInfo) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{3}
}

func (x *ShardInfo) GetShard_ID() []byte {
	if x != nil {
		return x.Shard_ID
	}
	return nil
}

func (x *ShardInfo) GetMetabasePath() string {
	if x != nil {
		return x.MetabasePath
	}
	return ""
}

func (x *ShardInfo) GetBlobstor() []*BlobstorInfo {
	if x != nil {
		return x.Blobstor
	}
	return nil
}

func (x *ShardInfo) GetWritecachePath() string {
	if x != nil {
		return x.WritecachePath
	}
	return ""
}

func (x *ShardInfo) GetMode() ShardMode {
	if x != nil {
		return x.Mode
	}
	return ShardMode_SHARD_MODE_UNDEFINED
}

func (x *ShardInfo) GetErrorCount() uint32 {
	if x != nil {
		return x.ErrorCount
	}
	return 0
}

func (x *ShardInfo) GetPiloramaPath() string {
	if x != nil {
		return x.PiloramaPath
	}
	return ""
}

// Blobstor component description.
type BlobstorInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Path to the root.
	Path string `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	// Component type.
	Type string `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
}

func (x *BlobstorInfo) Reset() {
	*x = BlobstorInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BlobstorInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BlobstorInfo) ProtoMessage() {}

func (x *BlobstorInfo) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BlobstorInfo.ProtoReflect.Descriptor instead.
func (*BlobstorInfo) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{4}
}

func (x *BlobstorInfo) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *BlobstorInfo) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

// Administrator-defined Attributes of the NeoFS Storage Node.
//
// `Attribute` is a Key-Value metadata pair. Key name must be a valid UTF-8
// string. Value can't be empty.
//
// Node's attributes are mostly used during Storage Policy evaluation to
// calculate object's placement and find a set of nodes satisfying policy
// requirements. There are some "well-known" node attributes common to all the
// Storage Nodes in the network and used implicitly with default values if not
// explicitly set:
//
//   - Capacity \
//     Total available disk space in Gigabytes.
//   - Price \
//     Price in GAS tokens for storing one GB of data during one Epoch. In node
//     attributes it's a string presenting floating point number with comma or
//     point delimiter for decimal part. In the Network Map it will be saved as
//     64-bit unsigned integer representing number of minimal token fractions.
//   - Locode \
//     Node's geographic location in
//     [UN/LOCODE](https://www.unece.org/cefact/codesfortrade/codes_index.html)
//     format approximated to the nearest point defined in standard.
//   - Country \
//     Country code in
//     [ISO 3166-1_alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)
//     format. Calculated automatically from `Locode` attribute
//   - Region \
//     Country's administative subdivision where node is located. Calculated
//     automatically from `Locode` attribute based on `SubDiv` field. Presented
//     in [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) format.
//   - City \
//     City, town, village or rural area name where node is located written
//     without diacritics . Calculated automatically from `Locode` attribute.
//
// For detailed description of each well-known attribute please see the
// corresponding section in NeoFS Technical specification.
type NodeInfo_Attribute struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Key of the node attribute.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// Value of the node attribute.
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	// Parent keys, if any. For example for `City` it could be `Region` and
	// `Country`.
	Parents []string `protobuf:"bytes,3,rep,name=parents,proto3" json:"parents,omitempty"`
}

func (x *NodeInfo_Attribute) Reset() {
	*x = NodeInfo_Attribute{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_types_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeInfo_Attribute) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeInfo_Attribute) ProtoMessage() {}

func (x *NodeInfo_Attribute) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_types_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeInfo_Attribute.ProtoReflect.Descriptor instead.
func (*NodeInfo_Attribute) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{1, 0}
}

func (x *NodeInfo_Attribute) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *NodeInfo_Attribute) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *NodeInfo_Attribute) GetParents() []string {
	if x != nil {
		return x.Parents
	}
	return nil
}

var File_pkg_services_control_types_proto protoreflect.FileDescriptor

var file_pkg_services_control_types_proto_rawDesc = []byte{
	0x0a, 0x20, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x63,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x22, 0x36, 0x0a, 0x09, 0x53,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x17, 0x0a, 0x04, 0x73, 0x69,
	0x67, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74,
	0x75, 0x72, 0x65, 0x22, 0x80, 0x02, 0x0a, 0x08, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x6e, 0x66, 0x6f,
	0x12, 0x1d, 0x0a, 0x0a, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x4b, 0x65, 0x79, 0x12,
	0x1c, 0x0a, 0x09, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x09, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x65, 0x73, 0x12, 0x3b, 0x0a,
	0x0a, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x1b, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x6f, 0x64, 0x65,
	0x49, 0x6e, 0x66, 0x6f, 0x2e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x52, 0x0a,
	0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x2b, 0x0a, 0x05, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x15, 0x2e, 0x63, 0x6f, 0x6e, 0x74,
	0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x1a, 0x4d, 0x0a, 0x09, 0x41, 0x74, 0x74, 0x72, 0x69,
	0x62, 0x75, 0x74, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18, 0x0a, 0x07,
	0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x70,
	0x61, 0x72, 0x65, 0x6e, 0x74, 0x73, 0x22, 0x47, 0x0a, 0x06, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70,
	0x12, 0x14, 0x0a, 0x05, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x05, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x12, 0x27, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e,
	0x4e, 0x6f, 0x64, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x22,
	0x94, 0x02, 0x0a, 0x09, 0x53, 0x68, 0x61, 0x72, 0x64, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x19, 0x0a,
	0x08, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x12, 0x23, 0x0a, 0x0d, 0x6d, 0x65, 0x74, 0x61,
	0x62, 0x61, 0x73, 0x65, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0c, 0x6d, 0x65, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x50, 0x61, 0x74, 0x68, 0x12, 0x31, 0x0a,
	0x08, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x74, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x15, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x42, 0x6c, 0x6f, 0x62, 0x73, 0x74,
	0x6f, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x08, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x74, 0x6f, 0x72,
	0x12, 0x27, 0x0a, 0x0f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x63, 0x61, 0x63, 0x68, 0x65, 0x5f, 0x70,
	0x61, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x77, 0x72, 0x69, 0x74, 0x65,
	0x63, 0x61, 0x63, 0x68, 0x65, 0x50, 0x61, 0x74, 0x68, 0x12, 0x26, 0x0a, 0x04, 0x6d, 0x6f, 0x64,
	0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f,
	0x6c, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x4d, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x6d, 0x6f, 0x64,
	0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0a, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x75, 0x6e,
	0x74, 0x12, 0x23, 0x0a, 0x0d, 0x70, 0x69, 0x6c, 0x6f, 0x72, 0x61, 0x6d, 0x61, 0x5f, 0x70, 0x61,
	0x74, 0x68, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x70, 0x69, 0x6c, 0x6f, 0x72, 0x61,
	0x6d, 0x61, 0x50, 0x61, 0x74, 0x68, 0x22, 0x36, 0x0a, 0x0c, 0x42, 0x6c, 0x6f, 0x62, 0x73, 0x74,
	0x6f, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79,
	0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x2a, 0x4e,
	0x0a, 0x0c, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x14,
	0x0a, 0x10, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e, 0x44, 0x45, 0x46, 0x49, 0x4e,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x4f, 0x4e, 0x4c, 0x49, 0x4e, 0x45, 0x10, 0x01,
	0x12, 0x0b, 0x0a, 0x07, 0x4f, 0x46, 0x46, 0x4c, 0x49, 0x4e, 0x45, 0x10, 0x02, 0x12, 0x0f, 0x0a,
	0x0b, 0x4d, 0x41, 0x49, 0x4e, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x43, 0x45, 0x10, 0x03, 0x2a, 0x57,
	0x0a, 0x0c, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x1b,
	0x0a, 0x17, 0x48, 0x45, 0x41, 0x4c, 0x54, 0x48, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
	0x55, 0x4e, 0x44, 0x45, 0x46, 0x49, 0x4e, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x53,
	0x54, 0x41, 0x52, 0x54, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x52, 0x45, 0x41,
	0x44, 0x59, 0x10, 0x02, 0x12, 0x11, 0x0a, 0x0d, 0x53, 0x48, 0x55, 0x54, 0x54, 0x49, 0x4e, 0x47,
	0x5f, 0x44, 0x4f, 0x57, 0x4e, 0x10, 0x03, 0x2a, 0x6a, 0x0a, 0x09, 0x53, 0x68, 0x61, 0x72, 0x64,
	0x4d, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x14, 0x53, 0x48, 0x41, 0x52, 0x44, 0x5f, 0x4d, 0x4f,
	0x44, 0x45, 0x5f, 0x55, 0x4e, 0x44, 0x45, 0x46, 0x49, 0x4e, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0e,
	0x0a, 0x0a, 0x52, 0x45, 0x41, 0x44, 0x5f, 0x57, 0x52, 0x49, 0x54, 0x45, 0x10, 0x01, 0x12, 0x0d,
	0x0a, 0x09, 0x52, 0x45, 0x41, 0x44, 0x5f, 0x4f, 0x4e, 0x4c, 0x59, 0x10, 0x02, 0x12, 0x0c, 0x0a,
	0x08, 0x44, 0x45, 0x47, 0x52, 0x41, 0x44, 0x45, 0x44, 0x10, 0x03, 0x12, 0x16, 0x0a, 0x12, 0x44,
	0x45, 0x47, 0x52, 0x41, 0x44, 0x45, 0x44, 0x5f, 0x52, 0x45, 0x41, 0x44, 0x5f, 0x4f, 0x4e, 0x4c,
	0x59, 0x10, 0x04, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6e, 0x73, 0x70, 0x63, 0x63, 0x2d, 0x64, 0x65, 0x76, 0x2f, 0x6e, 0x65, 0x6f, 0x66,
	0x73, 0x2d, 0x6e, 0x6f, 0x64, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x73, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pkg_services_control_types_proto_rawDescOnce sync.Once
	file_pkg_services_control_types_proto_rawDescData = file_pkg_services_control_types_proto_rawDesc
)

func file_pkg_services_control_types_proto_rawDescGZIP() []byte {
	file_pkg_services_control_types_proto_rawDescOnce.Do(func() {
		file_pkg_services_control_types_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_services_control_types_proto_rawDescData)
	})
	return file_pkg_services_control_types_proto_rawDescData
}

var file_pkg_services_control_types_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_pkg_services_control_types_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_pkg_services_control_types_proto_goTypes = []interface{}{
	(NetmapStatus)(0),          // 0: control.NetmapStatus
	(HealthStatus)(0),          // 1: control.HealthStatus
	(ShardMode)(0),             // 2: control.ShardMode
	(*Signature)(nil),          // 3: control.Signature
	(*NodeInfo)(nil),           // 4: control.NodeInfo
	(*Netmap)(nil),             // 5: control.Netmap
	(*ShardInfo)(nil),          // 6: control.ShardInfo
	(*BlobstorInfo)(nil),       // 7: control.BlobstorInfo
	(*NodeInfo_Attribute)(nil), // 8: control.NodeInfo.Attribute
}
var file_pkg_services_control_types_proto_depIdxs = []int32{
	8, // 0: control.NodeInfo.attributes:type_name -> control.NodeInfo.Attribute
	0, // 1: control.NodeInfo.state:type_name -> control.NetmapStatus
	4, // 2: control.Netmap.nodes:type_name -> control.NodeInfo
	7, // 3: control.ShardInfo.blobstor:type_name -> control.BlobstorInfo
	2, // 4: control.ShardInfo.mode:type_name -> control.ShardMode
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_pkg_services_control_types_proto_init() }
func file_pkg_services_control_types_proto_init() {
	if File_pkg_services_control_types_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_services_control_types_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Signature); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_services_control_types_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NodeInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_services_control_types_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Netmap); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_services_control_types_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ShardInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_services_control_types_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BlobstorInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_services_control_types_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NodeInfo_Attribute); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_services_control_types_proto_rawDesc,
			NumEnums:      3,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_services_control_types_proto_goTypes,
		DependencyIndexes: file_pkg_services_control_types_proto_depIdxs,
		EnumInfos:         file_pkg_services_control_types_proto_enumTypes,
		MessageInfos:      file_pkg_services_control_types_proto_msgTypes,
	}.Build()
	File_pkg_services_control_types_proto = out.File
	file_pkg_services_control_types_proto_rawDesc = nil
	file_pkg_services_control_types_proto_goTypes = nil
	file_pkg_services_control_types_proto_depIdxs = nil
}
