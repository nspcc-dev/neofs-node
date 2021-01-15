// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.14.0
// source: pkg/services/control/types.proto

package control

import (
	proto "github.com/golang/protobuf/proto"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

// Health status of the storage node.
type HealthStatus int32

const (
	// Undefined status, default value.
	HealthStatus_STATUS_UNDEFINED HealthStatus = 0
	// Node is online.
	HealthStatus_ONLINE HealthStatus = 1
	// Node is offline.
	HealthStatus_OFFLINE HealthStatus = 2
)

// Enum value maps for HealthStatus.
var (
	HealthStatus_name = map[int32]string{
		0: "STATUS_UNDEFINED",
		1: "ONLINE",
		2: "OFFLINE",
	}
	HealthStatus_value = map[string]int32{
		"STATUS_UNDEFINED": 0,
		"ONLINE":           1,
		"OFFLINE":          2,
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
	return file_pkg_services_control_types_proto_enumTypes[0].Descriptor()
}

func (HealthStatus) Type() protoreflect.EnumType {
	return &file_pkg_services_control_types_proto_enumTypes[0]
}

func (x HealthStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use HealthStatus.Descriptor instead.
func (HealthStatus) EnumDescriptor() ([]byte, []int) {
	return file_pkg_services_control_types_proto_rawDescGZIP(), []int{0}
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
	Address string `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	// Carries list of the NeoFS node attributes in a key-value form. Key name
	// must be a node-unique valid UTF-8 string. Value can't be empty. NodeInfo
	// structures with duplicated attribute names or attributes with empty values
	// will be considered invalid.
	Attributes []*NodeInfo_Attribute `protobuf:"bytes,3,rep,name=attributes,proto3" json:"attributes,omitempty"`
	// Carries state of the NeoFS node.
	State HealthStatus `protobuf:"varint,4,opt,name=state,proto3,enum=control.HealthStatus" json:"state,omitempty"`
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

func (x *NodeInfo) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *NodeInfo) GetAttributes() []*NodeInfo_Attribute {
	if x != nil {
		return x.Attributes
	}
	return nil
}

func (x *NodeInfo) GetState() HealthStatus {
	if x != nil {
		return x.State
	}
	return HealthStatus_STATUS_UNDEFINED
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
// * Capacity \
//   Total available disk space in Gigabytes.
// * Price \
//   Price in GAS tokens for storing one GB of data during one Epoch. In node
//   attributes it's a string presenting floating point number with comma or
//   point delimiter for decimal part. In the Network Map it will be saved as
//   64-bit unsigned integer representing number of minimal token fractions.
// * Subnet \
//   String ID of Node's storage subnet. There can be only one subnet served
//   by the Storage Node.
// * Locode \
//   Node's geographic location in
//   [UN/LOCODE](https://www.unece.org/cefact/codesfortrade/codes_index.html)
//   format approximated to the nearest point defined in standard.
// * Country \
//   Country code in
//   [ISO 3166-1_alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)
//   format. Calculated automatically from `Locode` attribute
// * Region \
//   Country's administative subdivision where node is located. Calculated
//   automatically from `Locode` attribute based on `SubDiv` field. Presented
//   in [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) format.
// * City \
//   City, town, village or rural area name where node is located written
//   without diacritics . Calculated automatically from `Locode` attribute.
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
		mi := &file_pkg_services_control_types_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeInfo_Attribute) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeInfo_Attribute) ProtoMessage() {}

func (x *NodeInfo_Attribute) ProtoReflect() protoreflect.Message {
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
	0x75, 0x72, 0x65, 0x22, 0xfc, 0x01, 0x0a, 0x08, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x6e, 0x66, 0x6f,
	0x12, 0x1d, 0x0a, 0x0a, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x4b, 0x65, 0x79, 0x12,
	0x18, 0x0a, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x3b, 0x0a, 0x0a, 0x61, 0x74, 0x74,
	0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e,
	0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x6e, 0x66, 0x6f,
	0x2e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x52, 0x0a, 0x61, 0x74, 0x74, 0x72,
	0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x2b, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x15, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e,
	0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x05, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x1a, 0x4d, 0x0a, 0x09, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x61, 0x72, 0x65,
	0x6e, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x70, 0x61, 0x72, 0x65, 0x6e,
	0x74, 0x73, 0x22, 0x47, 0x0a, 0x06, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x12, 0x14, 0x0a, 0x05,
	0x65, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x65, 0x70, 0x6f,
	0x63, 0x68, 0x12, 0x27, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x6f, 0x64, 0x65,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x2a, 0x3d, 0x0a, 0x0c, 0x48,
	0x65, 0x61, 0x6c, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x14, 0x0a, 0x10, 0x53,
	0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e, 0x44, 0x45, 0x46, 0x49, 0x4e, 0x45, 0x44, 0x10,
	0x00, 0x12, 0x0a, 0x0a, 0x06, 0x4f, 0x4e, 0x4c, 0x49, 0x4e, 0x45, 0x10, 0x01, 0x12, 0x0b, 0x0a,
	0x07, 0x4f, 0x46, 0x46, 0x4c, 0x49, 0x4e, 0x45, 0x10, 0x02, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x73, 0x70, 0x63, 0x63, 0x2d, 0x64,
	0x65, 0x76, 0x2f, 0x6e, 0x65, 0x6f, 0x66, 0x73, 0x2d, 0x6e, 0x6f, 0x64, 0x65, 0x2f, 0x70, 0x6b,
	0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
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

var file_pkg_services_control_types_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_pkg_services_control_types_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_services_control_types_proto_goTypes = []interface{}{
	(HealthStatus)(0),          // 0: control.HealthStatus
	(*Signature)(nil),          // 1: control.Signature
	(*NodeInfo)(nil),           // 2: control.NodeInfo
	(*Netmap)(nil),             // 3: control.Netmap
	(*NodeInfo_Attribute)(nil), // 4: control.NodeInfo.Attribute
}
var file_pkg_services_control_types_proto_depIdxs = []int32{
	4, // 0: control.NodeInfo.attributes:type_name -> control.NodeInfo.Attribute
	0, // 1: control.NodeInfo.state:type_name -> control.HealthStatus
	2, // 2: control.Netmap.nodes:type_name -> control.NodeInfo
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
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
			NumEnums:      1,
			NumMessages:   4,
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
