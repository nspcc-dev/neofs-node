// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.20.1
// source: pkg/services/tree/types.proto

package tree

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

// KeyValue represents key-value pair attached to an object.
type KeyValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *KeyValue) Reset() {
	*x = KeyValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_tree_types_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValue) ProtoMessage() {}

func (x *KeyValue) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_tree_types_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyValue.ProtoReflect.Descriptor instead.
func (*KeyValue) Descriptor() ([]byte, []int) {
	return file_pkg_services_tree_types_proto_rawDescGZIP(), []int{0}
}

func (x *KeyValue) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KeyValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

// LogMove represents log-entry for a single move operation.
type LogMove struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// ID of the parent node.
	ParentId uint64 `protobuf:"varint,1,opt,name=parent_id,json=parentID,proto3" json:"parent_id,omitempty"`
	// Node meta information, including operation timestamp.
	Meta []byte `protobuf:"bytes,2,opt,name=meta,proto3" json:"meta,omitempty"`
	// ID of the node to move.
	ChildId uint64 `protobuf:"varint,3,opt,name=child_id,json=childID,proto3" json:"child_id,omitempty"`
}

func (x *LogMove) Reset() {
	*x = LogMove{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_tree_types_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogMove) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogMove) ProtoMessage() {}

func (x *LogMove) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_tree_types_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogMove.ProtoReflect.Descriptor instead.
func (*LogMove) Descriptor() ([]byte, []int) {
	return file_pkg_services_tree_types_proto_rawDescGZIP(), []int{1}
}

func (x *LogMove) GetParentId() uint64 {
	if x != nil {
		return x.ParentId
	}
	return 0
}

func (x *LogMove) GetMeta() []byte {
	if x != nil {
		return x.Meta
	}
	return nil
}

func (x *LogMove) GetChildId() uint64 {
	if x != nil {
		return x.ChildId
	}
	return 0
}

// Signature of a message.
type Signature struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key  []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Sign []byte `protobuf:"bytes,2,opt,name=sign,json=signature,proto3" json:"sign,omitempty"`
}

func (x *Signature) Reset() {
	*x = Signature{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_tree_types_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Signature) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Signature) ProtoMessage() {}

func (x *Signature) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_tree_types_proto_msgTypes[2]
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
	return file_pkg_services_tree_types_proto_rawDescGZIP(), []int{2}
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

var File_pkg_services_tree_types_proto protoreflect.FileDescriptor

var file_pkg_services_tree_types_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x74,
	0x72, 0x65, 0x65, 0x2f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x04, 0x74, 0x72, 0x65, 0x65, 0x22, 0x32, 0x0a, 0x08, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x55, 0x0a, 0x07, 0x4c, 0x6f, 0x67,
	0x4d, 0x6f, 0x76, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x49,
	0x44, 0x12, 0x12, 0x0a, 0x04, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x6d, 0x65, 0x74, 0x61, 0x12, 0x19, 0x0a, 0x08, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x5f, 0x69,
	0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x63, 0x68, 0x69, 0x6c, 0x64, 0x49, 0x44,
	0x22, 0x36, 0x0a, 0x09, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12,
	0x17, 0x0a, 0x04, 0x73, 0x69, 0x67, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x73,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x42, 0x33, 0x5a, 0x31, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x73, 0x70, 0x63, 0x63, 0x2d, 0x64, 0x65, 0x76,
	0x2f, 0x6e, 0x65, 0x6f, 0x66, 0x73, 0x2d, 0x6e, 0x6f, 0x64, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x74, 0x72, 0x65, 0x65, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_services_tree_types_proto_rawDescOnce sync.Once
	file_pkg_services_tree_types_proto_rawDescData = file_pkg_services_tree_types_proto_rawDesc
)

func file_pkg_services_tree_types_proto_rawDescGZIP() []byte {
	file_pkg_services_tree_types_proto_rawDescOnce.Do(func() {
		file_pkg_services_tree_types_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_services_tree_types_proto_rawDescData)
	})
	return file_pkg_services_tree_types_proto_rawDescData
}

var file_pkg_services_tree_types_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_pkg_services_tree_types_proto_goTypes = []interface{}{
	(*KeyValue)(nil),  // 0: tree.KeyValue
	(*LogMove)(nil),   // 1: tree.LogMove
	(*Signature)(nil), // 2: tree.Signature
}
var file_pkg_services_tree_types_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_services_tree_types_proto_init() }
func file_pkg_services_tree_types_proto_init() {
	if File_pkg_services_tree_types_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_services_tree_types_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyValue); i {
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
		file_pkg_services_tree_types_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogMove); i {
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
		file_pkg_services_tree_types_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_services_tree_types_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_services_tree_types_proto_goTypes,
		DependencyIndexes: file_pkg_services_tree_types_proto_depIdxs,
		MessageInfos:      file_pkg_services_tree_types_proto_msgTypes,
	}.Build()
	File_pkg_services_tree_types_proto = out.File
	file_pkg_services_tree_types_proto_rawDesc = nil
	file_pkg_services_tree_types_proto_goTypes = nil
	file_pkg_services_tree_types_proto_depIdxs = nil
}
