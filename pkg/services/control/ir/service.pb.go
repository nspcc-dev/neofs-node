// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.21.2
// source: pkg/services/control/ir/service.proto

package control

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Health check request.
type HealthCheckRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Body of health check request message.
	Body *HealthCheckRequest_Body `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
	// Body signature.
	// Should be signed by node key or one of
	// the keys configured by the node.
	Signature *Signature `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
}

func (x *HealthCheckRequest) Reset() {
	*x = HealthCheckRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_ir_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckRequest) ProtoMessage() {}

func (x *HealthCheckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_ir_service_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckRequest.ProtoReflect.Descriptor instead.
func (*HealthCheckRequest) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_ir_service_proto_rawDescGZIP(), []int{0}
}

func (x *HealthCheckRequest) GetBody() *HealthCheckRequest_Body {
	if x != nil {
		return x.Body
	}
	return nil
}

func (x *HealthCheckRequest) GetSignature() *Signature {
	if x != nil {
		return x.Signature
	}
	return nil
}

// Health check response.
type HealthCheckResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Body of health check response message.
	Body *HealthCheckResponse_Body `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
	// Body signature.
	Signature *Signature `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
}

func (x *HealthCheckResponse) Reset() {
	*x = HealthCheckResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_ir_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckResponse) ProtoMessage() {}

func (x *HealthCheckResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_ir_service_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckResponse.ProtoReflect.Descriptor instead.
func (*HealthCheckResponse) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_ir_service_proto_rawDescGZIP(), []int{1}
}

func (x *HealthCheckResponse) GetBody() *HealthCheckResponse_Body {
	if x != nil {
		return x.Body
	}
	return nil
}

func (x *HealthCheckResponse) GetSignature() *Signature {
	if x != nil {
		return x.Signature
	}
	return nil
}

// Health check request body.
type HealthCheckRequest_Body struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *HealthCheckRequest_Body) Reset() {
	*x = HealthCheckRequest_Body{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_ir_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckRequest_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckRequest_Body) ProtoMessage() {}

func (x *HealthCheckRequest_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_ir_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckRequest_Body.ProtoReflect.Descriptor instead.
func (*HealthCheckRequest_Body) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_ir_service_proto_rawDescGZIP(), []int{0, 0}
}

// Health check response body
type HealthCheckResponse_Body struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Health status of IR node application.
	HealthStatus HealthStatus `protobuf:"varint,1,opt,name=health_status,json=healthStatus,proto3,enum=ircontrol.HealthStatus" json:"health_status,omitempty"`
}

func (x *HealthCheckResponse_Body) Reset() {
	*x = HealthCheckResponse_Body{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_ir_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckResponse_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckResponse_Body) ProtoMessage() {}

func (x *HealthCheckResponse_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_ir_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckResponse_Body.ProtoReflect.Descriptor instead.
func (*HealthCheckResponse_Body) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_ir_service_proto_rawDescGZIP(), []int{1, 0}
}

func (x *HealthCheckResponse_Body) GetHealthStatus() HealthStatus {
	if x != nil {
		return x.HealthStatus
	}
	return HealthStatus_HEALTH_STATUS_UNDEFINED
}

var File_pkg_services_control_ir_service_proto protoreflect.FileDescriptor

var file_pkg_services_control_ir_service_proto_rawDesc = []byte{
	0x0a, 0x25, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x63,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2f, 0x69, 0x72, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x09, 0x69, 0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x1a, 0x23, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73,
	0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2f, 0x69, 0x72, 0x2f, 0x74, 0x79, 0x70, 0x65,
	0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x88, 0x01, 0x0a, 0x12, 0x48, 0x65, 0x61, 0x6c,
	0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x36,
	0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x69,
	0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x42, 0x6f, 0x64, 0x79,
	0x52, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x12, 0x32, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74,
	0x75, 0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x69, 0x72, 0x63, 0x6f,
	0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52,
	0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x1a, 0x06, 0x0a, 0x04, 0x42, 0x6f,
	0x64, 0x79, 0x22, 0xc8, 0x01, 0x0a, 0x13, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65,
	0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x37, 0x0a, 0x04, 0x62, 0x6f,
	0x64, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x69, 0x72, 0x63, 0x6f, 0x6e,
	0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x42, 0x6f, 0x64, 0x79, 0x52, 0x04, 0x62,
	0x6f, 0x64, 0x79, 0x12, 0x32, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x69, 0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x2e, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69,
	0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x1a, 0x44, 0x0a, 0x04, 0x42, 0x6f, 0x64, 0x79, 0x12,
	0x3c, 0x0a, 0x0d, 0x68, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17, 0x2e, 0x69, 0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52,
	0x0c, 0x68, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x32, 0x5e, 0x0a,
	0x0e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12,
	0x4c, 0x0a, 0x0b, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x12, 0x1d,
	0x2e, 0x69, 0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74,
	0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e,
	0x69, 0x72, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x39, 0x5a,
	0x37, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x73, 0x70, 0x63,
	0x63, 0x2d, 0x64, 0x65, 0x76, 0x2f, 0x6e, 0x65, 0x6f, 0x66, 0x73, 0x2d, 0x6e, 0x6f, 0x64, 0x65,
	0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x69, 0x72,
	0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_services_control_ir_service_proto_rawDescOnce sync.Once
	file_pkg_services_control_ir_service_proto_rawDescData = file_pkg_services_control_ir_service_proto_rawDesc
)

func file_pkg_services_control_ir_service_proto_rawDescGZIP() []byte {
	file_pkg_services_control_ir_service_proto_rawDescOnce.Do(func() {
		file_pkg_services_control_ir_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_services_control_ir_service_proto_rawDescData)
	})
	return file_pkg_services_control_ir_service_proto_rawDescData
}

var file_pkg_services_control_ir_service_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_services_control_ir_service_proto_goTypes = []interface{}{
	(*HealthCheckRequest)(nil),       // 0: ircontrol.HealthCheckRequest
	(*HealthCheckResponse)(nil),      // 1: ircontrol.HealthCheckResponse
	(*HealthCheckRequest_Body)(nil),  // 2: ircontrol.HealthCheckRequest.Body
	(*HealthCheckResponse_Body)(nil), // 3: ircontrol.HealthCheckResponse.Body
	(*Signature)(nil),                // 4: ircontrol.Signature
	(HealthStatus)(0),                // 5: ircontrol.HealthStatus
}
var file_pkg_services_control_ir_service_proto_depIdxs = []int32{
	2, // 0: ircontrol.HealthCheckRequest.body:type_name -> ircontrol.HealthCheckRequest.Body
	4, // 1: ircontrol.HealthCheckRequest.signature:type_name -> ircontrol.Signature
	3, // 2: ircontrol.HealthCheckResponse.body:type_name -> ircontrol.HealthCheckResponse.Body
	4, // 3: ircontrol.HealthCheckResponse.signature:type_name -> ircontrol.Signature
	5, // 4: ircontrol.HealthCheckResponse.Body.health_status:type_name -> ircontrol.HealthStatus
	0, // 5: ircontrol.ControlService.HealthCheck:input_type -> ircontrol.HealthCheckRequest
	1, // 6: ircontrol.ControlService.HealthCheck:output_type -> ircontrol.HealthCheckResponse
	6, // [6:7] is the sub-list for method output_type
	5, // [5:6] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_pkg_services_control_ir_service_proto_init() }
func file_pkg_services_control_ir_service_proto_init() {
	if File_pkg_services_control_ir_service_proto != nil {
		return
	}
	file_pkg_services_control_ir_types_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_pkg_services_control_ir_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HealthCheckRequest); i {
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
		file_pkg_services_control_ir_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HealthCheckResponse); i {
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
		file_pkg_services_control_ir_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HealthCheckRequest_Body); i {
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
		file_pkg_services_control_ir_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HealthCheckResponse_Body); i {
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
			RawDescriptor: file_pkg_services_control_ir_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_services_control_ir_service_proto_goTypes,
		DependencyIndexes: file_pkg_services_control_ir_service_proto_depIdxs,
		MessageInfos:      file_pkg_services_control_ir_service_proto_msgTypes,
	}.Build()
	File_pkg_services_control_ir_service_proto = out.File
	file_pkg_services_control_ir_service_proto_rawDesc = nil
	file_pkg_services_control_ir_service_proto_goTypes = nil
	file_pkg_services_control_ir_service_proto_depIdxs = nil
}
