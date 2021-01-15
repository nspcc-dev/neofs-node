// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.14.0
// source: pkg/services/control/service.proto

package control

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

// Health check request.
type HealthCheckRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Body of health check request message.
	Body *HealthCheckRequest_Body `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
	// Body signature.
	Signature *Signature `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
}

func (x *HealthCheckRequest) Reset() {
	*x = HealthCheckRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckRequest) ProtoMessage() {}

func (x *HealthCheckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[0]
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
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{0}
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

// Health check request.
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
		mi := &file_pkg_services_control_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckResponse) ProtoMessage() {}

func (x *HealthCheckResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[1]
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
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{1}
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

// Get netmap snapshot request.
type NetmapSnapshotRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Body of get netmap snapshot request message.
	Body *NetmapSnapshotRequest_Body `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
	// Body signature.
	Signature *Signature `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
}

func (x *NetmapSnapshotRequest) Reset() {
	*x = NetmapSnapshotRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NetmapSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NetmapSnapshotRequest) ProtoMessage() {}

func (x *NetmapSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NetmapSnapshotRequest.ProtoReflect.Descriptor instead.
func (*NetmapSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{2}
}

func (x *NetmapSnapshotRequest) GetBody() *NetmapSnapshotRequest_Body {
	if x != nil {
		return x.Body
	}
	return nil
}

func (x *NetmapSnapshotRequest) GetSignature() *Signature {
	if x != nil {
		return x.Signature
	}
	return nil
}

// Get netmap snapshot request.
type NetmapSnapshotResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Body of get netmap snapshot response message.
	Body *NetmapSnapshotResponse_Body `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
	// Body signature.
	Signature *Signature `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
}

func (x *NetmapSnapshotResponse) Reset() {
	*x = NetmapSnapshotResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NetmapSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NetmapSnapshotResponse) ProtoMessage() {}

func (x *NetmapSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NetmapSnapshotResponse.ProtoReflect.Descriptor instead.
func (*NetmapSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{3}
}

func (x *NetmapSnapshotResponse) GetBody() *NetmapSnapshotResponse_Body {
	if x != nil {
		return x.Body
	}
	return nil
}

func (x *NetmapSnapshotResponse) GetSignature() *Signature {
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
		mi := &file_pkg_services_control_service_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckRequest_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckRequest_Body) ProtoMessage() {}

func (x *HealthCheckRequest_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[4]
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
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{0, 0}
}

// Health check response body
type HealthCheckResponse_Body struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Health status of storage node.
	Status HealthStatus `protobuf:"varint,1,opt,name=status,proto3,enum=control.HealthStatus" json:"status,omitempty"`
}

func (x *HealthCheckResponse_Body) Reset() {
	*x = HealthCheckResponse_Body{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HealthCheckResponse_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckResponse_Body) ProtoMessage() {}

func (x *HealthCheckResponse_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[5]
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
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{1, 0}
}

func (x *HealthCheckResponse_Body) GetStatus() HealthStatus {
	if x != nil {
		return x.Status
	}
	return HealthStatus_STATUS_UNDEFINED
}

// Get netmap snapshot request body.
type NetmapSnapshotRequest_Body struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *NetmapSnapshotRequest_Body) Reset() {
	*x = NetmapSnapshotRequest_Body{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NetmapSnapshotRequest_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NetmapSnapshotRequest_Body) ProtoMessage() {}

func (x *NetmapSnapshotRequest_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NetmapSnapshotRequest_Body.ProtoReflect.Descriptor instead.
func (*NetmapSnapshotRequest_Body) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{2, 0}
}

// Get netmap snapshot response body
type NetmapSnapshotResponse_Body struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Structure of the requested network map.
	Netmap *Netmap `protobuf:"bytes,1,opt,name=netmap,proto3" json:"netmap,omitempty"`
}

func (x *NetmapSnapshotResponse_Body) Reset() {
	*x = NetmapSnapshotResponse_Body{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_services_control_service_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NetmapSnapshotResponse_Body) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NetmapSnapshotResponse_Body) ProtoMessage() {}

func (x *NetmapSnapshotResponse_Body) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_services_control_service_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NetmapSnapshotResponse_Body.ProtoReflect.Descriptor instead.
func (*NetmapSnapshotResponse_Body) Descriptor() ([]byte, []int) {
	return file_pkg_services_control_service_proto_rawDescGZIP(), []int{3, 0}
}

func (x *NetmapSnapshotResponse_Body) GetNetmap() *Netmap {
	if x != nil {
		return x.Netmap
	}
	return nil
}

var File_pkg_services_control_service_proto protoreflect.FileDescriptor

var file_pkg_services_control_service_proto_rawDesc = []byte{
	0x0a, 0x22, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x63,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x1a, 0x20, 0x70,
	0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x63, 0x6f, 0x6e, 0x74,
	0x72, 0x6f, 0x6c, 0x2f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x84, 0x01, 0x0a, 0x12, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x34, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48,
	0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x2e, 0x42, 0x6f, 0x64, 0x79, 0x52, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x12, 0x30, 0x0a, 0x09,
	0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74,
	0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x1a, 0x06,
	0x0a, 0x04, 0x42, 0x6f, 0x64, 0x79, 0x22, 0xb5, 0x01, 0x0a, 0x13, 0x48, 0x65, 0x61, 0x6c, 0x74,
	0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x35,
	0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x63,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65,
	0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x42, 0x6f, 0x64, 0x79, 0x52,
	0x04, 0x62, 0x6f, 0x64, 0x79, 0x12, 0x30, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75,
	0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x2e, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69,
	0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x1a, 0x35, 0x0a, 0x04, 0x42, 0x6f, 0x64, 0x79, 0x12,
	0x2d, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x15, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x8a,
	0x01, 0x0a, 0x15, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x37, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c,
	0x2e, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x42, 0x6f, 0x64, 0x79, 0x52, 0x04, 0x62, 0x6f, 0x64,
	0x79, 0x12, 0x30, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x53,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74,
	0x75, 0x72, 0x65, 0x1a, 0x06, 0x0a, 0x04, 0x42, 0x6f, 0x64, 0x79, 0x22, 0xb5, 0x01, 0x0a, 0x16,
	0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x38, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x24, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e,
	0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x42, 0x6f, 0x64, 0x79, 0x52, 0x04, 0x62, 0x6f, 0x64, 0x79,
	0x12, 0x30, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x53, 0x69,
	0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75,
	0x72, 0x65, 0x1a, 0x2f, 0x0a, 0x04, 0x42, 0x6f, 0x64, 0x79, 0x12, 0x27, 0x0a, 0x06, 0x6e, 0x65,
	0x74, 0x6d, 0x61, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x6f, 0x6e,
	0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x52, 0x06, 0x6e, 0x65, 0x74,
	0x6d, 0x61, 0x70, 0x32, 0xad, 0x01, 0x0a, 0x0e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x53,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x48, 0x0a, 0x0b, 0x48, 0x65, 0x61, 0x6c, 0x74, 0x68,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x12, 0x1b, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e,
	0x48, 0x65, 0x61, 0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x1c, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x48, 0x65, 0x61,
	0x6c, 0x74, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x51, 0x0a, 0x0e, 0x4e, 0x65, 0x74, 0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68,
	0x6f, 0x74, 0x12, 0x1e, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x65, 0x74,
	0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x2e, 0x4e, 0x65, 0x74,
	0x6d, 0x61, 0x70, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6e, 0x73, 0x70, 0x63, 0x63, 0x2d, 0x64, 0x65, 0x76, 0x2f, 0x6e, 0x65, 0x6f, 0x66,
	0x73, 0x2d, 0x6e, 0x6f, 0x64, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x73, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pkg_services_control_service_proto_rawDescOnce sync.Once
	file_pkg_services_control_service_proto_rawDescData = file_pkg_services_control_service_proto_rawDesc
)

func file_pkg_services_control_service_proto_rawDescGZIP() []byte {
	file_pkg_services_control_service_proto_rawDescOnce.Do(func() {
		file_pkg_services_control_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_services_control_service_proto_rawDescData)
	})
	return file_pkg_services_control_service_proto_rawDescData
}

var file_pkg_services_control_service_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_pkg_services_control_service_proto_goTypes = []interface{}{
	(*HealthCheckRequest)(nil),          // 0: control.HealthCheckRequest
	(*HealthCheckResponse)(nil),         // 1: control.HealthCheckResponse
	(*NetmapSnapshotRequest)(nil),       // 2: control.NetmapSnapshotRequest
	(*NetmapSnapshotResponse)(nil),      // 3: control.NetmapSnapshotResponse
	(*HealthCheckRequest_Body)(nil),     // 4: control.HealthCheckRequest.Body
	(*HealthCheckResponse_Body)(nil),    // 5: control.HealthCheckResponse.Body
	(*NetmapSnapshotRequest_Body)(nil),  // 6: control.NetmapSnapshotRequest.Body
	(*NetmapSnapshotResponse_Body)(nil), // 7: control.NetmapSnapshotResponse.Body
	(*Signature)(nil),                   // 8: control.Signature
	(HealthStatus)(0),                   // 9: control.HealthStatus
	(*Netmap)(nil),                      // 10: control.Netmap
}
var file_pkg_services_control_service_proto_depIdxs = []int32{
	4,  // 0: control.HealthCheckRequest.body:type_name -> control.HealthCheckRequest.Body
	8,  // 1: control.HealthCheckRequest.signature:type_name -> control.Signature
	5,  // 2: control.HealthCheckResponse.body:type_name -> control.HealthCheckResponse.Body
	8,  // 3: control.HealthCheckResponse.signature:type_name -> control.Signature
	6,  // 4: control.NetmapSnapshotRequest.body:type_name -> control.NetmapSnapshotRequest.Body
	8,  // 5: control.NetmapSnapshotRequest.signature:type_name -> control.Signature
	7,  // 6: control.NetmapSnapshotResponse.body:type_name -> control.NetmapSnapshotResponse.Body
	8,  // 7: control.NetmapSnapshotResponse.signature:type_name -> control.Signature
	9,  // 8: control.HealthCheckResponse.Body.status:type_name -> control.HealthStatus
	10, // 9: control.NetmapSnapshotResponse.Body.netmap:type_name -> control.Netmap
	0,  // 10: control.ControlService.HealthCheck:input_type -> control.HealthCheckRequest
	2,  // 11: control.ControlService.NetmapSnapshot:input_type -> control.NetmapSnapshotRequest
	1,  // 12: control.ControlService.HealthCheck:output_type -> control.HealthCheckResponse
	3,  // 13: control.ControlService.NetmapSnapshot:output_type -> control.NetmapSnapshotResponse
	12, // [12:14] is the sub-list for method output_type
	10, // [10:12] is the sub-list for method input_type
	10, // [10:10] is the sub-list for extension type_name
	10, // [10:10] is the sub-list for extension extendee
	0,  // [0:10] is the sub-list for field type_name
}

func init() { file_pkg_services_control_service_proto_init() }
func file_pkg_services_control_service_proto_init() {
	if File_pkg_services_control_service_proto != nil {
		return
	}
	file_pkg_services_control_types_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_pkg_services_control_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
		file_pkg_services_control_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_pkg_services_control_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NetmapSnapshotRequest); i {
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
		file_pkg_services_control_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NetmapSnapshotResponse); i {
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
		file_pkg_services_control_service_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
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
		file_pkg_services_control_service_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
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
		file_pkg_services_control_service_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NetmapSnapshotRequest_Body); i {
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
		file_pkg_services_control_service_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NetmapSnapshotResponse_Body); i {
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
			RawDescriptor: file_pkg_services_control_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_services_control_service_proto_goTypes,
		DependencyIndexes: file_pkg_services_control_service_proto_depIdxs,
		MessageInfos:      file_pkg_services_control_service_proto_msgTypes,
	}.Build()
	File_pkg_services_control_service_proto = out.File
	file_pkg_services_control_service_proto_rawDesc = nil
	file_pkg_services_control_service_proto_goTypes = nil
	file_pkg_services_control_service_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// ControlServiceClient is the client API for ControlService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ControlServiceClient interface {
	// Performs health check of the storage node.
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
	// Returns network map snapshot of the current NeoFS epoch.
	NetmapSnapshot(ctx context.Context, in *NetmapSnapshotRequest, opts ...grpc.CallOption) (*NetmapSnapshotResponse, error)
}

type controlServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControlServiceClient(cc grpc.ClientConnInterface) ControlServiceClient {
	return &controlServiceClient{cc}
}

func (c *controlServiceClient) HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, "/control.ControlService/HealthCheck", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) NetmapSnapshot(ctx context.Context, in *NetmapSnapshotRequest, opts ...grpc.CallOption) (*NetmapSnapshotResponse, error) {
	out := new(NetmapSnapshotResponse)
	err := c.cc.Invoke(ctx, "/control.ControlService/NetmapSnapshot", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControlServiceServer is the server API for ControlService service.
type ControlServiceServer interface {
	// Performs health check of the storage node.
	HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
	// Returns network map snapshot of the current NeoFS epoch.
	NetmapSnapshot(context.Context, *NetmapSnapshotRequest) (*NetmapSnapshotResponse, error)
}

// UnimplementedControlServiceServer can be embedded to have forward compatible implementations.
type UnimplementedControlServiceServer struct {
}

func (*UnimplementedControlServiceServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}
func (*UnimplementedControlServiceServer) NetmapSnapshot(context.Context, *NetmapSnapshotRequest) (*NetmapSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NetmapSnapshot not implemented")
}

func RegisterControlServiceServer(s *grpc.Server, srv ControlServiceServer) {
	s.RegisterService(&_ControlService_serviceDesc, srv)
}

func _ControlService_HealthCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).HealthCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/control.ControlService/HealthCheck",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).HealthCheck(ctx, req.(*HealthCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_NetmapSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NetmapSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).NetmapSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/control.ControlService/NetmapSnapshot",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).NetmapSnapshot(ctx, req.(*NetmapSnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ControlService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "control.ControlService",
	HandlerType: (*ControlServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "HealthCheck",
			Handler:    _ControlService_HealthCheck_Handler,
		},
		{
			MethodName: "NetmapSnapshot",
			Handler:    _ControlService_NetmapSnapshot_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/services/control/service.proto",
}
