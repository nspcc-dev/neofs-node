// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package control

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ControlServiceClient is the client API for ControlService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ControlServiceClient interface {
	// Performs health check of the storage node.
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
	// Returns network map snapshot of the current NeoFS epoch.
	NetmapSnapshot(ctx context.Context, in *NetmapSnapshotRequest, opts ...grpc.CallOption) (*NetmapSnapshotResponse, error)
	// Sets status of the storage node in NeoFS network map.
	SetNetmapStatus(ctx context.Context, in *SetNetmapStatusRequest, opts ...grpc.CallOption) (*SetNetmapStatusResponse, error)
	// Mark objects to be removed from node's local object storage.
	DropObjects(ctx context.Context, in *DropObjectsRequest, opts ...grpc.CallOption) (*DropObjectsResponse, error)
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

func (c *controlServiceClient) SetNetmapStatus(ctx context.Context, in *SetNetmapStatusRequest, opts ...grpc.CallOption) (*SetNetmapStatusResponse, error) {
	out := new(SetNetmapStatusResponse)
	err := c.cc.Invoke(ctx, "/control.ControlService/SetNetmapStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) DropObjects(ctx context.Context, in *DropObjectsRequest, opts ...grpc.CallOption) (*DropObjectsResponse, error) {
	out := new(DropObjectsResponse)
	err := c.cc.Invoke(ctx, "/control.ControlService/DropObjects", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControlServiceServer is the server API for ControlService service.
// All implementations should embed UnimplementedControlServiceServer
// for forward compatibility
type ControlServiceServer interface {
	// Performs health check of the storage node.
	HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
	// Returns network map snapshot of the current NeoFS epoch.
	NetmapSnapshot(context.Context, *NetmapSnapshotRequest) (*NetmapSnapshotResponse, error)
	// Sets status of the storage node in NeoFS network map.
	SetNetmapStatus(context.Context, *SetNetmapStatusRequest) (*SetNetmapStatusResponse, error)
	// Mark objects to be removed from node's local object storage.
	DropObjects(context.Context, *DropObjectsRequest) (*DropObjectsResponse, error)
}

// UnimplementedControlServiceServer should be embedded to have forward compatible implementations.
type UnimplementedControlServiceServer struct {
}

func (UnimplementedControlServiceServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}
func (UnimplementedControlServiceServer) NetmapSnapshot(context.Context, *NetmapSnapshotRequest) (*NetmapSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NetmapSnapshot not implemented")
}
func (UnimplementedControlServiceServer) SetNetmapStatus(context.Context, *SetNetmapStatusRequest) (*SetNetmapStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetNetmapStatus not implemented")
}
func (UnimplementedControlServiceServer) DropObjects(context.Context, *DropObjectsRequest) (*DropObjectsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropObjects not implemented")
}

// UnsafeControlServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ControlServiceServer will
// result in compilation errors.
type UnsafeControlServiceServer interface {
	mustEmbedUnimplementedControlServiceServer()
}

func RegisterControlServiceServer(s grpc.ServiceRegistrar, srv ControlServiceServer) {
	s.RegisterService(&ControlService_ServiceDesc, srv)
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

func _ControlService_SetNetmapStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetNetmapStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).SetNetmapStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/control.ControlService/SetNetmapStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).SetNetmapStatus(ctx, req.(*SetNetmapStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_DropObjects_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DropObjectsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).DropObjects(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/control.ControlService/DropObjects",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).DropObjects(ctx, req.(*DropObjectsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ControlService_ServiceDesc is the grpc.ServiceDesc for ControlService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ControlService_ServiceDesc = grpc.ServiceDesc{
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
		{
			MethodName: "SetNetmapStatus",
			Handler:    _ControlService_SetNetmapStatus_Handler,
		},
		{
			MethodName: "DropObjects",
			Handler:    _ControlService_DropObjects_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/services/control/service.proto",
}
