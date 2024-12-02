// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.0
// source: pkg/services/control/ir/service.proto

package control

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ControlService_HealthCheck_FullMethodName   = "/ircontrol.ControlService/HealthCheck"
	ControlService_NotaryList_FullMethodName    = "/ircontrol.ControlService/NotaryList"
	ControlService_NotaryRequest_FullMethodName = "/ircontrol.ControlService/NotaryRequest"
	ControlService_NotarySign_FullMethodName    = "/ircontrol.ControlService/NotarySign"
)

// ControlServiceClient is the client API for ControlService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// `ControlService` provides an interface for internal work with the Inner Ring node.
type ControlServiceClient interface {
	// Performs health check of the IR node.
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
	// NotaryList list notary requests from the network.
	NotaryList(ctx context.Context, in *NotaryListRequest, opts ...grpc.CallOption) (*NotaryListResponse, error)
	// NotaryRequest create and send notary request to the network.
	NotaryRequest(ctx context.Context, in *NotaryRequestRequest, opts ...grpc.CallOption) (*NotaryRequestResponse, error)
	// NotarySign sign a notary request by it hash.
	NotarySign(ctx context.Context, in *NotarySignRequest, opts ...grpc.CallOption) (*NotarySignResponse, error)
}

type controlServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControlServiceClient(cc grpc.ClientConnInterface) ControlServiceClient {
	return &controlServiceClient{cc}
}

func (c *controlServiceClient) HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, ControlService_HealthCheck_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) NotaryList(ctx context.Context, in *NotaryListRequest, opts ...grpc.CallOption) (*NotaryListResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(NotaryListResponse)
	err := c.cc.Invoke(ctx, ControlService_NotaryList_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) NotaryRequest(ctx context.Context, in *NotaryRequestRequest, opts ...grpc.CallOption) (*NotaryRequestResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(NotaryRequestResponse)
	err := c.cc.Invoke(ctx, ControlService_NotaryRequest_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) NotarySign(ctx context.Context, in *NotarySignRequest, opts ...grpc.CallOption) (*NotarySignResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(NotarySignResponse)
	err := c.cc.Invoke(ctx, ControlService_NotarySign_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControlServiceServer is the server API for ControlService service.
// All implementations should embed UnimplementedControlServiceServer
// for forward compatibility.
//
// `ControlService` provides an interface for internal work with the Inner Ring node.
type ControlServiceServer interface {
	// Performs health check of the IR node.
	HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
	// NotaryList list notary requests from the network.
	NotaryList(context.Context, *NotaryListRequest) (*NotaryListResponse, error)
	// NotaryRequest create and send notary request to the network.
	NotaryRequest(context.Context, *NotaryRequestRequest) (*NotaryRequestResponse, error)
	// NotarySign sign a notary request by it hash.
	NotarySign(context.Context, *NotarySignRequest) (*NotarySignResponse, error)
}

// UnimplementedControlServiceServer should be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedControlServiceServer struct{}

func (UnimplementedControlServiceServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}
func (UnimplementedControlServiceServer) NotaryList(context.Context, *NotaryListRequest) (*NotaryListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NotaryList not implemented")
}
func (UnimplementedControlServiceServer) NotaryRequest(context.Context, *NotaryRequestRequest) (*NotaryRequestResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NotaryRequest not implemented")
}
func (UnimplementedControlServiceServer) NotarySign(context.Context, *NotarySignRequest) (*NotarySignResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NotarySign not implemented")
}
func (UnimplementedControlServiceServer) testEmbeddedByValue() {}

// UnsafeControlServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ControlServiceServer will
// result in compilation errors.
type UnsafeControlServiceServer interface {
	mustEmbedUnimplementedControlServiceServer()
}

func RegisterControlServiceServer(s grpc.ServiceRegistrar, srv ControlServiceServer) {
	// If the following call pancis, it indicates UnimplementedControlServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
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
		FullMethod: ControlService_HealthCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).HealthCheck(ctx, req.(*HealthCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_NotaryList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NotaryListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).NotaryList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_NotaryList_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).NotaryList(ctx, req.(*NotaryListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_NotaryRequest_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NotaryRequestRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).NotaryRequest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_NotaryRequest_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).NotaryRequest(ctx, req.(*NotaryRequestRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_NotarySign_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NotarySignRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).NotarySign(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_NotarySign_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).NotarySign(ctx, req.(*NotarySignRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ControlService_ServiceDesc is the grpc.ServiceDesc for ControlService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ControlService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ircontrol.ControlService",
	HandlerType: (*ControlServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "HealthCheck",
			Handler:    _ControlService_HealthCheck_Handler,
		},
		{
			MethodName: "NotaryList",
			Handler:    _ControlService_NotaryList_Handler,
		},
		{
			MethodName: "NotaryRequest",
			Handler:    _ControlService_NotaryRequest_Handler,
		},
		{
			MethodName: "NotarySign",
			Handler:    _ControlService_NotarySign_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/services/control/ir/service.proto",
}
