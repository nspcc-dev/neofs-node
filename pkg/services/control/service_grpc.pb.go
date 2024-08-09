// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: pkg/services/control/service.proto

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

const (
	ControlService_HealthCheck_FullMethodName     = "/control.ControlService/HealthCheck"
	ControlService_SetNetmapStatus_FullMethodName = "/control.ControlService/SetNetmapStatus"
	ControlService_DropObjects_FullMethodName     = "/control.ControlService/DropObjects"
	ControlService_ListShards_FullMethodName      = "/control.ControlService/ListShards"
	ControlService_SetShardMode_FullMethodName    = "/control.ControlService/SetShardMode"
	ControlService_DumpShard_FullMethodName       = "/control.ControlService/DumpShard"
	ControlService_RestoreShard_FullMethodName    = "/control.ControlService/RestoreShard"
	ControlService_SynchronizeTree_FullMethodName = "/control.ControlService/SynchronizeTree"
	ControlService_EvacuateShard_FullMethodName   = "/control.ControlService/EvacuateShard"
	ControlService_FlushCache_FullMethodName      = "/control.ControlService/FlushCache"
	ControlService_ObjectStatus_FullMethodName    = "/control.ControlService/ObjectStatus"
)

// ControlServiceClient is the client API for ControlService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ControlServiceClient interface {
	// Performs health check of the storage node.
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
	// Sets status of the storage node in NeoFS network map.
	SetNetmapStatus(ctx context.Context, in *SetNetmapStatusRequest, opts ...grpc.CallOption) (*SetNetmapStatusResponse, error)
	// Mark objects to be removed from node's local object storage.
	DropObjects(ctx context.Context, in *DropObjectsRequest, opts ...grpc.CallOption) (*DropObjectsResponse, error)
	// Returns list that contains information about all shards of a node.
	ListShards(ctx context.Context, in *ListShardsRequest, opts ...grpc.CallOption) (*ListShardsResponse, error)
	// Sets mode of the shard.
	SetShardMode(ctx context.Context, in *SetShardModeRequest, opts ...grpc.CallOption) (*SetShardModeResponse, error)
	// Dump objects from the shard.
	DumpShard(ctx context.Context, in *DumpShardRequest, opts ...grpc.CallOption) (*DumpShardResponse, error)
	// Restore objects from dump.
	RestoreShard(ctx context.Context, in *RestoreShardRequest, opts ...grpc.CallOption) (*RestoreShardResponse, error)
	// Synchronizes all log operations for the specified tree.
	SynchronizeTree(ctx context.Context, in *SynchronizeTreeRequest, opts ...grpc.CallOption) (*SynchronizeTreeResponse, error)
	// EvacuateShard moves all data from one shard to the others.
	EvacuateShard(ctx context.Context, in *EvacuateShardRequest, opts ...grpc.CallOption) (*EvacuateShardResponse, error)
	// FlushCache moves all data from one shard to the others.
	FlushCache(ctx context.Context, in *FlushCacheRequest, opts ...grpc.CallOption) (*FlushCacheResponse, error)
	// ObjectStatus requests object status in the storage engine.
	ObjectStatus(ctx context.Context, in *ObjectStatusRequest, opts ...grpc.CallOption) (*ObjectStatusResponse, error)
}

type controlServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControlServiceClient(cc grpc.ClientConnInterface) ControlServiceClient {
	return &controlServiceClient{cc}
}

func (c *controlServiceClient) HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, ControlService_HealthCheck_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) SetNetmapStatus(ctx context.Context, in *SetNetmapStatusRequest, opts ...grpc.CallOption) (*SetNetmapStatusResponse, error) {
	out := new(SetNetmapStatusResponse)
	err := c.cc.Invoke(ctx, ControlService_SetNetmapStatus_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) DropObjects(ctx context.Context, in *DropObjectsRequest, opts ...grpc.CallOption) (*DropObjectsResponse, error) {
	out := new(DropObjectsResponse)
	err := c.cc.Invoke(ctx, ControlService_DropObjects_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) ListShards(ctx context.Context, in *ListShardsRequest, opts ...grpc.CallOption) (*ListShardsResponse, error) {
	out := new(ListShardsResponse)
	err := c.cc.Invoke(ctx, ControlService_ListShards_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) SetShardMode(ctx context.Context, in *SetShardModeRequest, opts ...grpc.CallOption) (*SetShardModeResponse, error) {
	out := new(SetShardModeResponse)
	err := c.cc.Invoke(ctx, ControlService_SetShardMode_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) DumpShard(ctx context.Context, in *DumpShardRequest, opts ...grpc.CallOption) (*DumpShardResponse, error) {
	out := new(DumpShardResponse)
	err := c.cc.Invoke(ctx, ControlService_DumpShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) RestoreShard(ctx context.Context, in *RestoreShardRequest, opts ...grpc.CallOption) (*RestoreShardResponse, error) {
	out := new(RestoreShardResponse)
	err := c.cc.Invoke(ctx, ControlService_RestoreShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) SynchronizeTree(ctx context.Context, in *SynchronizeTreeRequest, opts ...grpc.CallOption) (*SynchronizeTreeResponse, error) {
	out := new(SynchronizeTreeResponse)
	err := c.cc.Invoke(ctx, ControlService_SynchronizeTree_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) EvacuateShard(ctx context.Context, in *EvacuateShardRequest, opts ...grpc.CallOption) (*EvacuateShardResponse, error) {
	out := new(EvacuateShardResponse)
	err := c.cc.Invoke(ctx, ControlService_EvacuateShard_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) FlushCache(ctx context.Context, in *FlushCacheRequest, opts ...grpc.CallOption) (*FlushCacheResponse, error) {
	out := new(FlushCacheResponse)
	err := c.cc.Invoke(ctx, ControlService_FlushCache_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlServiceClient) ObjectStatus(ctx context.Context, in *ObjectStatusRequest, opts ...grpc.CallOption) (*ObjectStatusResponse, error) {
	out := new(ObjectStatusResponse)
	err := c.cc.Invoke(ctx, ControlService_ObjectStatus_FullMethodName, in, out, opts...)
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
	// Sets status of the storage node in NeoFS network map.
	SetNetmapStatus(context.Context, *SetNetmapStatusRequest) (*SetNetmapStatusResponse, error)
	// Mark objects to be removed from node's local object storage.
	DropObjects(context.Context, *DropObjectsRequest) (*DropObjectsResponse, error)
	// Returns list that contains information about all shards of a node.
	ListShards(context.Context, *ListShardsRequest) (*ListShardsResponse, error)
	// Sets mode of the shard.
	SetShardMode(context.Context, *SetShardModeRequest) (*SetShardModeResponse, error)
	// Dump objects from the shard.
	DumpShard(context.Context, *DumpShardRequest) (*DumpShardResponse, error)
	// Restore objects from dump.
	RestoreShard(context.Context, *RestoreShardRequest) (*RestoreShardResponse, error)
	// Synchronizes all log operations for the specified tree.
	SynchronizeTree(context.Context, *SynchronizeTreeRequest) (*SynchronizeTreeResponse, error)
	// EvacuateShard moves all data from one shard to the others.
	EvacuateShard(context.Context, *EvacuateShardRequest) (*EvacuateShardResponse, error)
	// FlushCache moves all data from one shard to the others.
	FlushCache(context.Context, *FlushCacheRequest) (*FlushCacheResponse, error)
	// ObjectStatus requests object status in the storage engine.
	ObjectStatus(context.Context, *ObjectStatusRequest) (*ObjectStatusResponse, error)
}

// UnimplementedControlServiceServer should be embedded to have forward compatible implementations.
type UnimplementedControlServiceServer struct {
}

func (UnimplementedControlServiceServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}
func (UnimplementedControlServiceServer) SetNetmapStatus(context.Context, *SetNetmapStatusRequest) (*SetNetmapStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetNetmapStatus not implemented")
}
func (UnimplementedControlServiceServer) DropObjects(context.Context, *DropObjectsRequest) (*DropObjectsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropObjects not implemented")
}
func (UnimplementedControlServiceServer) ListShards(context.Context, *ListShardsRequest) (*ListShardsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListShards not implemented")
}
func (UnimplementedControlServiceServer) SetShardMode(context.Context, *SetShardModeRequest) (*SetShardModeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetShardMode not implemented")
}
func (UnimplementedControlServiceServer) DumpShard(context.Context, *DumpShardRequest) (*DumpShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DumpShard not implemented")
}
func (UnimplementedControlServiceServer) RestoreShard(context.Context, *RestoreShardRequest) (*RestoreShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RestoreShard not implemented")
}
func (UnimplementedControlServiceServer) SynchronizeTree(context.Context, *SynchronizeTreeRequest) (*SynchronizeTreeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SynchronizeTree not implemented")
}
func (UnimplementedControlServiceServer) EvacuateShard(context.Context, *EvacuateShardRequest) (*EvacuateShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EvacuateShard not implemented")
}
func (UnimplementedControlServiceServer) FlushCache(context.Context, *FlushCacheRequest) (*FlushCacheResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FlushCache not implemented")
}
func (UnimplementedControlServiceServer) ObjectStatus(context.Context, *ObjectStatusRequest) (*ObjectStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ObjectStatus not implemented")
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
		FullMethod: ControlService_HealthCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).HealthCheck(ctx, req.(*HealthCheckRequest))
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
		FullMethod: ControlService_SetNetmapStatus_FullMethodName,
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
		FullMethod: ControlService_DropObjects_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).DropObjects(ctx, req.(*DropObjectsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_ListShards_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListShardsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).ListShards(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_ListShards_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).ListShards(ctx, req.(*ListShardsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_SetShardMode_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetShardModeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).SetShardMode(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_SetShardMode_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).SetShardMode(ctx, req.(*SetShardModeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_DumpShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DumpShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).DumpShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_DumpShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).DumpShard(ctx, req.(*DumpShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_RestoreShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RestoreShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).RestoreShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_RestoreShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).RestoreShard(ctx, req.(*RestoreShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_SynchronizeTree_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SynchronizeTreeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).SynchronizeTree(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_SynchronizeTree_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).SynchronizeTree(ctx, req.(*SynchronizeTreeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_EvacuateShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EvacuateShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).EvacuateShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_EvacuateShard_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).EvacuateShard(ctx, req.(*EvacuateShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_FlushCache_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FlushCacheRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).FlushCache(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_FlushCache_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).FlushCache(ctx, req.(*FlushCacheRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlService_ObjectStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ObjectStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlServiceServer).ObjectStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControlService_ObjectStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlServiceServer).ObjectStatus(ctx, req.(*ObjectStatusRequest))
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
			MethodName: "SetNetmapStatus",
			Handler:    _ControlService_SetNetmapStatus_Handler,
		},
		{
			MethodName: "DropObjects",
			Handler:    _ControlService_DropObjects_Handler,
		},
		{
			MethodName: "ListShards",
			Handler:    _ControlService_ListShards_Handler,
		},
		{
			MethodName: "SetShardMode",
			Handler:    _ControlService_SetShardMode_Handler,
		},
		{
			MethodName: "DumpShard",
			Handler:    _ControlService_DumpShard_Handler,
		},
		{
			MethodName: "RestoreShard",
			Handler:    _ControlService_RestoreShard_Handler,
		},
		{
			MethodName: "SynchronizeTree",
			Handler:    _ControlService_SynchronizeTree_Handler,
		},
		{
			MethodName: "EvacuateShard",
			Handler:    _ControlService_EvacuateShard_Handler,
		},
		{
			MethodName: "FlushCache",
			Handler:    _ControlService_FlushCache_Handler,
		},
		{
			MethodName: "ObjectStatus",
			Handler:    _ControlService_ObjectStatus_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/services/control/service.proto",
}
