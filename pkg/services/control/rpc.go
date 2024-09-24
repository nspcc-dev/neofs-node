package control

import (
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/common"
)

const serviceName = "control.ControlService"

const (
	rpcHealthCheck     = "HealthCheck"
	rpcSetNetmapStatus = "SetNetmapStatus"
	rpcDropObjects     = "DropObjects"
	rpcListShards      = "ListShards"
	rpcListObjects     = "ListObjects"
	rpcSetShardMode    = "SetShardMode"
	rpcDumpShard       = "DumpShard"
	rpcRestoreShard    = "RestoreShard"
	rpcSynchronizeTree = "SynchronizeTree"
	rpcEvacuateShard   = "EvacuateShard"
	rpcFlushCache      = "FlushCache"
	rpcObjectStatus    = "ObjectStatus"
)

// HealthCheck executes ControlService.HealthCheck RPC.
func HealthCheck(
	cli *client.Client,
	req *HealthCheckRequest,
	opts ...client.CallOption,
) (*HealthCheckResponse, error) {
	wResp := &healthCheckResponseWrapper{
		m: new(HealthCheckResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcHealthCheck), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// SetNetmapStatus executes ControlService.SetNetmapStatus RPC.
func SetNetmapStatus(
	cli *client.Client,
	req *SetNetmapStatusRequest,
	opts ...client.CallOption,
) (*SetNetmapStatusResponse, error) {
	wResp := &setNetmapStatusResponseWrapper{
		m: new(SetNetmapStatusResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcSetNetmapStatus), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// DropObjects executes ControlService.DropObjects RPC.
func DropObjects(
	cli *client.Client,
	req *DropObjectsRequest,
	opts ...client.CallOption,
) (*DropObjectsResponse, error) {
	wResp := &dropObjectsResponseWrapper{
		m: new(DropObjectsResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}
	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcDropObjects), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// ListShards executes ControlService.ListShards RPC.
func ListShards(
	cli *client.Client,
	req *ListShardsRequest,
	opts ...client.CallOption,
) (*ListShardsResponse, error) {
	wResp := &listShardsResponseWrapper{
		m: new(ListShardsResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}
	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcListShards), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// ListObjects executes ControlService.ListObjects RPC.
func ListObjects(
	cli *client.Client,
	req *ListObjectsRequest,
	handleResp func(*ListObjectsResponse) error,
	opts ...client.CallOption,
) error {
	wReq := &requestWrapper{
		m: req,
	}

	stream, err := client.OpenServerStream(cli, common.CallMethodInfoServerStream(serviceName, rpcListObjects), wReq, opts...)
	if err != nil {
		return err
	}

	for {
		wResp := &listObjectsResponseWrapper{
			m: new(ListObjectsResponse),
		}
		err = stream.ReadMessage(wResp)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		err = handleResp(wResp.m)
		if err != nil {
			return err
		}
	}

	return nil
}

// SetShardMode executes ControlService.SetShardMode RPC.
func SetShardMode(
	cli *client.Client,
	req *SetShardModeRequest,
	opts ...client.CallOption,
) (*SetShardModeResponse, error) {
	wResp := &setShardModeResponseWrapper{
		m: new(SetShardModeResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}
	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcSetShardMode), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// DumpShard executes ControlService.DumpShard RPC.
func DumpShard(cli *client.Client, req *DumpShardRequest, opts ...client.CallOption) (*DumpShardResponse, error) {
	wResp := &dumpShardResponseWrapper{new(DumpShardResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcDumpShard), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.DumpShardResponse, nil
}

// RestoreShard executes ControlService.DumpShard RPC.
func RestoreShard(cli *client.Client, req *RestoreShardRequest, opts ...client.CallOption) (*RestoreShardResponse, error) {
	wResp := &restoreShardResponseWrapper{new(RestoreShardResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcRestoreShard), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.RestoreShardResponse, nil
}

// SynchronizeTree executes ControlService.SynchronizeTree RPC.
func SynchronizeTree(cli *client.Client, req *SynchronizeTreeRequest, opts ...client.CallOption) (*SynchronizeTreeResponse, error) {
	wResp := &synchronizeTreeResponseWrapper{new(SynchronizeTreeResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcSynchronizeTree), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.SynchronizeTreeResponse, nil
}

// EvacuateShard executes ControlService.EvacuateShard RPC.
func EvacuateShard(cli *client.Client, req *EvacuateShardRequest, opts ...client.CallOption) (*EvacuateShardResponse, error) {
	wResp := &evacuateShardResponseWrapper{new(EvacuateShardResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcEvacuateShard), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.EvacuateShardResponse, nil
}

// FlushCache executes ControlService.FlushCache RPC.
func FlushCache(cli *client.Client, req *FlushCacheRequest, opts ...client.CallOption) (*FlushCacheResponse, error) {
	wResp := &flushCacheResponseWrapper{new(FlushCacheResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcFlushCache), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.FlushCacheResponse, nil
}

// ObjectStatus executes ControlService.ObjectStatus RPC.
func ObjectStatus(cli *client.Client, req *ObjectStatusRequest, opts ...client.CallOption) (*ObjectStatusResponse, error) {
	wResp := &objectStatusResponseWrapper{new(ObjectStatusResponse)}
	wReq := &requestWrapper{m: req}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcObjectStatus), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.ObjectStatusResponse, nil
}
