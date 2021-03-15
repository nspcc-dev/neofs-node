package control

import (
	"github.com/nspcc-dev/neofs-api-go/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/rpc/common"
)

const serviceName = "control.ControlService"

const (
	rpcHealthCheck     = "HealthCheck"
	rpcNetmapSnapshot  = "NetmapSnapshot"
	rpcSetNetmapStatus = "SetNetmapStatus"
	rpcDropObjects     = "DropObjects"
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

// NetmapSnapshot executes ControlService.NetmapSnapshot RPC.
func NetmapSnapshot(
	cli *client.Client,
	req *NetmapSnapshotRequest,
	opts ...client.CallOption,
) (*NetmapSnapshotResponse, error) {
	wResp := &netmapSnapshotResponseWrapper{
		m: new(NetmapSnapshotResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNetmapSnapshot), wReq, wResp, opts...)
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
