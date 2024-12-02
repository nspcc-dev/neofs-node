package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/common"
)

const serviceName = "ircontrol.ControlService"

const (
	rpcHealthCheck      = "HealthCheck"
	rpcNetworkList      = "NetworkList"
	rpcNetworkEpochTick = "NetworkEpochTick"
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

// NetworkList executes ControlService.NetworkList RPC.
func NetworkList(
	cli *client.Client,
	req *NetworkListRequest,
	opts ...client.CallOption,
) (*NetworkListResponse, error) {
	wResp := &networkListResponseWrapper{
		m: new(NetworkListResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNetworkList), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// NetworkEpochTick executes ControlService.NetworkEpochTick RPC.
func NetworkEpochTick(
	cli *client.Client,
	req *NetworkEpochTickRequest,
	opts ...client.CallOption,
) (*NetworkEpochTickResponse, error) {
	wResp := &networkEpochTickResponseWrapper{
		m: new(NetworkEpochTickResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNetworkEpochTick), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}
