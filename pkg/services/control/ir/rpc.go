package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/common"
)

const serviceName = "ircontrol.ControlService"

const (
	rpcHealthCheck   = "HealthCheck"
	rpcNotaryList    = "NotaryList"
	rpcNotaryRequest = "NotaryRequest"
	rpcNotarySign    = "NotarySign"
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

// NotaryList executes ControlService.NotaryList RPC.
func NotaryList(
	cli *client.Client,
	req *NotaryListRequest,
	opts ...client.CallOption,
) (*NotaryListResponse, error) {
	wResp := &notaryListResponseWrapper{
		m: new(NotaryListResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNotaryList), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// NotaryRequest executes ControlService.NotaryRequest RPC.
func NotaryRequest(
	cli *client.Client,
	req *NotaryRequestRequest,
	opts ...client.CallOption,
) (*NotaryRequestResponse, error) {
	wResp := &notaryRequestResponseWrapper{
		m: new(NotaryRequestResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNotaryRequest), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}

// NotarySign executes ControlService.NotarySign RPC.
func NotarySign(
	cli *client.Client,
	req *NotarySignRequest,
	opts ...client.CallOption,
) (*NotarySignResponse, error) {
	wResp := &notarySignResponseWrapper{
		m: new(NotarySignResponse),
	}

	wReq := &requestWrapper{
		m: req,
	}

	err := client.SendUnary(cli, common.CallMethodInfoUnary(serviceName, rpcNotarySign), wReq, wResp, opts...)
	if err != nil {
		return nil, err
	}

	return wResp.m, nil
}
