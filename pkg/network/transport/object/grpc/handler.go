package object

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/object"
)

type (
	// requestHandler is an interface of Object service cross-request handler.
	requestHandler interface {
		// Handles request by parameter-bound logic.
		handleRequest(context.Context, handleRequestParams) (interface{}, error)
	}

	handleRequestParams struct {
		// Processing request.
		request serviceRequest

		// Processing request executor.
		executor requestHandleExecutor
	}

	// coreRequestHandler is an implementation of requestHandler interface used in Object service production.
	coreRequestHandler struct {
		// Request preprocessor.
		preProc requestPreProcessor

		// Request postprocessor.
		postProc requestPostProcessor
	}

	// requestHandleExecutor is an interface of universal Object operation executor.
	requestHandleExecutor interface {
		// Executes actions parameter-bound logic and returns execution result.
		executeRequest(context.Context, serviceRequest) (interface{}, error)
	}
)

var _ requestHandler = (*coreRequestHandler)(nil)

// requestHandler method implementation.
//
// If internal requestPreProcessor returns non-nil error for request argument, it returns.
// Otherwise, requestHandleExecutor argument performs actions. Received error is passed to requestPoistProcessor routine.
// Returned results of requestHandleExecutor are return.
func (s *coreRequestHandler) handleRequest(ctx context.Context, p handleRequestParams) (interface{}, error) {
	if err := s.preProc.preProcess(ctx, p.request); err != nil {
		return nil, err
	}

	res, err := p.executor.executeRequest(ctx, p.request)

	go s.postProc.postProcess(ctx, p.request, err)

	return res, err
}

// TODO: separate executors for each operation
// requestHandleExecutor method implementation.
func (s *objectService) executeRequest(ctx context.Context, req serviceRequest) (interface{}, error) {
	switch r := req.(type) {
	case *object.SearchRequest:
		return s.objSearcher.searchObjects(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pSrch.Timeout,
		})
	case *putRequest:
		addr, err := s.objStorer.putObject(ctx, r)
		if err != nil {
			return nil, err
		}

		resp := makePutResponse(*addr)
		if err := s.respPreparer.prepareResponse(ctx, r.PutRequest, resp); err != nil {
			return nil, err
		}

		return nil, r.srv.SendAndClose(resp)
	case *object.DeleteRequest:
		return nil, s.objRemover.delete(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pDel.Timeout,
		})
	case *object.GetRequest:
		return s.objRecv.getObject(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pGet.Timeout,
		})
	case *object.HeadRequest:
		return s.objRecv.getObject(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pHead.Timeout,
		})
	case *GetRangeRequest:
		return s.payloadRngRecv.getRangeData(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pRng.Timeout,
		})
	case *object.GetRangeHashRequest:
		return s.rngRecv.getRange(ctx, &transportRequest{
			serviceRequest: r,
			timeout:        s.pRng.Timeout,
		})
	default:
		panic(fmt.Sprintf(pmWrongRequestType, r))
	}
}
