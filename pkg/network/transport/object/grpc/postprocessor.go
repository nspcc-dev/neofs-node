package object

import (
	"context"
)

type (
	// requestPostProcessor is an interface of RPC call outcome handler.
	requestPostProcessor interface {
		// Performs actions based on the outcome of request processing.
		postProcess(context.Context, serviceRequest, error)
	}

	// complexPostProcessor is an implementation of requestPostProcessor interface.
	complexPostProcessor struct {
		// Sequence of requestPostProcessor instances.
		list []requestPostProcessor
	}
)

var _ requestPostProcessor = (*complexPostProcessor)(nil)

// requestPostProcessor method implementation.
//
// Panics with pmEmptyServiceRequest on nil request argument.
//
// Passes request through the sequence of requestPostProcessor instances.
//
// Warn: adding instance to list itself provoke endless recursion.
func (s *complexPostProcessor) postProcess(ctx context.Context, req serviceRequest, e error) {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	for i := range s.list {
		s.list[i].postProcess(ctx, req, e)
	}
}

// Creates requestPostProcessor based on Params.
//
// Uses complexPostProcessor instance as a result implementation.
func newPostProcessor() requestPostProcessor {
	return &complexPostProcessor{
		list: []requestPostProcessor{},
	}
}
