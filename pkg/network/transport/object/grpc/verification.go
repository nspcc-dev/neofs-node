package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/service"
)

type (
	verifyRequestFunc func(token service.RequestVerifyData) error

	// verifyPreProcessor is an implementation of requestPreProcessor interface.
	verifyPreProcessor struct {
		// Verifying function.
		fVerify verifyRequestFunc
	}
)

var _ requestPreProcessor = (*verifyPreProcessor)(nil)

// requestPreProcessor method implementation.
//
// Panics with pmEmptyServiceRequest on empty request.
//
// Returns result of internal requestVerifyFunc instance.
func (s *verifyPreProcessor) preProcess(_ context.Context, req serviceRequest) (err error) {
	if req == nil {
		panic(pmEmptyServiceRequest)
	}

	if err = s.fVerify(req); err != nil {
		err = errUnauthenticated
	}

	return
}
