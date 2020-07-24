package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testVerificationEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

func Test_verifyPreProcessor_preProcess(t *testing.T) {
	ctx := context.TODO()

	t.Run("empty request", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			_ = new(verifyPreProcessor).preProcess(ctx, nil)
		})
	})

	t.Run("correct result", func(t *testing.T) {
		t.Run("failure", func(t *testing.T) {
			// create custom error
			vErr := errors.New("test error for verifying func")

			s := &verifyPreProcessor{
				fVerify: func(service.RequestVerifyData) error { return vErr }, // force requestVerifyFunc to return vErr
			}

			// ascertain that error returns as expected
			require.EqualError(t,
				s.preProcess(ctx, new(testVerificationEntity)),
				errUnauthenticated.Error(),
			)
		})

		t.Run("success", func(t *testing.T) {
			s := &verifyPreProcessor{
				fVerify: func(service.RequestVerifyData) error { return nil }, // force requestVerifyFunc to return nil
			}

			// ascertain that nil error returns as expected
			require.NoError(t, s.preProcess(ctx, new(testVerificationEntity)))
		})
	})
}
