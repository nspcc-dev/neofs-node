package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testPreProcessorEntity struct {
		// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.
		serviceRequest
		Placer
		storage.AddressStoreComponent
		EpochReceiver

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var _ requestPreProcessor = (*testPreProcessorEntity)(nil)

func (s *testPreProcessorEntity) preProcess(_ context.Context, req serviceRequest) error {
	if s.f != nil {
		s.f(req)
	}
	return s.err
}

func TestSigningPreProcessor_preProcess(t *testing.T) {
	ctx := context.TODO()

	req := new(object.SearchRequest)

	t.Run("internal pre-processor error", func(t *testing.T) {
		ppErr := errors.New("test error for pre-processor")

		s := &signingPreProcessor{
			preProc: &testPreProcessorEntity{
				f: func(items ...interface{}) {
					t.Run("correct internal pre-processor params", func(t *testing.T) {
						require.Equal(t, req, items[0].(serviceRequest))
					})
				},
				err: ppErr,
			},
		}

		require.EqualError(t, s.preProcess(ctx, req), ppErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		key := test.DecodeKey(0)

		exp := signRequest(key, req)

		s := &signingPreProcessor{
			preProc: new(testPreProcessorEntity),
			key:     key,
		}

		require.Equal(t, exp, s.preProcess(ctx, req))
	})
}

func TestComplexPreProcessor_PreProcess(t *testing.T) {
	ctx := context.TODO()

	t.Run("empty request argument", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			// ascertain that nil request causes panic
			_ = new(complexPreProcessor).preProcess(ctx, nil)
		})
	})

	// create serviceRequest instance.
	req := new(testPreProcessorEntity)

	t.Run("empty list", func(t *testing.T) {
		require.NoError(t, new(complexPreProcessor).preProcess(ctx, req))
	})

	t.Run("non-empty list", func(t *testing.T) {
		firstCalled := false
		p1 := &testPreProcessorEntity{
			f: func(items ...interface{}) {
				t.Run("correct nested pre processor params", func(t *testing.T) {
					require.Equal(t, req, items[0].(serviceRequest))
				})

				firstCalled = true // mark first requestPreProcessor call
			},
			err: nil, // force requestPreProcessor to return nil error
		}

		// create custom error
		pErr := errors.New("pre processor error for test")
		p2 := &testPreProcessorEntity{
			err: pErr, // force second requestPreProcessor to return created error
		}

		thirdCalled := false
		p3 := &testPreProcessorEntity{
			f: func(_ ...interface{}) {
				thirdCalled = true // mark third requestPreProcessor call
			},
			err: nil, // force requestPreProcessor to return nil error
		}

		// create complex requestPreProcessor
		p := &complexPreProcessor{
			list: []requestPreProcessor{p1, p2, p3}, // order is important
		}

		// ascertain error returns as expected
		require.EqualError(t,
			p.preProcess(ctx, req),
			pErr.Error(),
		)

		// ascertain first requestPreProcessor was called
		require.True(t, firstCalled)

		// ascertain first requestPreProcessor was not called
		require.False(t, thirdCalled)
	})
}
