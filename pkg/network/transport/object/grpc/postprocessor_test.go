package object

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testPostProcessorEntity struct {
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

var _ requestPostProcessor = (*testPostProcessorEntity)(nil)

func (s *testPostProcessorEntity) postProcess(_ context.Context, req serviceRequest, e error) {
	if s.f != nil {
		s.f(req, e)
	}
}

func TestComplexPostProcessor_PostProcess(t *testing.T) {
	ctx := context.TODO()

	t.Run("empty request argument", func(t *testing.T) {
		require.PanicsWithValue(t, pmEmptyServiceRequest, func() {
			// ascertain that nil request causes panic
			new(complexPostProcessor).postProcess(ctx, nil, nil)
		})
	})

	t.Run("correct behavior", func(t *testing.T) {
		// create serviceRequest instance.
		req := new(testPostProcessorEntity)

		// create custom error
		pErr := errors.New("test error for post processor")

		// create list of post processors
		postProcCount := 10
		postProcessors := make([]requestPostProcessor, 0, postProcCount)

		postProcessorCalls := make([]struct{}, 0, postProcCount)

		for i := 0; i < postProcCount; i++ {
			postProcessors = append(postProcessors, &testPostProcessorEntity{
				f: func(items ...interface{}) {
					t.Run("correct arguments", func(t *testing.T) {
						postProcessorCalls = append(postProcessorCalls, struct{}{})
					})
				},
			})
		}

		s := &complexPostProcessor{list: postProcessors}

		s.postProcess(ctx, req, pErr)

		// ascertain all internal requestPostProcessor instances were called
		require.Len(t, postProcessorCalls, postProcCount)
	})
}

func Test_newPostProcessor(t *testing.T) {
	res := newPostProcessor()

	pp := res.(*complexPostProcessor)
	require.Len(t, pp.list, 0)
}
