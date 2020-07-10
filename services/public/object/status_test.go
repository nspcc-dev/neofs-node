package object

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/nspcc-dev/neofs-node/lib/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testPanickingHandler struct{}

func (*testPanickingHandler) handleRequest(context.Context, handleRequestParams) (interface{}, error) {
	panic("panicking handler")
}

func TestStatusCalculator(t *testing.T) {
	t.Run("unknown error", func(t *testing.T) {
		e := internal.Error("error for test")

		s := newStatusCalculator()

		require.Equal(t, e, s.make(requestError{
			e: e,
		}))
	})

	t.Run("common error", func(t *testing.T) {
		v := &statusInfo{
			c: codes.Aborted,
			m: "test error message",
			d: []proto.Message{
				&errdetails.ResourceInfo{
					ResourceType: "type",
					ResourceName: "name",
					Owner:        "owner",
					Description:  "description",
				},
			},
		}

		s := newStatusCalculator()

		e := internal.Error("error for test")

		s.addCommon(e, v)

		ok, err := statusError(v)
		require.True(t, ok)

		require.Equal(t,
			err,
			s.make(requestError{
				e: e,
			}),
		)
	})

	t.Run("custom error", func(t *testing.T) {
		var (
			c1, c2 = codes.Aborted, codes.AlreadyExists
			t1, t2 = object.RequestPut, object.RequestGet
			e1, e2 = internal.Error("test error 1"), internal.Error("test error 2")
			m1, m2 = "message 1", "message 2"
		)

		s := newStatusCalculator()

		s1 := &statusInfo{
			c: c1,
			m: m1,
		}

		re1 := requestError{
			t: t1,
			e: e1,
		}

		s.addCustom(re1, s1)

		s2 := &statusInfo{
			c: c2,
			m: m2,
		}

		r2 := requestError{
			t: t2,
			e: e2,
		}

		s.addCustom(r2, s2)

		ok, err1 := statusError(s1)
		require.True(t, ok)

		ok, err2 := statusError(s2)
		require.True(t, ok)

		require.Equal(t,
			err1,
			s.make(re1),
		)

		require.Equal(t,
			err2,
			s.make(r2),
		)
	})
}

func testStatusCommon(t *testing.T, h requestHandler, c codes.Code, m string, d []interface{}) {
	ctx := context.TODO()

	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	errPut := s.Put(&testPutEntity{
		res: object.MakePutRequestHeader(new(Object)),
	})

	errGet := s.Get(new(object.GetRequest), new(testGetEntity))

	_, errHead := s.Head(ctx, new(object.HeadRequest))

	_, errDelete := s.Head(ctx, new(object.HeadRequest))

	errRange := s.GetRange(new(GetRangeRequest), new(testRangeEntity))

	_, errRangeHash := s.GetRangeHash(ctx, new(object.GetRangeHashRequest))

	errSearch := s.Search(new(object.SearchRequest), new(testSearchEntity))

	errs := []error{
		errPut,
		errGet,
		errHead,
		errRange,
		errRangeHash,
		errSearch,
		errDelete,
	}

	for _, err := range errs {
		st, ok := status.FromError(err)
		require.True(t, ok)

		require.Equal(t, c, st.Code())
		require.Equal(t, m, st.Message())
		require.Equal(t, d, st.Details())
	}
}

func TestStatusCommon(t *testing.T) {
	t.Run("handler panic", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			new(testPanickingHandler),
			codes.Internal,
			msgServerPanic,
			ds,
		)
	})

	t.Run("request authentication", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range requestAuthDetails() {
			ds = append(ds, d)
		}

		testStatusCommon(t,
			&testPutEntity{
				err: errUnauthenticated,
			},
			codes.Unauthenticated,
			msgUnauthenticated,
			ds,
		)
	})

	t.Run("re-signing problem", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errReSigning,
			},
			codes.Internal,
			msgReSigning,
			ds,
		)
	})

	t.Run("invalid TTL", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range invalidTTLDetails() {
			ds = append(ds, d)
		}

		testStatusCommon(t,
			&testPutEntity{
				err: errInvalidTTL,
			},
			codes.InvalidArgument,
			msgInvalidTTL,
			ds,
		)
	})

	t.Run("container affiliation problem", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errContainerAffiliationProblem,
			},
			codes.Internal,
			msgContainerAffiliationProblem,
			ds,
		)
	})

	t.Run("container not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errContainerNotFound,
			},
			codes.NotFound,
			msgContainerNotFound,
			ds,
		)
	})

	t.Run("server is missing in container", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range containerAbsenceDetails() {
			ds = append(ds, d)
		}

		testStatusCommon(t,
			&testPutEntity{
				err: errNotLocalContainer,
			},
			codes.FailedPrecondition,
			msgNotLocalContainer,
			ds,
		)
	})

	t.Run("placement problem", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errPlacementProblem,
			},
			codes.Internal,
			msgPlacementProblem,
			ds,
		)
	})

	t.Run("system resource overloaded", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errOverloaded,
			},
			codes.Unavailable,
			msgOverloaded,
			ds,
		)
	})

	t.Run("access denied", func(t *testing.T) {
		ds := make([]interface{}, 0)

		testStatusCommon(t,
			&testPutEntity{
				err: errAccessDenied,
			},
			codes.PermissionDenied,
			msgAccessDenied,
			ds,
		)
	})

	t.Run("max processing payload size overflow", func(t *testing.T) {
		maxSz := uint64(100)

		ds := make([]interface{}, 0)

		for _, d := range maxProcPayloadSizeDetails(maxSz) {
			ds = append(ds, d)
		}

		testStatusCommon(t,
			&testPutEntity{
				err: &detailedError{
					error: errProcPayloadSize,
					d:     maxProcPayloadSizeDetails(maxSz),
				},
			},
			codes.FailedPrecondition,
			msgProcPayloadSize,
			ds,
		)
	})
}

func testStatusPut(t *testing.T, h requestHandler, srv object.Service_PutServer, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	err := s.Put(srv)

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusPut(t *testing.T) {
	t.Run("invalid first message type", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range putFirstMessageDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestChunk(nil),
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgPutMessageProblem,
		}

		testStatusPut(t, nil, srv, info, ds)
	})

	t.Run("invalid first message type", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range putNilObjectDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(nil),
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgPutNilObject,
		}

		testStatusPut(t, nil, srv, info, ds)
	})

	t.Run("invalid first message type", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range payloadSizeDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: transformer.ErrPayloadEOF,
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgCutObjectPayload,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("token w/o public keys", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range tokenKeysDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errMissingOwnerKeys,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgMissingTokenKeys,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("broken token", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errBrokenToken,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgBrokenToken,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("missing object in token", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range tokenOIDDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errWrongTokenAddress,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgTokenObjectID,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("object from future", func(t *testing.T) {
		e := uint64(3)

		ds := make([]interface{}, 0)

		for _, d := range objectCreationEpochDetails(e) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errObjectFromTheFuture,
				d:     objectCreationEpochDetails(e),
			},
		}

		info := statusInfo{
			c: codes.FailedPrecondition,
			m: msgObjectCreationEpoch,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("max object payload size", func(t *testing.T) {
		sz := uint64(3)

		ds := make([]interface{}, 0)

		for _, d := range maxObjectPayloadSizeDetails(sz) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errObjectPayloadSize,
				d:     maxObjectPayloadSizeDetails(sz),
			},
		}

		info := statusInfo{
			c: codes.FailedPrecondition,
			m: msgObjectPayloadSize,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("local storage overflow", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range localStorageOverflowDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errLocalStorageOverflow,
		}

		info := statusInfo{
			c: codes.Unavailable,
			m: msgLocalStorageOverflow,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("invalid payload checksum", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range payloadChecksumHeaderDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errPayloadChecksum,
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgPayloadChecksum,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("invalid object header structure", func(t *testing.T) {
		e := internal.Error("test error")

		ds := make([]interface{}, 0)

		for _, d := range objectHeadersVerificationDetails(e) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errObjectHeadersVerification,
				d:     objectHeadersVerificationDetails(e),
			},
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgObjectHeadersVerification,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("put generated object failure", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errIncompleteOperation,
		}

		info := statusInfo{
			c: codes.Unavailable,
			m: msgForwardPutObject,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("private token receive failure", func(t *testing.T) {
		owner := OwnerID{1, 2, 3}
		tokenID := session.TokenID{4, 5, 6}

		ds := make([]interface{}, 0)

		for _, d := range privateTokenRecvDetails(tokenID, owner) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errTokenRetrieval,
				d:     privateTokenRecvDetails(tokenID, owner),
			},
		}

		info := statusInfo{
			c: codes.Aborted,
			m: msgPrivateTokenRecv,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("invalid SG headers", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range sgLinkingDetails() {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: transformer.ErrInvalidSGLinking,
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgInvalidSGLinking,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("incomplete SG info", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: implementations.ErrIncompleteSGInfo,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgIncompleteSGInfo,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("object transformation failure", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: errTransformer,
		}

		info := statusInfo{
			c: codes.Internal,
			m: msgTransformationFailure,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("wrong SG size", func(t *testing.T) {
		var exp, act uint64 = 1, 2

		ds := make([]interface{}, 0)

		for _, d := range sgSizeDetails(exp, act) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errWrongSGSize,
				d:     sgSizeDetails(exp, act),
			},
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgWrongSGSize,
		}

		testStatusPut(t, h, srv, info, ds)
	})

	t.Run("wrong SG size", func(t *testing.T) {
		var exp, act = Hash{1}, Hash{2}

		ds := make([]interface{}, 0)

		for _, d := range sgHashDetails(exp, act) {
			ds = append(ds, d)
		}

		srv := &testPutEntity{
			res: object.MakePutRequestHeader(new(Object)),
		}

		h := &testPutEntity{
			err: &detailedError{
				error: errWrongSGHash,
				d:     sgHashDetails(exp, act),
			},
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgWrongSGHash,
		}

		testStatusPut(t, h, srv, info, ds)
	})
}

func testStatusGet(t *testing.T, h requestHandler, srv object.Service_GetServer, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	err := s.Get(new(object.GetRequest), srv)

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusGet(t *testing.T) {
	t.Run("object not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testGetEntity)

		h := &testGetEntity{
			err: errIncompleteOperation,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgObjectNotFound,
		}

		testStatusGet(t, h, srv, info, ds)
	})

	t.Run("non-assembly", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testGetEntity)

		h := &testGetEntity{
			err: errNonAssembly,
		}

		info := statusInfo{
			c: codes.Unimplemented,
			m: msgNonAssembly,
		}

		testStatusGet(t, h, srv, info, ds)
	})

	t.Run("children not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testGetEntity)

		h := &testGetEntity{
			err: childrenNotFound,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgObjectNotFound,
		}

		testStatusGet(t, h, srv, info, ds)
	})
}

func testStatusHead(t *testing.T, h requestHandler, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	_, err := s.Head(context.TODO(), new(object.HeadRequest))

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusHead(t *testing.T) {
	t.Run("object not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errIncompleteOperation,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgObjectHeaderNotFound,
		}

		testStatusHead(t, h, info, ds)
	})

	t.Run("non-assembly", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errNonAssembly,
		}

		info := statusInfo{
			c: codes.Unimplemented,
			m: msgNonAssembly,
		}

		testStatusHead(t, h, info, ds)
	})

	t.Run("children not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: childrenNotFound,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgObjectHeaderNotFound,
		}

		testStatusHead(t, h, info, ds)
	})
}

func testStatusGetRange(t *testing.T, h requestHandler, srv object.Service_GetRangeServer, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	err := s.GetRange(new(GetRangeRequest), srv)

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusGetRange(t *testing.T) {
	t.Run("payload range is out of bounds", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testRangeEntity)

		h := &testRangeEntity{
			err: localstore.ErrOutOfRange,
		}

		info := statusInfo{
			c: codes.OutOfRange,
			m: msgPayloadOutOfRange,
		}

		testStatusGetRange(t, h, srv, info, ds)
	})

	t.Run("payload range not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testRangeEntity)

		h := &testRangeEntity{
			err: errPayloadRangeNotFound,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgPayloadRangeNotFound,
		}

		testStatusGetRange(t, h, srv, info, ds)
	})
}

func testStatusDelete(t *testing.T, h requestHandler, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	_, err := s.Delete(context.TODO(), new(object.DeleteRequest))

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusDelete(t *testing.T) {
	t.Run("missing token", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range missingTokenDetails() {
			ds = append(ds, d)
		}

		h := &testHeadEntity{
			err: errNilToken,
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgMissingToken,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("missing public keys in token", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range tokenKeysDetails() {
			ds = append(ds, d)
		}

		h := &testHeadEntity{
			err: errMissingOwnerKeys,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgMissingTokenKeys,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("broken token structure", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errBrokenToken,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgBrokenToken,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("missing object ID in token", func(t *testing.T) {
		ds := make([]interface{}, 0)

		for _, d := range tokenOIDDetails() {
			ds = append(ds, d)
		}

		h := &testHeadEntity{
			err: errWrongTokenAddress,
		}

		info := statusInfo{
			c: codes.PermissionDenied,
			m: msgTokenObjectID,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("private token receive", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errTokenRetrieval,
		}

		info := statusInfo{
			c: codes.Aborted,
			m: msgPrivateTokenRecv,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("incomplete tombstone put", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errIncompleteOperation,
		}

		info := statusInfo{
			c: codes.Unavailable,
			m: msgPutTombstone,
		}

		testStatusDelete(t, h, info, ds)
	})

	t.Run("delete preparation failure", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testHeadEntity{
			err: errDeletePrepare,
		}

		info := statusInfo{
			c: codes.Internal,
			m: msgDeletePrepare,
		}

		testStatusDelete(t, h, info, ds)
	})
}

func testStatusSearch(t *testing.T, h requestHandler, srv object.Service_SearchServer, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	err := s.Search(new(object.SearchRequest), srv)

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusSearch(t *testing.T) {
	t.Run("unsupported query version", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testSearchEntity)

		h := &testSearchEntity{
			err: errUnsupportedQueryVersion,
		}

		info := statusInfo{
			c: codes.Unimplemented,
			m: msgQueryVersion,
		}

		testStatusSearch(t, h, srv, info, ds)
	})

	t.Run("query unmarshal failure", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testSearchEntity)

		h := &testSearchEntity{
			err: errSearchQueryUnmarshal,
		}

		info := statusInfo{
			c: codes.InvalidArgument,
			m: msgSearchQueryUnmarshal,
		}

		testStatusSearch(t, h, srv, info, ds)
	})

	t.Run("query imposing problems", func(t *testing.T) {
		ds := make([]interface{}, 0)

		srv := new(testSearchEntity)

		h := &testSearchEntity{
			err: errLocalQueryImpose,
		}

		info := statusInfo{
			c: codes.Internal,
			m: msgLocalQueryImpose,
		}

		testStatusSearch(t, h, srv, info, ds)
	})
}

func testStatusGetRangeHash(t *testing.T, h requestHandler, info statusInfo, d []interface{}) {
	s := &objectService{
		log:              test.NewTestLogger(false),
		requestHandler:   h,
		statusCalculator: serviceStatusCalculator(),
	}

	_, err := s.GetRangeHash(context.TODO(), new(object.GetRangeHashRequest))

	st, ok := status.FromError(err)
	require.True(t, ok)

	require.Equal(t, info.c, st.Code())
	require.Equal(t, info.m, st.Message())
	require.Equal(t, d, st.Details())
}

func TestStatusGetRangeHash(t *testing.T) {
	t.Run("payload range not found", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testRangeEntity{
			err: errPayloadRangeNotFound,
		}

		info := statusInfo{
			c: codes.NotFound,
			m: msgPayloadRangeNotFound,
		}

		testStatusGetRangeHash(t, h, info, ds)
	})

	t.Run("range out-of-bounds", func(t *testing.T) {
		ds := make([]interface{}, 0)

		h := &testRangeEntity{
			err: localstore.ErrOutOfRange,
		}

		info := statusInfo{
			c: codes.OutOfRange,
			m: msgPayloadOutOfRange,
		}

		testStatusGetRangeHash(t, h, info, ds)
	})
}
