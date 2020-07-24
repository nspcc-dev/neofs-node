package object

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport/storagegroup"
	"github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// group of value for status error construction.
type statusInfo struct {
	// status code
	c codes.Code
	// error message
	m string
	// error details
	d []proto.Message
}

type requestError struct {
	// type of request
	t object.RequestType
	// request handler error
	e error
}

// error implementation used for details attaching.
type detailedError struct {
	error

	d []proto.Message
}

type statusCalculator struct {
	*sync.RWMutex

	common map[error]*statusInfo

	custom map[requestError]*statusInfo
}

const panicLogMsg = "rpc handler caused panic"

const msgServerPanic = "panic occurred during request processing"

var errServerPanic = errors.New("panic on call handler")

const msgUnauthenticated = "request does not have valid authentication credentials for the operation"

var errUnauthenticated = errors.New("unauthenticated request")

const msgReSigning = "server could not re-sign request"

var errReSigning = errors.New("could not re-sign request")

const msgInvalidTTL = "invalid TTL value"

var errInvalidTTL = errors.New("invalid TTL value")

const (
	msgNotLocalContainer  = "server is not presented in container"
	descNotLocalContainer = "server is outside container"
)

var errNotLocalContainer = errors.New("not local container")

const msgContainerAffiliationProblem = "server could not check container affiliation"

var errContainerAffiliationProblem = errors.New("could not check container affiliation")

const (
	msgContainerNotFound  = "container not found"
	descContainerNotFound = "handling a non-existent container"
)

var errContainerNotFound = errors.New("container not found")

const msgPlacementProblem = "there were problems building the placement vector on the server"

var errPlacementProblem = errors.New("could not traverse over container")

const msgOverloaded = "system resource overloaded"

var errOverloaded = errors.New("system resource overloaded")

const msgAccessDenied = "access to requested operation is denied"

var errAccessDenied = errors.New("access denied")

const msgPutMessageProblem = "invalid message type"

var msgPutNilObject = "object is null"

const msgCutObjectPayload = "lack of object payload data"

const (
	msgMissingTokenKeys = "missing public keys in token"
	msgBrokenToken      = "token structure failed verification"
	msgTokenObjectID    = "missing object ID in token"
)

const msgProcPayloadSize = "max payload size of processing object overflow"

var errProcPayloadSize = errors.New("max processing object payload size overflow")

const msgObjectCreationEpoch = "invalid creation epoch of object"

var errObjectFromTheFuture = errors.New("object from the future")

const msgObjectPayloadSize = "max object payload size overflow"

var errObjectPayloadSize = errors.New("max object payload size overflow")

const msgLocalStorageOverflow = "not enough space in local storage"

var errLocalStorageOverflow = errors.New("local storage overflow")

const msgPayloadChecksum = "invalid payload checksum"

var errPayloadChecksum = errors.New("invalid payload checksum")

const msgObjectHeadersVerification = "object headers failed verification"

var errObjectHeadersVerification = errors.New("object headers failed verification")

const msgForwardPutObject = "forward object failure"

const msgPutLocalFailure = "local object put failure"

var errPutLocal = errors.New("local object put failure")

const msgPrivateTokenRecv = "private token receive failure"

const msgInvalidSGLinking = "invalid storage group headers"

const msgIncompleteSGInfo = "collect storage group info failure"

const msgTransformationFailure = "object preparation failure"

const msgWrongSGSize = "wrong storage group size"

var errWrongSGSize = errors.New("wrong storage group size")

const msgWrongSGHash = "wrong storage group homomorphic hash"

var errWrongSGHash = errors.New("wrong storage group homomorphic hash")

const msgObjectNotFound = "object not found"

const msgObjectHeaderNotFound = "object header not found"

const msgNonAssembly = "assembly option is not enabled on the server"

const msgPayloadOutOfRange = "range is out of object payload bounds"

const msgPayloadRangeNotFound = "object payload range not found"

var errPayloadRangeNotFound = errors.New("object payload range not found")

const msgMissingToken = "missing token in request"

const msgPutTombstone = "could not store tombstone"

const msgDeletePrepare = "delete information preparation failure"

var errDeletePrepare = errors.New("delete information preparation failure")

const msgQueryVersion = "unsupported query version"

const msgSearchQueryUnmarshal = "query unmarshal failure"

const msgLocalQueryImpose = "local query imposing failure"

var mStatusCommon = map[error]*statusInfo{
	// RPC implementation recovered panic
	errServerPanic: {
		c: codes.Internal,
		m: msgServerPanic,
	},
	// Request authentication credentials problem
	errUnauthenticated: {
		c: codes.Unauthenticated,
		m: msgUnauthenticated,
		d: requestAuthDetails(),
	},
	// Request re-signing problem
	errReSigning: {
		c: codes.Internal,
		m: msgReSigning,
	},
	// Invalid request TTL
	errInvalidTTL: {
		c: codes.InvalidArgument,
		m: msgInvalidTTL,
		d: invalidTTLDetails(),
	},
	// Container affiliation check problem
	errContainerAffiliationProblem: {
		c: codes.Internal,
		m: msgContainerAffiliationProblem,
	},
	// Server is outside container
	errNotLocalContainer: {
		c: codes.FailedPrecondition,
		m: msgNotLocalContainer,
		d: containerAbsenceDetails(),
	},
	// Container not found in storage
	errContainerNotFound: {
		c: codes.NotFound,
		m: msgContainerNotFound,
	},
	// Container placement build problem
	errPlacementProblem: {
		c: codes.Internal,
		m: msgPlacementProblem,
	},
	// System resource overloaded
	errOverloaded: {
		c: codes.Unavailable,
		m: msgOverloaded,
	},
	// Access violations
	errAccessDenied: {
		c: codes.PermissionDenied,
		m: msgAccessDenied,
	},
	// Maximum processing payload size overflow
	errProcPayloadSize: {
		c: codes.FailedPrecondition,
		m: msgProcPayloadSize,
		d: nil, // TODO: NSPCC-1048
	},
}

var mStatusCustom = map[requestError]*statusInfo{
	// Invalid first message in Put client stream
	{
		t: object.RequestPut,
		e: errHeaderExpected,
	}: {
		c: codes.InvalidArgument,
		m: msgPutMessageProblem,
		d: putFirstMessageDetails(),
	},
	// Nil object in Put request
	{
		t: object.RequestPut,
		e: errObjectExpected,
	}: {
		c: codes.InvalidArgument,
		m: msgPutNilObject,
		d: putNilObjectDetails(),
	},
	// Lack of object payload data
	{
		t: object.RequestPut,
		e: transformer.ErrPayloadEOF,
	}: {
		c: codes.InvalidArgument,
		m: msgCutObjectPayload,
		d: payloadSizeDetails(),
	},
	// Lack of public keys in the token
	{
		t: object.RequestPut,
		e: errMissingOwnerKeys,
	}: {
		c: codes.PermissionDenied,
		m: msgMissingTokenKeys,
		d: tokenKeysDetails(),
	},
	// Broken token structure
	{
		t: object.RequestPut,
		e: errBrokenToken,
	}: {
		c: codes.PermissionDenied,
		m: msgBrokenToken,
	},
	// Missing object ID in token
	{
		t: object.RequestPut,
		e: errWrongTokenAddress,
	}: {
		c: codes.PermissionDenied,
		m: msgTokenObjectID,
		d: tokenOIDDetails(),
	},
	// Invalid after-first message in stream
	{
		t: object.RequestPut,
		e: errChunkExpected,
	}: {
		c: codes.InvalidArgument,
		m: msgPutMessageProblem,
		d: putChunkMessageDetails(),
	},
	{
		t: object.RequestPut,
		e: errObjectFromTheFuture,
	}: {
		c: codes.FailedPrecondition,
		m: msgObjectCreationEpoch,
		d: nil, // TODO: NSPCC-1048
	},
	{
		t: object.RequestPut,
		e: errObjectPayloadSize,
	}: {
		c: codes.FailedPrecondition,
		m: msgObjectPayloadSize,
		d: nil, // TODO: NSPCC-1048
	},
	{
		t: object.RequestPut,
		e: errLocalStorageOverflow,
	}: {
		c: codes.Unavailable,
		m: msgLocalStorageOverflow,
		d: localStorageOverflowDetails(),
	},
	{
		t: object.RequestPut,
		e: errPayloadChecksum,
	}: {
		c: codes.InvalidArgument,
		m: msgPayloadChecksum,
		d: payloadChecksumHeaderDetails(),
	},
	{
		t: object.RequestPut,
		e: errObjectHeadersVerification,
	}: {
		c: codes.InvalidArgument,
		m: msgObjectHeadersVerification,
	},
	{
		t: object.RequestPut,
		e: errIncompleteOperation,
	}: {
		c: codes.Unavailable,
		m: msgForwardPutObject,
	},
	{
		t: object.RequestPut,
		e: errPutLocal,
	}: {
		c: codes.Internal,
		m: msgPutLocalFailure,
	},
	{
		t: object.RequestPut,
		e: errTokenRetrieval,
	}: {
		c: codes.Aborted,
		m: msgPrivateTokenRecv,
	},
	{
		t: object.RequestPut,
		e: transformer.ErrInvalidSGLinking,
	}: {
		c: codes.InvalidArgument,
		m: msgInvalidSGLinking,
		d: sgLinkingDetails(),
	},
	{
		t: object.RequestPut,
		e: storagegroup.ErrIncompleteSGInfo,
	}: {
		c: codes.NotFound,
		m: msgIncompleteSGInfo,
	},
	{
		t: object.RequestPut,
		e: errTransformer,
	}: {
		c: codes.Internal,
		m: msgTransformationFailure,
	},
	{
		t: object.RequestPut,
		e: errWrongSGSize,
	}: {
		c: codes.InvalidArgument,
		m: msgWrongSGSize,
	},
	{
		t: object.RequestPut,
		e: errWrongSGHash,
	}: {
		c: codes.InvalidArgument,
		m: msgWrongSGHash,
	},
	{
		t: object.RequestGet,
		e: errIncompleteOperation,
	}: {
		c: codes.NotFound,
		m: msgObjectNotFound,
	},
	{
		t: object.RequestHead,
		e: errIncompleteOperation,
	}: {
		c: codes.NotFound,
		m: msgObjectHeaderNotFound,
	},
	{
		t: object.RequestGet,
		e: errNonAssembly,
	}: {
		c: codes.Unimplemented,
		m: msgNonAssembly,
	},
	{
		t: object.RequestHead,
		e: errNonAssembly,
	}: {
		c: codes.Unimplemented,
		m: msgNonAssembly,
	},
	{
		t: object.RequestGet,
		e: childrenNotFound,
	}: {
		c: codes.NotFound,
		m: msgObjectNotFound,
	},
	{
		t: object.RequestHead,
		e: childrenNotFound,
	}: {
		c: codes.NotFound,
		m: msgObjectHeaderNotFound,
	},
	{
		t: object.RequestRange,
		e: localstore.ErrOutOfRange,
	}: {
		c: codes.OutOfRange,
		m: msgPayloadOutOfRange,
	},
	{
		t: object.RequestRange,
		e: errPayloadRangeNotFound,
	}: {
		c: codes.NotFound,
		m: msgPayloadRangeNotFound,
	},
	{
		t: object.RequestDelete,
		e: errNilToken,
	}: {
		c: codes.InvalidArgument,
		m: msgMissingToken,
		d: missingTokenDetails(),
	},
	{
		t: object.RequestDelete,
		e: errMissingOwnerKeys,
	}: {
		c: codes.PermissionDenied,
		m: msgMissingTokenKeys,
		d: tokenKeysDetails(),
	},
	{
		t: object.RequestDelete,
		e: errBrokenToken,
	}: {
		c: codes.PermissionDenied,
		m: msgBrokenToken,
	},
	{
		t: object.RequestDelete,
		e: errWrongTokenAddress,
	}: {
		c: codes.PermissionDenied,
		m: msgTokenObjectID,
		d: tokenOIDDetails(),
	},
	{
		t: object.RequestDelete,
		e: errTokenRetrieval,
	}: {
		c: codes.Aborted,
		m: msgPrivateTokenRecv,
	},
	{
		t: object.RequestDelete,
		e: errIncompleteOperation,
	}: {
		c: codes.Unavailable,
		m: msgPutTombstone,
	},
	{
		t: object.RequestDelete,
		e: errDeletePrepare,
	}: {
		c: codes.Internal,
		m: msgDeletePrepare,
	},
	{
		t: object.RequestSearch,
		e: errUnsupportedQueryVersion,
	}: {
		c: codes.Unimplemented,
		m: msgQueryVersion,
	},
	{
		t: object.RequestSearch,
		e: errSearchQueryUnmarshal,
	}: {
		c: codes.InvalidArgument,
		m: msgSearchQueryUnmarshal,
	},
	{
		t: object.RequestSearch,
		e: errLocalQueryImpose,
	}: {
		c: codes.Internal,
		m: msgLocalQueryImpose,
	},
	{
		t: object.RequestRangeHash,
		e: errPayloadRangeNotFound,
	}: {
		c: codes.NotFound,
		m: msgPayloadRangeNotFound,
	},
	{
		t: object.RequestRangeHash,
		e: localstore.ErrOutOfRange,
	}: {
		c: codes.OutOfRange,
		m: msgPayloadOutOfRange,
	},
}

func serviceStatusCalculator() *statusCalculator {
	s := newStatusCalculator()

	for k, v := range mStatusCommon {
		s.addCommon(k, v)
	}

	for k, v := range mStatusCustom {
		s.addCustom(k, v)
	}

	return s
}

func statusError(v *statusInfo) (bool, error) {
	st, err := status.New(v.c, v.m).WithDetails(v.d...)
	if err != nil {
		return false, nil
	}

	return true, st.Err()
}

func (s *statusCalculator) addCommon(k error, v *statusInfo) {
	s.Lock()
	s.common[k] = v
	s.Unlock()
}

func (s *statusCalculator) addCustom(k requestError, v *statusInfo) {
	s.Lock()
	s.custom[k] = v
	s.Unlock()
}

func (s *statusCalculator) make(e requestError) error {
	s.RLock()
	defer s.RUnlock()

	var (
		ok  bool
		v   *statusInfo
		d   []proto.Message
		err = errors.Cause(e.e)
	)

	if v, ok := err.(*detailedError); ok {
		d = v.d
		err = v.error
	} else if v, ok := err.(detailedError); ok {
		d = v.d
		err = v.error
	}

	if v, ok = s.common[err]; !ok {
		if v, ok = s.custom[requestError{
			t: e.t,
			e: err,
		}]; !ok {
			return e.e
		}
	}

	vv := *v

	vv.d = append(vv.d, d...)

	if ok, res := statusError(&vv); ok {
		return res
	}

	return e.e
}

func newStatusCalculator() *statusCalculator {
	return &statusCalculator{
		RWMutex: new(sync.RWMutex),
		common:  make(map[error]*statusInfo),
		custom:  make(map[requestError]*statusInfo),
	}
}

func requestAuthDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "Signatures",
					Description: "should be formed according to VerificationHeader signing",
				},
			},
		},
	}
}

func invalidTTLDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "TTL",
					Description: "should greater or equal than NonForwardingTTL",
				},
			},
		},
	}
}

func containerAbsenceDetails() []proto.Message {
	return []proto.Message{
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{
					Type:        "container options",
					Subject:     "container nodes",
					Description: "server node should be presented container",
				},
			},
		},
	}
}

func containerDetails(cid CID, desc string) []proto.Message {
	return []proto.Message{
		&errdetails.ResourceInfo{
			ResourceType: "container",
			ResourceName: cid.String(),
			Description:  desc,
		},
	}
}

func putFirstMessageDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R",
					Description: "should be PutRequest_Header",
				},
			},
		},
	}
}

func putChunkMessageDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R",
					Description: "should be PutRequest_Chunk",
				},
				{
					Field:       "R.Chunk",
					Description: "should not be empty",
				},
			},
		},
	}
}

func putNilObjectDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object",
					Description: "should not be null",
				},
			},
		},
	}
}

func payloadSizeDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.SystemHeader.PayloadLength",
					Description: "should be equal to the sum of the sizes of the streaming payload chunks",
				},
			},
		},
	}
}

func tokenKeysDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Token.PublicKeys",
					Description: "should be non-empty list of marshaled ecdsa public keys",
				},
			},
		},
	}
}

func tokenOIDDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Token.ObjectID",
					Description: "should contain requested object",
				},
			},
		},
	}
}

func maxProcPayloadSizeDetails(sz uint64) []proto.Message {
	return []proto.Message{
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{
					Type:        "object requirements",
					Subject:     "max processing payload size",
					Description: fmt.Sprintf("should not be greater than %d bytes", sz),
				},
			},
		},
	}
}

func objectCreationEpochDetails(e uint64) []proto.Message {
	return []proto.Message{
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{
					Type:        "object requirements",
					Subject:     "creation epoch",
					Description: fmt.Sprintf("should not be greater than %d", e),
				},
			},
		},
	}
}

func maxObjectPayloadSizeDetails(sz uint64) []proto.Message {
	return []proto.Message{
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{
					Type:        "object requirements",
					Subject:     "max object payload size",
					Description: fmt.Sprintf("should not be greater than %d bytes", sz),
				},
			},
		},
	}
}

func localStorageOverflowDetails() []proto.Message {
	return []proto.Message{
		&errdetails.ResourceInfo{
			ResourceType: "local storage",
			ResourceName: "disk storage",
			Description:  "not enough space",
		},
	}
}

func payloadChecksumHeaderDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.Headers",
					Description: "should contain correct payload checksum header",
				},
			},
		},
	}
}

func objectHeadersVerificationDetails(e error) []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.Headers",
					Description: e.Error(),
				},
			},
		},
	}
}

func privateTokenRecvDetails(id session.TokenID, owner OwnerID) []proto.Message {
	return []proto.Message{
		&errdetails.ResourceInfo{
			ResourceType: "private token",
			ResourceName: id.String(),
			Owner:        owner.String(),
			Description:  "problems with getting a private token",
		},
	}
}

func sgLinkingDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.Headers",
					Description: "should not contain Header_StorageGroup and Link_StorageGroup or should contain both",
				},
			},
		},
	}
}

func sgSizeDetails(exp, act uint64) []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.Headers",
					Description: fmt.Sprintf("wrong storage group size: expected %d, collected %d", exp, act),
				},
			},
		},
	}
}

func sgHashDetails(exp, act Hash) []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "R.Object.Headers",
					Description: fmt.Sprintf("wrong storage group hash: expected %s, collected %s", exp, act),
				},
			},
		},
	}
}

func missingTokenDetails() []proto.Message {
	return []proto.Message{
		&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "Token",
					Description: "should not be null",
				},
			},
		},
	}
}
