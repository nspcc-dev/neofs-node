package transport

import (
	"context"
	"io"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
)

type (
	// ObjectTransport is an interface of the executor of object remote operations.
	ObjectTransport interface {
		Transport(context.Context, ObjectTransportParams)
	}

	// ObjectTransportParams groups the parameters of remote object operation.
	ObjectTransportParams struct {
		TransportInfo MetaInfo
		TargetNode    multiaddr.Multiaddr
		ResultHandler ResultHandler
	}

	// ResultHandler is an interface of remote object operation's result handler.
	ResultHandler interface {
		HandleResult(context.Context, multiaddr.Multiaddr, interface{}, error)
	}

	// MetaInfo is an interface of the container of cross-operation values.
	MetaInfo interface {
		GetTTL() uint32
		GetTimeout() time.Duration
		service.SessionTokenSource
		GetRaw() bool
		Type() object.RequestType
		service.BearerTokenSource
		service.ExtendedHeadersSource
	}

	// SearchInfo is an interface of the container of object Search operation parameters.
	SearchInfo interface {
		MetaInfo
		GetCID() refs.CID
		GetQuery() []byte
	}

	// PutInfo is an interface of the container of object Put operation parameters.
	PutInfo interface {
		MetaInfo
		GetHead() *object.Object
		Payload() io.Reader
		CopiesNumber() uint32
	}

	// AddressInfo is an interface of the container of object request by Address.
	AddressInfo interface {
		MetaInfo
		GetAddress() refs.Address
	}

	// GetInfo is an interface of the container of object Get operation parameters.
	GetInfo interface {
		AddressInfo
	}

	// HeadInfo is an interface of the container of object Head operation parameters.
	HeadInfo interface {
		GetInfo
		GetFullHeaders() bool
	}

	// RangeInfo is an interface of the container of object GetRange operation parameters.
	RangeInfo interface {
		AddressInfo
		GetRange() object.Range
	}

	// RangeHashInfo is an interface of the container of object GetRangeHash operation parameters.
	RangeHashInfo interface {
		AddressInfo
		GetRanges() []object.Range
		GetSalt() []byte
	}
)

const (
	// KeyID is a filter key to object ID field.
	KeyID = "ID"

	// KeyTombstone is a filter key to tombstone header.
	KeyTombstone = "TOMBSTONE"

	// KeyStorageGroup is a filter key to storage group link.
	KeyStorageGroup = "STORAGE_GROUP"

	// KeyNoChildren is a filter key to objects w/o child links.
	KeyNoChildren = "LEAF"

	// KeyParent is a filter key to parent link.
	KeyParent = "PARENT"

	// KeyHasParent is a filter key to objects with parent link.
	KeyHasParent = "HAS_PAR"
)
