package acl

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// RequestHeaders represents key-value header map of the request.
type RequestHeaders interface {
	// GetRequestHeaderByKey returns value of the request header by key or zero if
	// the header is missing.
	GetRequestHeaderByKey(key string) string
}

var (
	errHeaderNotAvailable = errors.New("unavailable header")
	errHeaderNotFound     = errors.New("header not found")
)

type objectHeadersContext struct {
	// static

	node Node

	op acl.Op

	cnr cid.ID

	objID *oid.ID

	// dynamic

	localObjUnavailable bool

	objHeaders *object.Object
}

func newObjectHeadersContext(node Node, cnr cid.ID, payload ContainerOpPayload) objectHeadersContext {
	return objectHeadersContext{
		node:       node,
		op:         payload.op,
		cnr:        cnr,
		objID:      payload.objID,
		objHeaders: payload.objHdrs,
	}
}

// returns string value of the object header described by the specified key. If
// object headers are not presented in the op payload, local object from the node
// is used (if available). Returns:
//   - errHeaderNotAvailable if value cannot be gotten in all possible ways
//   - errHeaderNotFound if object header with the specified key is missing
//     in the object
func (x *objectHeadersContext) getObjectHeaderByKey(key string) (string, error) {
	if key == v2acl.FilterObjectContainerID {
		return x.cnr.EncodeToString(), nil
	}

	if x.objHeaders == nil {
		if x.localObjUnavailable {
			// we already tried, see code below
			return "", errHeaderNotAvailable
		}

		if x.objID == nil {
			// operation context is wider than single object, so we cannot process
			// headers of the particular object
			return "", errHeaderNotAvailable
		}

		// try to get object headers from the local node's storage
		hdr, err := x.node.ReadLocalObjectHeaders(x.cnr, *x.objID) // nil checked in switch above
		if err != nil {
			// cache failure of local storage op to prevent undesired repeat:
			// recall will stop on switch above
			x.localObjUnavailable = true
			return "", fmt.Errorf("%w: %v", errHeaderNotAvailable, err)
		}

		x.objHeaders = &hdr
	}

	if !strings.HasPrefix(key, v2acl.ObjectFilterPrefix) {
		attrs := x.objHeaders.Attributes()
		for i := range attrs {
			if attrs[i].Key() == key {
				return attrs[i].Value(), nil
			}
		}

		return "", errHeaderNotFound
	}

	switch key {
	default:
		return "", fmt.Errorf("unsupported header %q", key)
	case v2acl.FilterObjectVersion:
		ver := x.objHeaders.Version()
		if ver == nil {
			return "", errHeaderNotFound
		}

		return version.EncodeToString(*ver), nil
	case v2acl.FilterObjectID:
		if x.objID == nil {
			if x.op != acl.OpObjectPut {
				// PUT request is the only case when object ID may be missing while header is
				// available (see PutRequest func), so any other case is worth panic
				panic(fmt.Sprintf("missing object ID in op %s", x.op))
			}

			return "", errHeaderNotFound
		}

		return x.objID.EncodeToString(), nil
	case v2acl.FilterObjectOwnerID:
		owner := x.objHeaders.OwnerID()
		if owner == nil {
			return "", errHeaderNotFound
		}

		return owner.EncodeToString(), nil
	case v2acl.FilterObjectCreationEpoch:
		return strconv.FormatUint(x.objHeaders.CreationEpoch(), 10), nil
	case v2acl.FilterObjectPayloadLength:
		return strconv.FormatUint(x.objHeaders.PayloadSize(), 10), nil
	case v2acl.FilterObjectType:
		return x.objHeaders.Type().EncodeToString(), nil
	case v2acl.FilterObjectPayloadHash:
		cs, set := x.objHeaders.PayloadChecksum()
		if !set {
			return "", errHeaderNotFound
		}

		return hex.EncodeToString(cs.Value()), nil
	case v2acl.FilterObjectHomomorphicHash:
		cs, set := x.objHeaders.PayloadChecksum()
		if !set {
			return "", errHeaderNotFound
		}

		return hex.EncodeToString(cs.Value()), nil
	}
}
