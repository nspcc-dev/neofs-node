package v2

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

type sysObjHdr struct {
	k, v string
}

func (s *sysObjHdr) Key() string {
	return s.k
}

func (s *sysObjHdr) Value() string {
	return s.v
}

func idValue(id *objectSDK.ID) string {
	return id.String()
}

func cidValue(id *container.ID) string {
	return id.String()
}

func ownerIDValue(id *owner.ID) string {
	return id.String()
}

func u64Value(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func headersFromObject(obj *object.Object, addr *objectSDK.Address) []eacl.Header {
	// TODO: optimize allocs
	res := make([]eacl.Header, 0)

	for ; obj != nil; obj = obj.GetParent() {
		res = append(res,
			cidHeader(addr.ContainerID()),
			// owner ID
			&sysObjHdr{
				k: acl.FilterObjectOwnerID,
				v: ownerIDValue(obj.OwnerID()),
			},
			// creation epoch
			&sysObjHdr{
				k: acl.FilterObjectCreationEpoch,
				v: u64Value(obj.CreationEpoch()),
			},
			// payload size
			&sysObjHdr{
				k: acl.FilterObjectPayloadLength,
				v: u64Value(obj.PayloadSize()),
			},
			oidHeader(addr.ObjectID()),
			// TODO: add others fields after neofs-api#84
		)

		attrs := obj.Attributes()
		hs := make([]eacl.Header, 0, len(attrs))

		for i := range attrs {
			hs = append(hs, attrs[i])
		}

		res = append(res, hs...)
	}

	return res
}
