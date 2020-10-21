package v2

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

type sysObjHdr struct {
	k, v string
}

func (s *sysObjHdr) GetKey() string {
	return s.k
}

func (s *sysObjHdr) GetValue() string {
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

func headersFromObject(obj *object.Object) []eacl.Header {
	// TODO: optimize allocs
	res := make([]eacl.Header, 0)

	for ; obj != nil; obj = obj.GetParent() {
		res = append(res,
			// object ID
			&sysObjHdr{
				k: objectSDK.HdrSysNameID,
				v: idValue(obj.GetID()),
			},
			// container ID
			&sysObjHdr{
				k: objectSDK.HdrSysNameCID,
				v: cidValue(obj.GetContainerID()),
			},
			// owner ID
			&sysObjHdr{
				k: objectSDK.HdrSysNameOwnerID,
				v: ownerIDValue(obj.GetOwnerID()),
			},
			// creation epoch
			&sysObjHdr{
				k: objectSDK.HdrSysNameCreatedEpoch,
				v: u64Value(obj.GetCreationEpoch()),
			},
			// payload size
			&sysObjHdr{
				k: objectSDK.HdrSysNamePayloadLength,
				v: u64Value(obj.GetPayloadSize()),
			},
		)

		attrs := obj.GetAttributes()
		hs := make([]eacl.Header, 0, len(attrs))

		for i := range attrs {
			hs = append(hs, attrs[i])
		}

		res = append(res, hs...)
	}

	return res
}
