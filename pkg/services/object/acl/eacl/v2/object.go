package v2

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
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

func cidValue(id *cid.ID) string {
	return id.String()
}

func ownerIDValue(id *owner.ID) string {
	return id.String()
}

func u64Value(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func headersFromObject(obj *object.Object, addr *objectSDK.Address) []eaclSDK.Header {
	var count int
	for obj := obj; obj != nil; obj = obj.GetParent() {
		count += 9 + len(obj.Attributes())
	}

	res := make([]eaclSDK.Header, 0, count)
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
			// object version
			&sysObjHdr{
				k: acl.FilterObjectVersion,
				v: obj.Version().String(),
			},
			// payload hash
			&sysObjHdr{
				k: acl.FilterObjectPayloadHash,
				v: obj.PayloadChecksum().String(),
			},
			// object type
			&sysObjHdr{
				k: acl.FilterObjectType,
				v: obj.Type().String(),
			},
			// payload homomorphic hash
			&sysObjHdr{
				k: acl.FilterObjectHomomorphicHash,
				v: obj.PayloadHomomorphicHash().String(),
			},
		)

		attrs := obj.Attributes()
		for i := range attrs {
			res = append(res, attrs[i])
		}
	}

	return res
}
