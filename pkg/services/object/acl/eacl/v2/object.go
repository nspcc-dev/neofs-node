package v2

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objectSDKID "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

func idValue(id *objectSDKID.ID) string {
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

func headersFromObject(obj *object.Object, addr *objectSDKAddress.Address) []eaclSDK.Header {
	var count int
	for obj := obj; obj != nil; obj = obj.Parent() {
		count += 9 + len(obj.Attributes())
	}

	res := make([]eaclSDK.Header, 0, count)
	for ; obj != nil; obj = obj.Parent() {
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
			// object type
			&sysObjHdr{
				k: acl.FilterObjectType,
				v: obj.Type().String(),
			},
		)

		cs, ok := obj.PayloadChecksum()
		if ok {
			res = append(res, &sysObjHdr{
				k: acl.FilterObjectPayloadHash,
				v: cs.String(),
			})
		}

		cs, ok = obj.PayloadHomomorphicHash()
		if ok {
			res = append(res, &sysObjHdr{
				k: acl.FilterObjectHomomorphicHash,
				v: cs.String(),
			})
		}

		attrs := obj.Attributes()
		for i := range attrs {
			res = append(res, &attrs[i]) // only pointer attrs can implement eaclSDK.Header interface
		}
	}

	return res
}
