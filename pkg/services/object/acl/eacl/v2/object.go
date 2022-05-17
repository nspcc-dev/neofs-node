package v2

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

type sysObjHdr struct {
	k, v string
}

func (s sysObjHdr) Key() string {
	return s.k
}

func (s sysObjHdr) Value() string {
	return s.v
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
		cnr, _ := addr.ContainerID()
		id, _ := addr.ObjectID()

		res = append(res,
			cidHeader(cnr),
			// creation epoch
			sysObjHdr{
				k: acl.FilterObjectCreationEpoch,
				v: u64Value(obj.CreationEpoch()),
			},
			// payload size
			sysObjHdr{
				k: acl.FilterObjectPayloadLength,
				v: u64Value(obj.PayloadSize()),
			},
			oidHeader(id),
			// object version
			sysObjHdr{
				k: acl.FilterObjectVersion,
				v: obj.Version().String(),
			},
			// object type
			sysObjHdr{
				k: acl.FilterObjectType,
				v: obj.Type().String(),
			},
		)

		if idOwner := obj.OwnerID(); idOwner != nil {
			res = append(res, ownerIDHeader(*idOwner))
		}

		cs, ok := obj.PayloadChecksum()
		if ok {
			res = append(res, sysObjHdr{
				k: acl.FilterObjectPayloadHash,
				v: cs.String(),
			})
		}

		cs, ok = obj.PayloadHomomorphicHash()
		if ok {
			res = append(res, sysObjHdr{
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
