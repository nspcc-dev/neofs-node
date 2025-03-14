package v2

import (
	"strconv"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
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

func headersFromObject(obj *object.Object, cnr cid.ID, oid *oid.ID) []eaclSDK.Header {
	var count int
	for obj := obj; obj != nil; obj = obj.Parent() {
		count += 9 + len(obj.Attributes())
	}

	res := make([]eaclSDK.Header, 0, count)
	for ; obj != nil; obj = obj.Parent() {
		var ver = obj.Version()
		if ver == nil {
			ver = &version.Version{}
		}
		res = append(res,
			cidHeader(cnr),
			// creation epoch
			sysObjHdr{
				k: eaclSDK.FilterObjectCreationEpoch,
				v: u64Value(obj.CreationEpoch()),
			},
			// payload size
			sysObjHdr{
				k: eaclSDK.FilterObjectPayloadSize,
				v: u64Value(obj.PayloadSize()),
			},
			// object version
			sysObjHdr{
				k: eaclSDK.FilterObjectVersion,
				v: ver.String(),
			},
			// object type
			sysObjHdr{
				k: eaclSDK.FilterObjectType,
				v: obj.Type().String(),
			},
		)

		if oid != nil {
			res = append(res, oidHeader(*oid))
		}

		if idOwner := obj.Owner(); !idOwner.IsZero() {
			res = append(res, ownerIDHeader(idOwner))
		}

		cs, ok := obj.PayloadChecksum()
		if ok {
			res = append(res, sysObjHdr{
				k: eaclSDK.FilterObjectPayloadChecksum,
				v: cs.String(),
			})
		}

		cs, ok = obj.PayloadHomomorphicHash()
		if ok {
			res = append(res, sysObjHdr{
				k: eaclSDK.FilterObjectPayloadHomomorphicChecksum,
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
