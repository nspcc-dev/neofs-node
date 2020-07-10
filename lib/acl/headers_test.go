package acl

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/stretchr/testify/require"
)

func TestNewTypedObjectExtendedHeader(t *testing.T) {
	var res acl.TypedHeader

	hdr := object.Header{}

	// nil value
	require.Nil(t, newTypedObjectExtendedHeader(hdr))

	// UserHeader
	{
		key := "key"
		val := "val"
		hdr.Value = &object.Header_UserHeader{
			UserHeader: &object.UserHeader{
				Key:   key,
				Value: val,
			},
		}

		res = newTypedObjectExtendedHeader(hdr)
		require.Equal(t, acl.HdrTypeObjUsr, res.HeaderType())
		require.Equal(t, key, res.Name())
		require.Equal(t, val, res.Value())
	}

	{ // Link
		link := new(object.Link)
		link.ID = object.ID{1, 2, 3}

		hdr.Value = &object.Header_Link{
			Link: link,
		}

		check := func(lt object.Link_Type, name string) {
			link.Type = lt

			res = newTypedObjectExtendedHeader(hdr)

			require.Equal(t, acl.HdrTypeObjSys, res.HeaderType())
			require.Equal(t, name, res.Name())
			require.Equal(t, link.ID.String(), res.Value())
		}

		check(object.Link_Previous, acl.HdrObjSysLinkPrev)
		check(object.Link_Next, acl.HdrObjSysLinkNext)
		check(object.Link_Parent, acl.HdrObjSysLinkPar)
		check(object.Link_Child, acl.HdrObjSysLinkChild)
		check(object.Link_StorageGroup, acl.HdrObjSysLinkSG)
	}
}
