package util

import (
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

var serverVer = version.Current().ProtoMessage()

// NeedVersionInResponse returns true only if version is not found OR if it is
// higher that the server's one.
func NeedVersionInResponse(reqMetaHeader *protosession.RequestMetaHeader) bool {
	if reqMetaHeader == nil || reqMetaHeader.Version == nil {
		return true
	}
	v := reqMetaHeader.Version
	return v.Major > serverVer.Major || (v.Major == serverVer.Major && v.Minor > serverVer.Minor)
}
