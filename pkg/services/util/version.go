package util

import (
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

var (
	serverVer         = version.Current().ProtoMessage()
	minRequestVersion = version.New(2, 22)
)

// NeedVersionInResponse returns true only if version is not found OR if it is
// higher that the server's one.
func NeedVersionInResponse(reqMetaHeader *protosession.RequestMetaHeader) bool {
	if reqMetaHeader == nil || reqMetaHeader.Version == nil {
		return true
	}
	v := reqMetaHeader.Version
	return v.Major > serverVer.Major || (v.Major == serverVer.Major && v.Minor > serverVer.Minor)
}

// VerifyRequestAPIVersion checks whether v is correct.
func VerifyRequestAPIVersion(v *protorefs.Version) error {
	if v == nil {
		return newBadRequestError("missing request API version")
	}

	ver := version.New(v.Major, v.Minor)
	if ver.Compare(minRequestVersion) < 0 {
		msg := fmt.Sprintf("request API version %s is outdated and no longer supported, minimum allowed is %s", ver, minRequestVersion)
		return newBadRequestError(msg)
	}

	return nil
}

func newBadRequestError(msg string) error {
	var res apistatus.BadRequest
	res.SetMessage(msg)
	return res
}
