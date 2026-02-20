package version

import (
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// SysObjTargetShouldBeInHeader returns true if TS/LOCK system objects should store
// its targets in header, not in payload.
func SysObjTargetShouldBeInHeader(v *version.Version) bool {
	if v == nil || !IsValid(*v) {
		return false
	}

	// This is the latest object (and API) version that allows to store LOCK/TS
	// target_s_ in object's payload as a separate protobuf message. After this
	// version, it is only allowed to have 1-to-1 relationship b/w system object
	// and its target. Such target should be stored in object's attributes.
	const latestSysObjTargetInPayloadMjr = 2
	const latestSysObjTargetInPayloadMnr = 17

	return v.Major() > latestSysObjTargetInPayloadMjr ||
		(v.Major() == latestSysObjTargetInPayloadMjr && v.Minor() > latestSysObjTargetInPayloadMnr)
}

// OwnerSignatureMatchRequired returns true if an object with the given version
// must have the owner matching the signature's public key. Objects below version
// 2.18 may have a mismatching owner due to a bug that allowed creating such
// objects, so they should not be rejected.
func OwnerSignatureMatchRequired(v *version.Version) bool {
	if v == nil || !IsValid(*v) {
		return true // assume latest
	}

	const (
		ownerMatchMjr = 2
		ownerMatchMnr = 18
	)

	return v.Major() > ownerMatchMjr ||
		(v.Major() == ownerMatchMjr && v.Minor() >= ownerMatchMnr)
}

// IsValid checks if Version is not earlier than the genesis version of the NeoFS.
func IsValid(v version.Version) bool {
	const (
		startMajor = 2
		startMinor = 7
	)

	mjr := v.Major()

	return mjr > startMajor || mjr == startMajor && v.Minor() >= startMinor
}
