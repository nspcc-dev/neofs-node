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

// IsValid checks if Version is not earlier than the genesis version of the NeoFS.
func IsValid(v version.Version) bool {
	const (
		startMajor = 2
		startMinor = 7
	)

	mjr := v.Major()

	return mjr > startMajor || mjr == startMajor && v.Minor() >= startMinor
}
