package version

import (
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// IsValid checks if Version is not earlier than the genesis version of the NeoFS.
func IsValid(v version.Version) bool {
	const (
		startMajor = 2
		startMinor = 7
	)

	mjr := v.Major()

	return mjr > startMajor || mjr == startMajor && v.Minor() >= startMinor
}
