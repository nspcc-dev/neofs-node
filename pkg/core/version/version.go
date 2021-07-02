package version

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
)

// IsValid checks if Version is not earlier than the genesis version of the NeoFS.
func IsValid(v pkg.Version) bool {
	const (
		startMajor = 2
		startMinor = 7
	)

	mjr := v.Major()

	return mjr > startMajor || mjr == startMajor && v.Minor() >= startMinor
}
