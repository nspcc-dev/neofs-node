package util

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// MergeSplitInfo ignores conflicts and rewrites `to` with non empty values
// from `from`.
func MergeSplitInfo(from, to *object.SplitInfo) *object.SplitInfo {
	to.SetSplitID(from.SplitID()) // overwrite SplitID and ignore conflicts

	if lp, ok := from.LastPart(); ok {
		to.SetLastPart(lp)
	}

	if link, ok := from.Link(); ok {
		to.SetLink(link)
	}

	if init, ok := from.FirstPart(); ok {
		to.SetFirstPart(init)
	}

	return to
}
