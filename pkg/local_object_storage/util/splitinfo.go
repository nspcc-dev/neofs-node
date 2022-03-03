package util

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// MergeSplitInfo ignores conflicts and rewrites `to` with non empty values
// from `from`.
func MergeSplitInfo(from, to *object.SplitInfo) *object.SplitInfo {
	to.SetSplitID(from.SplitID()) // overwrite SplitID and ignore conflicts

	if lp := from.LastPart(); lp != nil {
		to.SetLastPart(lp)
	}

	if link := from.Link(); link != nil {
		to.SetLink(link)
	}

	return to
}
