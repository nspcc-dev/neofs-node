package util

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// MergeSplitInfo ignores conflicts and rewrites `to` with non empty values
// from `from`.
func MergeSplitInfo(from, to *object.SplitInfo) *object.SplitInfo {
	to.SetSplitID(from.SplitID()) // overwrite SplitID and ignore conflicts

	if lp := from.GetLastPart(); !lp.IsZero() {
		to.SetLastPart(lp)
	}

	if link := from.GetLink(); !link.IsZero() {
		to.SetLink(link)
	}

	if init := from.GetFirstPart(); !init.IsZero() {
		to.SetFirstPart(init)
	}

	return to
}
