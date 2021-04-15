package util

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
)

// MergeSplitInfo ignores conflicts and rewrites `to` with non empty values
// from `from`.
func MergeSplitInfo(from, to *objectSDK.SplitInfo) *objectSDK.SplitInfo {
	to.SetSplitID(from.SplitID()) // overwrite SplitID and ignore conflicts

	if lp := from.LastPart(); lp != nil {
		to.SetLastPart(lp)
	}

	if link := from.Link(); link != nil {
		to.SetLink(link)
	}

	return to
}
