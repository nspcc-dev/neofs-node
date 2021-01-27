package loadcontroller

import "github.com/nspcc-dev/neofs-api-go/pkg/container"

func usedSpaceFilterEpochEQ(epoch uint64) UsedSpaceFilter {
	return func(a container.UsedSpaceAnnouncement) bool {
		return a.Epoch() == epoch
	}
}
