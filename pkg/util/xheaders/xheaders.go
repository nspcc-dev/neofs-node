package xheaders

import (
	"fmt"
	"strconv"

	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/proto/session"
)

// CheckRequestContainerRevision checks if requested container revision matches
// the actual container's revision. In a negative scenario, returns an error
// that satisfies comparison with [apistatus.ErrContainerRevisionMismatch].
func CheckRequestContainerRevision(mh *session.RequestMetaHeader, cID cid.ID, cnrSrc containercore.Source) error {
	var ind = -1
	for i, xh := range mh.GetXHeaders() {
		if xh.Key == client.XHeaderContainerRevision {
			ind = i
			break
		}
	}
	if ind == -1 {
		return nil
	}

	var val = mh.XHeaders[ind].Value
	revClient, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		var errBadReq apistatus.BadRequest
		errBadReq.SetMessage(fmt.Sprintf("parsing container revision from '%s' X-Header: %s", val, err.Error()))
		return errBadReq
	}
	cnr, err := cnrSrc.Get(cID)
	if err != nil {
		return fmt.Errorf("fetching container: %w", err)
	}
	if revServer := cnr.Revision(); revClient != revServer {
		return apistatus.NewContainerRevisionMismatch(fmt.Sprintf(
			"container revision does not match: requested: %d, server's: %d", revClient, revServer))
	}

	return nil
}
