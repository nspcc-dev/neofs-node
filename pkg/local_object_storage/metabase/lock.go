package meta

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// suffix for container buckets with locked objects.
const bucketNameSuffixLocked = invalidBase58String + "LOCKED"

// returns name of the bucket with locked objects for specified container.
func bucketNameLocked(idCnr cid.ID) []byte {
	return []byte(idCnr.String() + bucketNameSuffixLocked)
}
