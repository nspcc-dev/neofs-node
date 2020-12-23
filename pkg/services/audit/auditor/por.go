package auditor

import (
	"bytes"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

func (c *Context) executePoR() {
	for i, sg := range c.task.StorageGroupList() {
		c.checkStorageGroupPoR(i, sg) // consider parallel it
	}
}

func (c *Context) checkStorageGroupPoR(ind int, sg *object.ID) {
	storageGroup, err := c.cnrCom.GetSG(c.task, sg) // get storage group
	if err != nil {
		c.log.Warn("can't get storage group",
			zap.Stringer("sgid", sg),
			zap.String("error", err.Error()))

		return
	}

	members := storageGroup.Members()
	c.sgMembersCache[ind] = members

	var (
		tzHash    []byte
		totalSize uint64
	)

	for i := range members {
		objectPlacement, err := c.buildPlacement(members[i])
		if err != nil {
			c.log.Info("can't build placement for storage group member",
				zap.Stringer("sg", sg),
				zap.Stringer("member_id", members[i]),
			)

			continue
		}

		for _, node := range placement.FlattenNodes(objectPlacement) {
			hdr, err := c.cnrCom.GetHeader(c.task, node, members[i], true)
			if err != nil {
				c.log.Debug("can't head object",
					zap.String("remote_node", node.Address()),
					zap.Stringer("oid", members[i]))

				continue
			}

			// update cache for PoR and PDP audit checks
			c.updateHeadResponses(hdr)

			if len(tzHash) == 0 {
				tzHash = hdr.PayloadHomomorphicHash().Sum()
			} else {
				tzHash, err = tz.Concat([][]byte{
					tzHash,
					hdr.PayloadHomomorphicHash().Sum(),
				})
				if err != nil {
					c.log.Debug("can't concatenate tz hash",
						zap.Stringer("oid", members[i]),
						zap.String("error", err.Error()))

					break
				}
			}

			totalSize += hdr.PayloadSize()

			break
		}
	}

	sizeCheck := storageGroup.ValidationDataSize() == totalSize
	tzCheck := bytes.Equal(tzHash, storageGroup.ValidationDataHash().Sum())

	if sizeCheck && tzCheck {
		c.report.PassedPoR(sg) // write report
	} else {
		if !sizeCheck {
			c.log.Debug("storage group size check failed",
				zap.Uint64("expected", storageGroup.ValidationDataSize()),
				zap.Uint64("got", totalSize))
		}

		if !tzCheck {
			c.log.Debug("storage group tz hash check failed")
		}

		c.report.FailedPoR(sg) // write report
	}
}
