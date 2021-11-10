package auditor

import (
	"bytes"
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

func (c *Context) executePoR() {
	wg := new(sync.WaitGroup)
	sgs := c.task.StorageGroupList()

	for i := range sgs {
		wg.Add(1)

		sg := sgs[i]

		if err := c.porWorkerPool.Submit(func() {
			c.checkStorageGroupPoR(i, sg)
			wg.Done()
		}); err != nil {
			wg.Done()
		}
	}

	wg.Wait()
	c.porWorkerPool.Release()

	c.report.SetPoRCounters(c.porRequests.Load(), c.porRetries.Load())
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
	c.updateSGInfo(ind, members)

	var (
		tzHash    []byte
		totalSize uint64

		accRequests, accRetries uint32
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

		flat := placement.FlattenNodes(objectPlacement)

		crand := rand.New() // math/rand with cryptographic source
		crand.Shuffle(len(flat), func(i, j int) {
			flat[i], flat[j] = flat[j], flat[i]
		})

		for j := range flat {
			accRequests++
			if j > 0 { // in best case audit get object header on first iteration
				accRetries++
			}

			hdr, err := c.cnrCom.GetHeader(c.task, flat[j], members[i], true)
			if err != nil {
				c.log.Debug("can't head object",
					zap.String("remote_node", hex.EncodeToString(flat[j].PublicKey())),
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

	c.porRequests.Add(accRequests)
	c.porRetries.Add(accRetries)

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
