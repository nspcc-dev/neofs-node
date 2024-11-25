package auditor

import (
	"bytes"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	storagegroupSDK "github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

func (c *Context) executePoR() {
	wg := new(sync.WaitGroup)
	sgs := c.task.StorageGroupList()

	for _, sg := range sgs {
		wg.Add(1)

		if err := c.porWorkerPool.Submit(func() {
			c.checkStorageGroupPoR(sg.ID(), sg.StorageGroup())
			wg.Done()
		}); err != nil {
			wg.Done()
		}
	}

	wg.Wait()
	c.porWorkerPool.Release()

	c.report.SetPoRCounters(c.porRequests.Load(), c.porRetries.Load())
}

func (c *Context) checkStorageGroupPoR(sgID oid.ID, sg storagegroupSDK.StorageGroup) {
	members := sg.Members()
	c.updateSGInfo(sgID, members)

	var (
		tzHash    []byte
		totalSize uint64

		accRequests, accRetries uint32
	)

	var getHeaderPrm GetHeaderPrm
	getHeaderPrm.Context = c.task.AuditContext()
	getHeaderPrm.CID = c.task.ContainerID()
	getHeaderPrm.NodeIsRelay = true

	homomorphicHashingEnabled := !c.task.ContainerStructure().IsHomomorphicHashingDisabled()

	for i := range members {
		objectPlacement, err := c.buildPlacement(members[i])
		if err != nil {
			c.log.Info("can't build placement for storage group member",
				zap.Stringer("sg", sgID),
				zap.String("member_id", members[i].String()),
			)

			continue
		}

		flat := placement.FlattenNodes(objectPlacement)

		rand.Shuffle(len(flat), func(i, j int) {
			flat[i], flat[j] = flat[j], flat[i]
		})

		getHeaderPrm.OID = members[i]

		for j := range flat {
			accRequests++
			if j > 0 { // in best case audit get object header on first iteration
				accRetries++
			}

			getHeaderPrm.Node = flat[j]

			hdr, err := c.cnrCom.GetHeader(getHeaderPrm)
			if err != nil {
				c.log.Debug("can't head object",
					zap.String("remote_node", netmap.StringifyPublicKey(flat[j])),
					zap.Stringer("oid", members[i]),
				)

				continue
			}

			// update cache for PoR and PDP audit checks
			c.updateHeadResponses(hdr)

			if homomorphicHashingEnabled {
				cs, _ := hdr.PayloadHomomorphicHash()
				if len(tzHash) == 0 {
					tzHash = cs.Value()
				} else {
					tzHash, err = tz.Concat([][]byte{
						tzHash,
						cs.Value(),
					})
					if err != nil {
						c.log.Debug("can't concatenate tz hash",
							zap.String("oid", members[i].String()),
							zap.Error(err))

						break
					}
				}
			}

			totalSize += hdr.PayloadSize()

			break
		}
	}

	c.porRequests.Add(accRequests)
	c.porRetries.Add(accRetries)

	sizeCheck := sg.ValidationDataSize() == totalSize
	cs, _ := sg.ValidationDataHash()
	tzCheck := !homomorphicHashingEnabled || bytes.Equal(tzHash, cs.Value())

	if sizeCheck && tzCheck {
		c.report.PassedPoR(sgID) // write report
	} else {
		if !sizeCheck {
			c.log.Debug("storage group size check failed",
				zap.Uint64("expected", sg.ValidationDataSize()),
				zap.Uint64("got", totalSize))
		}

		if !tzCheck {
			c.log.Debug("storage group tz hash check failed")
		}

		c.report.FailedPoR(sgID) // write report
	}
}
