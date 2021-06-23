package auditor

import (
	"bytes"
	"encoding/hex"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

func (c *Context) executePDP() {
	c.processPairs()
	c.writePairsResult()
}

func (c *Context) processPairs() {
	wg := new(sync.WaitGroup)

	for i := range c.pairs {
		p := &c.pairs[i]
		wg.Add(1)

		if err := c.pdpWorkerPool.Submit(func() {
			c.processPair(p)
			wg.Done()
		}); err != nil {
			wg.Done()
		}
	}

	wg.Wait()
}

func (c *Context) processPair(p *gamePair) {
	c.distributeRanges(p)
	c.collectHashes(p)
	c.analyzeHashes(p)
}

func (c *Context) distributeRanges(p *gamePair) {
	p.rn1 = make([]*object.Range, hashRangeNumber-1)
	p.rn2 = make([]*object.Range, hashRangeNumber-1)

	for i := 0; i < hashRangeNumber-1; i++ {
		p.rn1[i] = object.NewRange()
		p.rn2[i] = object.NewRange()
	}

	notches := c.splitPayload(p.id)

	{ // node 1
		// [0:n2]
		p.rn1[0].SetLength(notches[1])

		// [n2:n3]
		p.rn1[1].SetOffset(notches[1])
		p.rn1[1].SetLength(notches[2] - notches[1])

		// [n3:full]
		p.rn1[2].SetOffset(notches[2])
		p.rn1[2].SetLength(notches[3] - notches[2])
	}

	{ // node 2
		// [0:n1]
		p.rn2[0].SetLength(notches[0])

		// [n1:n2]
		p.rn2[1].SetOffset(notches[0])
		p.rn2[1].SetLength(notches[1] - notches[0])

		// [n2:full]
		p.rn2[2].SetOffset(notches[1])
		p.rn2[2].SetLength(notches[3] - notches[1])
	}
}

func (c *Context) splitPayload(id *object.ID) []uint64 {
	var (
		prev    uint64
		size    = c.objectSize(id)
		notches = make([]uint64, 0, hashRangeNumber)
	)

	for i := uint64(0); i < hashRangeNumber; i++ {
		var nextLn uint64
		if i < hashRangeNumber-1 {
			nextLn = randUint64(size-prev-(hashRangeNumber-i)) + 1
		} else {
			nextLn = size - prev
		}

		notches = append(notches, prev+nextLn)

		prev += nextLn
	}

	return notches
}

func (c *Context) collectHashes(p *gamePair) {
	fn := func(n *netmap.Node, rngs []*object.Range, hashWriter func([]byte)) {
		// TODO: add order randomization
		for i := range rngs {
			sleepDur := time.Duration(randUint64(c.maxPDPSleep))

			c.log.Debug("sleep before get range hash",
				zap.Stringer("interval", sleepDur),
			)

			time.Sleep(time.Duration(sleepDur))

			h, err := c.cnrCom.GetRangeHash(c.task, n, p.id, rngs[i])
			if err != nil {
				c.log.Debug("could not get payload range hash",
					zap.Stringer("id", p.id),
					zap.String("node", hex.EncodeToString(n.PublicKey())),
					zap.String("error", err.Error()),
				)
				return
			}

			hashWriter(h)
		}
	}

	p.hh1 = make([][]byte, 0, len(p.rn1))

	fn(p.n1, p.rn1, func(h []byte) {
		p.hh1 = append(p.hh1, h)
	})

	p.hh2 = make([][]byte, 0, len(p.rn2))

	fn(p.n2, p.rn2, func(h []byte) {
		p.hh2 = append(p.hh2, h)
	})
}

func (c *Context) analyzeHashes(p *gamePair) {
	if len(p.hh1) != hashRangeNumber-1 || len(p.hh2) != hashRangeNumber-1 {
		c.failNodesPDP(p.n1, p.n2)
		return
	}

	h1, err := tz.Concat([][]byte{p.hh2[0], p.hh2[1]})
	if err != nil || !bytes.Equal(p.hh1[0], h1) {
		c.failNodesPDP(p.n1, p.n2)
		return
	}

	h2, err := tz.Concat([][]byte{p.hh1[1], p.hh1[2]})
	if err != nil || !bytes.Equal(p.hh2[2], h2) {
		c.failNodesPDP(p.n1, p.n2)
		return
	}

	fh, err := tz.Concat([][]byte{h1, h2})
	if err != nil || !bytes.Equal(fh, c.objectHomoHash(p.id)) {
		c.failNodesPDP(p.n1, p.n2)
		return
	}

	c.passNodesPDP(p.n1, p.n2)
}

func (c *Context) failNodesPDP(ns ...*netmap.Node) {
	c.pairedMtx.Lock()

	for i := range ns {
		c.pairedNodes[ns[i].Hash()].failedPDP = true
	}

	c.pairedMtx.Unlock()
}

func (c *Context) passNodesPDP(ns ...*netmap.Node) {
	c.pairedMtx.Lock()

	for i := range ns {
		c.pairedNodes[ns[i].Hash()].passedPDP = true
	}

	c.pairedMtx.Unlock()
}

func (c *Context) writePairsResult() {
	var failCount, okCount int

	c.iteratePairedNodes(
		func(*netmap.Node) { failCount++ },
		func(*netmap.Node) { okCount++ },
	)

	failedNodes := make([][]byte, 0, failCount)
	passedNodes := make([][]byte, 0, okCount)

	c.iteratePairedNodes(
		func(n *netmap.Node) {
			failedNodes = append(failedNodes, n.PublicKey())
		},
		func(n *netmap.Node) {
			passedNodes = append(passedNodes, n.PublicKey())
		},
	)

	c.report.SetPDPResults(passedNodes, failedNodes)
}

func (c *Context) iteratePairedNodes(onFail, onPass func(*netmap.Node)) {
	for _, pairedNode := range c.pairedNodes {
		if pairedNode.failedPDP {
			onFail(pairedNode.node)
		}

		if pairedNode.passedPDP {
			onPass(pairedNode.node)
		}
	}
}
