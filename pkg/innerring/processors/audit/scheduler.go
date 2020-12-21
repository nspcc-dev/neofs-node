package audit

import (
	"sort"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/pkg/errors"
)

var ErrInvalidIRNode = errors.New("node is not in the inner ring list")

func (ap *Processor) selectContainersToAudit(epoch uint64) ([]*container.ID, error) {
	containers, err := invoke.ListContainers(ap.morphClient, ap.containerContract)
	if err != nil {
		return nil, errors.Wrap(err, "can't get list of containers to start audit")
	}

	// consider getting extra information about container complexity from
	// audit contract there

	sort.Slice(containers, func(i, j int) bool {
		return strings.Compare(containers[i].String(), containers[j].String()) < 0
	})

	ind := ap.irList.Index()
	irSize := ap.irList.InnerRingSize()

	if ind < 0 || ind >= irSize {
		return nil, ErrInvalidIRNode
	}

	return Select(containers, epoch, uint64(ind), uint64(irSize)), nil
}

func Select(ids []*container.ID, epoch, index, size uint64) []*container.ID {
	if index >= size {
		return nil
	}

	var a, b uint64

	ln := uint64(len(ids))
	pivot := ln % size
	delta := ln / size

	index = (index + epoch) % size
	if index < pivot {
		a = delta + 1
	} else {
		a = delta
		b = pivot
	}

	from := a*index + b
	to := a*(index+1) + b

	return ids[from:to]
}
