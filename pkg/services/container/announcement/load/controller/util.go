package loadcontroller

import (
	"context"

	"github.com/nspcc-dev/neofs-sdk-go/container"
)

func usedSpaceFilterEpochEQ(epoch uint64) UsedSpaceFilter {
	return func(a container.UsedSpaceAnnouncement) bool {
		return a.Epoch() == epoch
	}
}

type storageWrapper struct {
	w Writer
	i Iterator
}

func (s storageWrapper) InitIterator(context.Context) (Iterator, error) {
	return s.i, nil
}

func (s storageWrapper) InitWriter(context.Context) (Writer, error) {
	return s.w, nil
}

func SimpleIteratorProvider(i Iterator) IteratorProvider {
	return &storageWrapper{
		i: i,
	}
}

func SimpleWriterProvider(w Writer) WriterProvider {
	return &storageWrapper{
		w: w,
	}
}
