package trustcontroller

import "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"

type storageWrapper struct {
	w common.Writer
	i Iterator
}

func (s storageWrapper) InitIterator(common.Context) (Iterator, error) {
	return s.i, nil
}

func (s storageWrapper) InitWriter(common.Context) (common.Writer, error) {
	return s.w, nil
}

// SimpleIteratorProvider returns IteratorProvider that provides
// static context-independent Iterator.
func SimpleIteratorProvider(i Iterator) IteratorProvider {
	return &storageWrapper{
		i: i,
	}
}

// SimpleWriterProvider returns WriterProvider that provides
// static context-independent Writer.
func SimpleWriterProvider(w common.Writer) common.WriterProvider {
	return &storageWrapper{
		w: w,
	}
}
