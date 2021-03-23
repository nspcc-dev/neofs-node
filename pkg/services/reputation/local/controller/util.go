package trustcontroller

type storageWrapper struct {
	w Writer
	i Iterator
}

func (s storageWrapper) InitIterator(Context) (Iterator, error) {
	return s.i, nil
}

func (s storageWrapper) InitWriter(Context) (Writer, error) {
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
func SimpleWriterProvider(w Writer) WriterProvider {
	return &storageWrapper{
		w: w,
	}
}
