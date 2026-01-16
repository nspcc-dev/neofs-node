package engine

import (
	"errors"
	"io"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GetRange reads a part of an object from local storage. Zero length is
// interpreted as requiring full object length independent of the offset.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object is inhumed.
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// If referenced object is a parent of some stored EC parts, GetRange returns
// [ierrors.ErrParentObject] wrapping [iec.ErrParts].
func (e *StorageEngine) GetRange(addr oid.Address, offset uint64, length uint64) ([]byte, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddRangeDuration)()
	}

	stream, err := e.getRangeStream(addr, offset, length)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	if offset == 0 && length == 0 {
		return io.ReadAll(stream)
	}

	data := make([]byte, length)

	if _, err = io.ReadFull(stream, data); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	return data, nil
}
