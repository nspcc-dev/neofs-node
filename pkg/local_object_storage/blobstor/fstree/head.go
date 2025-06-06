package fstree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const (
	fieldObjectPayload = 4
	maxHeaderSize      = 16 * 1024 // 16 KiB
)

// Head returns an object's header from the storage by address without reading the full payload.
// It reads only the first maxHeaderSize bytes, which should contain the header information.
func (t *FSTree) Head(addr oid.Address) (*objectSDK.Object, error) {
	p := t.treePath(addr)

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return nil, fmt.Errorf("read file %q: %w", p, err)
	}
	defer f.Close()

	data, err := extractHeaderOnly(addr.Object(), f)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return nil, fmt.Errorf("extract object header from %q: %w", p, err)
	}

	data, err = t.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompress header data %q: %w", p, err)
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("decode object header: %w", err)
	}

	return obj, nil
}

// extractHeaderOnly reads the header of an object from a file.
func extractHeaderOnly(id oid.ID, f *os.File) ([]byte, error) {
	var (
		comBuf     [combinedDataOff]byte
		isCombined bool
	)

	for {
		n, err := io.ReadFull(f, comBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if !isCombined {
					return comBuf[:n], nil
				}
				return nil, fs.ErrNotExist
			}
			return nil, err
		}
		thisOID, l := parseCombinedPrefix(comBuf)
		if thisOID == nil {
			if isCombined {
				return nil, errors.New("malformed combined file")
			}
			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("seek to start of file: %w", err)
			}

			return readUntilPayload(f)
		}
		isCombined = true
		if bytes.Equal(thisOID, id[:]) {
			return readUntilPayload(f)
		}
		_, err = f.Seek(int64(l), 1)
		if err != nil {
			return nil, err
		}
	}
}

func readUntilPayload(f *os.File) ([]byte, error) {
	data := make([]byte, maxHeaderSize)

	n, err := f.Read(data)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read file: %w", err)
	}
	data = data[:n]

	var out []byte
	offset := 0

	for offset < len(data) {
		tag, tagLen := binary.Uvarint(data[offset:])
		if tagLen <= 0 {
			return nil, fmt.Errorf("invalid varint tag at offset %d", offset)
		}
		fieldNum := int(tag >> 3)
		wireType := int(tag & 0x7)

		// wireType != protowire.BytesType
		if wireType != 2 {
			return nil, fmt.Errorf("unsupported wire type: %d", wireType)
		}

		if fieldNum == fieldObjectPayload {
			return out, nil
		}

		offset += tagLen

		fieldLen, lenLen := binary.Uvarint(data[offset:])
		if lenLen <= 0 {
			return nil, fmt.Errorf("invalid varint length at offset %d", offset)
		}
		offset += lenLen

		fieldEnd := offset + int(fieldLen)
		if fieldEnd > len(data) {
			return nil, fmt.Errorf("field exceeds data boundary")
		}
		out = append(out, data[offset-tagLen-lenLen:fieldEnd]...)
		offset = fieldEnd
	}

	return out, nil
}
