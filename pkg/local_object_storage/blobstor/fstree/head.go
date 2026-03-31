package fstree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/klauspost/compress/zstd"
	objectwire "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Head returns an object's header from the storage by address without reading the full payload.
func (t *FSTree) Head(addr oid.Address) (*object.Object, error) {
	obj, reader, err := t.getObjectStream(addr)
	if err != nil {
		return nil, err
	}
	_ = reader.Close()

	return obj, nil
}

// ReadHeader reads first bytes of the referenced object's binary containing its
// full header from t into buf. Returns number of bytes read.
//
// Read part may include payload prefix.
//
// If object is missing, ReadHeader returns [apistatus.ErrObjectNotFound].
//
// Passed buf must have 2*[objectwire.NonPayloadFieldsBufferLength] bytes len at least.
func (t *FSTree) ReadHeader(addr oid.Address, buf []byte) (int, error) {
	n, stream, err := t.ReadObject(addr, buf)
	if err != nil {
		return 0, err
	}

	stream.Close()

	return n, nil
}

func (t *FSTree) _readObject(addr oid.Address, buf []byte) ([]byte, io.ReadCloser, error) {
	if len(buf) < 2*objectwire.NonPayloadFieldsBufferLength {
		return nil, nil, fmt.Errorf("too short buffer %d bytes", len(buf))
	}

	p := t.treePath(addr)

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, nil, fmt.Errorf("read file %q: %w", p, err)
	}

	initial, stream, err := t.readHeader(addr.Object(), f, buf)
	if err != nil {
		stream.Close()
		return nil, nil, err
	}

	return t.preprocessStreamHead(stream, initial)
}

// ReadObject reads first bytes of the referenced object's binary containing its
// full header from t into buf. Returns number of bytes read and stream of
// remaining bytes. The stream must be finally closed by the caller.
//
// Read part may include payload prefix.
//
// If object is missing, ReadObject returns [apistatus.ErrObjectNotFound].
//
// Passed buf must have 2*[objectwire.NonPayloadFieldsBufferLength] bytes len at least.
func (t *FSTree) ReadObject(addr oid.Address, buf []byte) (int, io.ReadCloser, error) {
	initial, stream, err := t._readObject(addr, buf)
	if err != nil {
		return 0, nil, err
	}

	if stream == nil {
		stream = nopReadCloser{}
	}

	return copy(buf, initial), stream, nil
}

// ReadPayloadRange is [FSTree.ReadObject] analogue for payload range reading.
// Zero range means full payload.
//
// If given range is out of payload bounds, ReadPayloadRange returns
// [apistatus.ErrObjectOutOfRange].
func (t *FSTree) ReadPayloadRange(addr oid.Address, off, ln uint64, hdrBuf []byte) (io.ReadCloser, error) {
	if err := verifyRequestedRange(off, ln); err != nil {
		return nil, err
	}

	initial, stream, err := t._readObject(addr, hdrBuf)
	if err != nil {
		return nil, err
	}

	// stream can be nil
	pldLen, pldFldOff, err := objectwire.GetPayloadLengthAndFieldOffset(initial)
	if err != nil {
		if stream != nil {
			stream.Close()
		}
		return nil, fmt.Errorf("get payload length and field in read header: %w", err)
	}

	if ln != 0 && !checkPayloadBounds(pldLen, off, ln) {
		if stream != nil {
			stream.Close()
		}
		return nil, apistatus.ErrObjectOutOfRange
	}

	// FIXME: cover pldFldOff < 0

	_, n, err := iprotobuf.ParseVarint(initial[pldFldOff:])
	if err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			if stream != nil {
				stream.Close()
			}
			return nil, fmt.Errorf("parse payload field len: %w", err)
		}

		if pldFldOff+binary.MaxVarintLen64 > len(initial) {
			if pldFldOff >= binary.MaxVarintLen64 {
				n = copy(initial[pldFldOff-binary.MaxVarintLen64:], initial[pldFldOff:])
				pldFldOff -= binary.MaxVarintLen64
			} else {
				initial = make([]byte, binary.MaxVarintLen64)
				n = copy(initial, initial[pldFldOff:])
				pldFldOff = 0
			}
		}

		extra, err := io.ReadFull(stream, initial[pldFldOff+n:])
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			if stream != nil {
				stream.Close()
			}
			return nil, fmt.Errorf("read stream: %w", err)
		}

		initial = initial[:pldFldOff+n+extra]

		_, n, err = iprotobuf.ParseVarint(initial[pldFldOff:])
		if err != nil {
			if stream != nil {
				stream.Close()
			}
			return nil, fmt.Errorf("parse payload field len: %w", err)
		}
	}

	initial = initial[pldFldOff+n:]

	if stream == nil && uint64(len(initial)) != pldLen {
		return nil, fmt.Errorf("diff len of object payload: in header %d, in field tag %d", pldLen, len(initial))
	}

	type readerCloser struct {
		io.Reader
		io.Closer
	}

	// check range is already bufferred

	if off == 0 {
		if ln == 0 { // full
			if stream == nil {
				return io.NopCloser(bytes.NewReader(initial)), nil
			}
			if len(initial) == 0 {
				return stream, nil
			}
			return readerCloser{Reader: io.MultiReader(bytes.NewReader(initial)), Closer: stream}, nil
		}

		if ln <= uint64(len(initial)) {
			if stream != nil {
				stream.Close()
			}
			return io.NopCloser(bytes.NewReader(initial[:ln])), nil
		}

		// stream is non-nil here according to conditions above

		if len(initial) == 0 {
			return stream, nil
		}

		if err := checkTooBigRange(off, ln); err != nil {
			stream.Close()
			return nil, err
		}

		return readerCloser{Reader: io.LimitReader(io.MultiReader(bytes.NewReader(initial), stream), int64(ln)), Closer: stream}, nil
	}

	if stream == nil {
		// range is within slice according to conditions above
		return io.NopCloser(bytes.NewReader(initial[off:][:ln])), nil
	}

	if err := checkTooBigRange(off, ln); err != nil {
		stream.Close()
		return nil, err
	}

	if off >= uint64(len(initial)) {
		if off > uint64(len(initial)) {
			if seeker, ok := stream.(io.Seeker); ok {
				_, err = seeker.Seek(int64(off)-int64(len(initial)), io.SeekCurrent)
			} else {
				_, err = io.CopyN(io.Discard, stream, int64(off)-int64(len(initial)))
			}
			if err != nil {
				stream.Close()
				return nil, fmt.Errorf("seek payload stream: %w", err)
			}
		}
		return readerCloser{Reader: io.LimitReader(stream, int64(ln)), Closer: stream}, nil
	}

	return readerCloser{Reader: io.LimitReader(io.MultiReader(bytes.NewReader(initial[off:]), stream), int64(ln)), Closer: stream}, nil
}

// getObjectStream reads an object from the storage by address as a stream.
// It returns the object with header only, and a reader for the payload.
func (t *FSTree) getObjectStream(addr oid.Address) (*object.Object, io.ReadSeekCloser, error) {
	p := t.treePath(addr)

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, nil, fmt.Errorf("read file %q: %w", p, err)
	}

	obj, reader, err := t.extractHeaderAndStream(addr.Object(), f)
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, nil, fmt.Errorf("extract object stream from %q: %w", p, err)
	}

	return obj, reader, nil
}

// extractHeaderAndStream reads the header of an object from a file.
// The caller is responsible for closing the returned io.ReadCloser if it is not nil.
func (t *FSTree) extractHeaderAndStream(id oid.ID, f *os.File) (*object.Object, io.ReadSeekCloser, error) {
	buf := make([]byte, 2*objectwire.NonPayloadFieldsBufferLength)

	initial, stream, err := t.readHeader(id, f, buf)
	if err != nil {
		stream.Close()
		return nil, nil, err
	}

	return t.readHeaderAndPayload(stream, initial)
}

func (t *FSTree) readHeader(id oid.ID, f *os.File, buf []byte) ([]byte, io.ReadCloser, error) {
	n, err := io.ReadFull(f, buf[:objectwire.NonPayloadFieldsBufferLength])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, f, err
	}
	if n < combinedDataOff {
		return buf[:n], f, nil
	}

	thisOID, l := parseCombinedPrefix(buf)
	if thisOID == nil {
		return buf[:n], f, nil
	}

	offset := combinedDataOff
	for {
		if bytes.Equal(thisOID, id[:]) {
			size := min(offset+int(l), offset+objectwire.NonPayloadFieldsBufferLength)
			if n < size {
				_, err = io.ReadFull(f, buf[n:size])
				if err != nil {
					return nil, f, fmt.Errorf("read up to size: %w", err)
				}
			}

			f := io.ReadCloser(f)
			if buffered := uint32(size - offset); l > buffered {
				f = struct {
					io.Reader
					io.Closer
				}{Reader: io.LimitReader(f, int64(l-buffered)), Closer: f}
			}

			return buf[offset:size], f, nil
		}

		offset += int(l)
		if n-offset < combinedDataOff {
			if offset > n {
				_, err = f.Seek(int64(offset-n), io.SeekCurrent)
				if err != nil {
					return nil, f, err
				}
			}
			n = copy(buf, buf[min(offset, n):n])
			offset = 0
			k, err := io.ReadFull(f, buf[n:n+objectwire.NonPayloadFieldsBufferLength])
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, f, fmt.Errorf("read full: %w", err)
			}
			if k == 0 {
				return nil, f, fmt.Errorf("file was found, but this object is not in it: %w", io.ErrUnexpectedEOF)
			}
			n += k
		}

		thisOID, l = parseCombinedPrefix(buf[offset:])
		if thisOID == nil {
			return nil, f, errors.New("malformed combined file")
		}

		offset += combinedDataOff
	}
}

// readHeaderAndPayload reads an object header from the file and returns reader for payload.
// This function takes ownership of the io.ReadCloser and will close it if it does not return it.
func (t *FSTree) readHeaderAndPayload(f io.ReadCloser, initial []byte) (*object.Object, io.ReadSeekCloser, error) {
	initial, reader, err := t.preprocessStreamHead(f, initial)
	if err != nil {
		return nil, nil, err
	}

	if reader == nil {
		var obj object.Object
		err = obj.Unmarshal(initial)
		if err != nil {
			return nil, nil, fmt.Errorf("unmarshal object: %w", err)
		}

		pld := obj.Payload()

		obj.SetPayload(nil)

		return &obj, &payloadReader{
			Reader: bytes.NewReader(pld),
			close:  func() error { return nil },
		}, nil
	}

	obj, payloadPrefix, err := objectwire.ExtractHeaderAndPayload(initial)
	if err != nil {
		_ = reader.Close()
		return nil, nil, fmt.Errorf("extract header and payload: %w", err)
	}

	return obj, &payloadReader{
		Reader: io.MultiReader(bytes.NewReader(payloadPrefix), reader),
		close:  reader.Close,
	}, nil
}

func (t *FSTree) preprocessStreamHead(f io.ReadCloser, initial []byte) ([]byte, io.ReadCloser, error) {
	var err error
	if len(initial) < objectwire.NonPayloadFieldsBufferLength {
		_ = f.Close()
		initial, err = t.Decompress(initial)
		if err != nil {
			return nil, nil, fmt.Errorf("decompress initial data: %w", err)
		}
		return initial, nil, nil
	}

	reader := f

	if t.IsCompressed(initial) {
		decoder, err := zstd.NewReader(io.MultiReader(bytes.NewReader(initial), f))
		if err != nil {
			return nil, nil, fmt.Errorf("zstd decoder: %w", err)
		}
		reader = readerTwoClosers{
			ReadCloser: decoder.IOReadCloser(),
			baseCloser: f,
		}

		buf := make([]byte, objectwire.NonPayloadFieldsBufferLength)
		n, err := decoder.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			reader.Close()
			return nil, nil, fmt.Errorf("zstd read: %w", err)
		}
		initial = buf[:n]
	}

	return initial, reader, nil
}

type payloadReader struct {
	io.Reader
	close func() error
}

func (p *payloadReader) Close() error {
	return p.close()
}

// Seek implements io.Seeker interface for payloadReader.
// If the Reader does not support seeking, it will discard the data until the offset
// is reached, but only if whence is io.SeekStart. If whence is not io.SeekStart,
// it returns an error indicating that seeking is not supported.
func (p *payloadReader) Seek(offset int64, whence int) (int64, error) {
	return seekReader(p.Reader, offset, whence)
}

func seekReader(r io.Reader, offset int64, whence int) (int64, error) {
	if seeker, ok := r.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	if whence == io.SeekStart {
		return io.CopyN(io.Discard, r, offset)
	}
	return 0, errors.New("payload reader does not support seeking")
}
