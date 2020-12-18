package transformer

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/pkg/errors"
)

type payloadSizeLimiter struct {
	maxSize, written uint64

	targetInit func() ObjectTarget

	target ObjectTarget

	current, parent *object.RawObject

	currentHashers, parentHashers []*payloadChecksumHasher

	previous []*objectSDK.ID

	chunkWriter io.Writer

	splitID *objectSDK.SplitID

	parAttrs []*objectSDK.Attribute
}

type payloadChecksumHasher struct {
	hasher hash.Hash

	checksumWriter func([]byte)
}

const tzChecksumSize = 64

// NewPayloadSizeLimiter returns ObjectTarget instance that restricts payload length
// of the writing object and writes generated objects to targets from initializer.
//
// Objects w/ payload size less or equal than max size remain untouched.
//
// TODO: describe behavior in details.
func NewPayloadSizeLimiter(maxSize uint64, targetInit TargetInitializer) ObjectTarget {
	return &payloadSizeLimiter{
		maxSize:    maxSize,
		targetInit: targetInit,
		splitID:    objectSDK.NewSplitID(),
	}
}

func (s *payloadSizeLimiter) WriteHeader(hdr *object.RawObject) error {
	s.current = fromObject(hdr)

	s.initialize()

	return nil
}

func (s *payloadSizeLimiter) Write(p []byte) (int, error) {
	if err := s.writeChunk(p); err != nil {
		return 0, err
	}

	return len(p), nil
}

func (s *payloadSizeLimiter) Close() (*AccessIdentifiers, error) {
	return s.release(true)
}

func (s *payloadSizeLimiter) initialize() {
	// if it is an object after the 1st
	if ln := len(s.previous); ln > 0 {
		// initialize parent object once (after 1st object)
		if ln == 1 {
			s.detachParent()
		}

		// set previous object to the last previous identifier
		s.current.SetPreviousID(s.previous[ln-1])
	}

	s.initializeCurrent()
}

func fromObject(obj *object.RawObject) *object.RawObject {
	res := object.NewRaw()
	res.SetContainerID(obj.ContainerID())
	res.SetOwnerID(obj.OwnerID())
	res.SetAttributes(obj.Attributes()...)
	res.SetType(obj.Type())

	// obj.SetSplitID creates splitHeader but we don't need to do it in case
	// of small objects, so we should make nil check.
	if obj.SplitID() != nil {
		res.SetSplitID(obj.SplitID())
	}

	return res
}

func (s *payloadSizeLimiter) initializeCurrent() {
	// initialize current object target
	s.target = s.targetInit()

	// create payload hashers
	s.currentHashers = payloadHashersForObject(s.current)

	// compose multi-writer from target and all payload hashers
	ws := make([]io.Writer, 0, 1+len(s.currentHashers)+len(s.parentHashers))

	ws = append(ws, s.target)

	for i := range s.currentHashers {
		ws = append(ws, s.currentHashers[i].hasher)
	}

	for i := range s.parentHashers {
		ws = append(ws, s.parentHashers[i].hasher)
	}

	s.chunkWriter = io.MultiWriter(ws...)
}

func payloadHashersForObject(obj *object.RawObject) []*payloadChecksumHasher {
	return []*payloadChecksumHasher{
		{
			hasher: sha256.New(),
			checksumWriter: func(cs []byte) {
				if ln := len(cs); ln != sha256.Size {
					panic(fmt.Sprintf("wrong checksum length: expected %d, has %d", ln, sha256.Size))
				}

				csSHA := [sha256.Size]byte{}
				copy(csSHA[:], cs)

				checksum := pkg.NewChecksum()
				checksum.SetSHA256(csSHA)

				obj.SetPayloadChecksum(checksum)
			},
		},
		{
			hasher: tz.New(),
			checksumWriter: func(cs []byte) {
				if ln := len(cs); ln != tzChecksumSize {
					panic(fmt.Sprintf("wrong checksum length: expected %d, has %d", ln, tzChecksumSize))
				}

				csTZ := [tzChecksumSize]byte{}
				copy(csTZ[:], cs)

				checksum := pkg.NewChecksum()
				checksum.SetTillichZemor(csTZ)

				obj.SetPayloadHomomorphicHash(checksum)
			},
		},
	}
}

func (s *payloadSizeLimiter) release(close bool) (*AccessIdentifiers, error) {
	// Arg close is true only from Close method.
	// We finalize parent and generate linking objects only if it is more
	// than 1 object in split-chain.
	withParent := close && len(s.previous) > 0

	if withParent {
		writeHashes(s.parentHashers)
		s.parent.SetPayloadSize(s.written)
		s.current.SetParent(s.parent.SDK().Object())
	}

	// release current object
	writeHashes(s.currentHashers)

	// release current, get its id
	if err := s.target.WriteHeader(s.current); err != nil {
		return nil, errors.Wrap(err, "could not write header")
	}

	ids, err := s.target.Close()
	if err != nil {
		return nil, errors.Wrap(err, "could not close target")
	}

	// save identifier of the released object
	s.previous = append(s.previous, ids.SelfID())

	if withParent {
		// generate and release linking object
		s.initializeLinking(ids.Parent())
		s.initializeCurrent()

		if _, err := s.release(false); err != nil {
			return nil, errors.Wrap(err, "could not release linking object")
		}
	}

	return ids, nil
}

func writeHashes(hashers []*payloadChecksumHasher) {
	for i := range hashers {
		hashers[i].checksumWriter(hashers[i].hasher.Sum(nil))

	}
}

func (s *payloadSizeLimiter) initializeLinking(parHdr *objectSDK.Object) {
	s.current = fromObject(s.current)
	s.current.SetParent(parHdr)
	s.current.SetChildren(s.previous...)
	s.current.SetSplitID(s.splitID)
}

func (s *payloadSizeLimiter) writeChunk(chunk []byte) error {
	// statement is true if the previous write of bytes reached exactly the boundary.
	if s.written > 0 && s.written%s.maxSize == 0 {
		if s.written == s.maxSize {
			s.prepareFirstChild()
		}

		// we need to release current object
		if _, err := s.release(false); err != nil {
			return errors.Wrap(err, "could not release object")
		}

		// initialize another object
		s.initialize()
	}

	var (
		ln         = uint64(len(chunk))
		cut        = ln
		leftToEdge = s.maxSize - s.written%s.maxSize
	)

	// write bytes no further than the boundary of the current object
	if ln > leftToEdge {
		cut = leftToEdge
	}

	if _, err := s.chunkWriter.Write(chunk[:cut]); err != nil {
		return errors.Wrap(err, "could not write chunk to target")
	}

	// increase written bytes counter
	s.written += cut

	// if there are more bytes in buffer we call method again to start filling another object
	if ln > leftToEdge {
		return s.writeChunk(chunk[cut:])
	}

	return nil
}

func (s *payloadSizeLimiter) prepareFirstChild() {
	// initialize split header with split ID on first object in chain
	s.current.InitRelations()
	s.current.SetSplitID(s.splitID)

	// cut source attributes
	s.parAttrs = s.current.Attributes()
	s.current.SetAttributes()

	// attributes will be added to parent in detachParent
}

func (s *payloadSizeLimiter) detachParent() {
	s.parent = s.current
	s.current = fromObject(s.parent)
	s.parent.ResetRelations()
	s.parent.SetSignature(nil)
	s.parentHashers = s.currentHashers

	// return source attributes
	s.parent.SetAttributes(s.parAttrs...)
}
