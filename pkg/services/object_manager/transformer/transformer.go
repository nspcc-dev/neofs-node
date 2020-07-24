package transformer

import (
	"context"
	"crypto/sha256"
	"io"
	"sort"
	"time"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/pkg/errors"
)

type (
	// Type is a type alias of
	// Type from object package of neofs-api-go.
	Type = object.Transform_Type

	// ProcUnit groups the information about transforming unit.
	ProcUnit struct {
		Head    *Object
		Payload io.Reader
	}

	// ProcUnitHandler is a handling ProcUnit function.
	ProcUnitHandler func(context.Context, ProcUnit) error

	// Transformer is an interface of object transformer.
	Transformer interface {
		Transform(context.Context, ProcUnit, ...ProcUnitHandler) error
	}

	// EpochReceiver is an interface of epoch number container with read access.
	EpochReceiver interface {
		Epoch() uint64
	}

	transformer struct {
		tPrelim  Transformer
		tSizeLim Transformer
		tSign    Transformer
	}

	preliminaryTransformer struct {
		fMoulder  Transformer
		sgMoulder Transformer
	}

	fieldMoulder struct {
		epochRecv EpochReceiver
	}

	sgMoulder struct {
		sgInfoRecv storagegroup.InfoReceiver
	}

	sizeLimiter struct {
		limit     uint64
		epochRecv EpochReceiver
	}

	headSigner struct {
		verifier verifier.Verifier
	}

	emptyReader struct{}

	// Params groups the parameters of object transformer's constructor.
	Params struct {
		SGInfoReceiver storagegroup.InfoReceiver
		EpochReceiver  EpochReceiver
		SizeLimit      uint64
		Verifier       verifier.Verifier
	}
)

// ErrPayloadEOF is returned by Transformer that
// received unexpected end of object payload.
var ErrPayloadEOF = errors.New("payload EOF")

const (
	verifyHeadersCount = 2 // payload checksum, integrity
	splitHeadersCount  = 4 // flag, parent, left, right

	transformerInstanceFailMsg = "could not create transformer instance"

	// PrivateSessionToken is a context key for session.PrivateToken.
	PrivateSessionToken = "private token"

	// PublicSessionToken is a context key for service.SessionToken.
	PublicSessionToken = "public token"
)

// ErrInvalidSGLinking is returned by Transformer that received
// an object with broken storage group links.
var ErrInvalidSGLinking = errors.New("invalid storage group linking")

var (
	errNoToken             = errors.New("no token provided")
	errEmptyInput          = errors.New("empty input")
	errChainNotFound       = errors.New("chain not found")
	errCutChain            = errors.New("GetChain failed: chain is not full")
	errMissingTransformHdr = errors.New("cannot find transformer header")
	errEmptySGInfoRecv     = errors.New("empty storage group info receivers")
	errInvalidSizeLimit    = errors.New("non-positive object size limit")
	errEmptyEpochReceiver  = errors.New("empty epoch receiver")
	errEmptyVerifier       = errors.New("empty object verifier")
)

// NewTransformer is an object transformer's constructor.
func NewTransformer(p Params) (Transformer, error) {
	switch {
	case p.SizeLimit <= 0:
		return nil, errors.Wrap(errInvalidSizeLimit, transformerInstanceFailMsg)
	case p.EpochReceiver == nil:
		return nil, errors.Wrap(errEmptyEpochReceiver, transformerInstanceFailMsg)
	case p.SGInfoReceiver == nil:
		return nil, errors.Wrap(errEmptySGInfoRecv, transformerInstanceFailMsg)
	case p.Verifier == nil:
		return nil, errors.Wrap(errEmptyVerifier, transformerInstanceFailMsg)
	}

	return &transformer{
		tPrelim: &preliminaryTransformer{
			fMoulder: &fieldMoulder{
				epochRecv: p.EpochReceiver,
			},
			sgMoulder: &sgMoulder{
				sgInfoRecv: p.SGInfoReceiver,
			},
		},
		tSizeLim: &sizeLimiter{
			limit:     p.SizeLimit,
			epochRecv: p.EpochReceiver,
		},
		tSign: &headSigner{
			verifier: p.Verifier,
		},
	}, nil
}

func (s *transformer) Transform(ctx context.Context, unit ProcUnit, handlers ...ProcUnitHandler) error {
	if err := s.tPrelim.Transform(ctx, unit); err != nil {
		return err
	}

	return s.tSizeLim.Transform(ctx, unit, func(ctx context.Context, unit ProcUnit) error {
		return s.tSign.Transform(ctx, unit, handlers...)
	})
}

func (s *preliminaryTransformer) Transform(ctx context.Context, unit ProcUnit, _ ...ProcUnitHandler) error {
	if err := s.fMoulder.Transform(ctx, unit); err != nil {
		return err
	}

	return s.sgMoulder.Transform(ctx, unit)
}

// TODO: simplify huge function.
func (s *sizeLimiter) Transform(ctx context.Context, unit ProcUnit, handlers ...ProcUnitHandler) error {
	if unit.Head.SystemHeader.PayloadLength <= s.limit {
		homoHashHdr := &object.Header_HomoHash{HomoHash: hash.Sum(make([]byte, 0))}

		unit.Head.AddHeader(&object.Header{Value: homoHashHdr})

		buf := make([]byte, unit.Head.SystemHeader.PayloadLength)

		if err := readChunk(unit, buf, nil, homoHashHdr); err != nil {
			return err
		}

		unit.Head.Payload = buf

		return procHandlers(ctx, EmptyPayloadUnit(unit.Head), handlers...)
	}

	var (
		err       error
		buf       = make([]byte, s.limit)
		hAcc      = sha256.New()
		srcHdrLen = len(unit.Head.Headers)
		pObj      = unit.Head
		resObj    = ProcUnit{
			Head: &Object{
				SystemHeader: object.SystemHeader{
					Version: pObj.SystemHeader.Version,
					OwnerID: pObj.SystemHeader.OwnerID,
					CID:     pObj.SystemHeader.CID,
					CreatedAt: object.CreationPoint{
						UnixTime: time.Now().Unix(),
						Epoch:    s.epochRecv.Epoch(),
					},
				},
			},
			Payload: unit.Payload,
		}
		left, right         = &object.Link{Type: object.Link_Previous}, &object.Link{Type: object.Link_Next}
		hashAccHdr, hashHdr = new(object.Header_PayloadChecksum), new(object.Header_PayloadChecksum)
		homoHashAccHdr      = &object.Header_HomoHash{HomoHash: hash.Sum(make([]byte, 0))}
		childCount          = pObj.SystemHeader.PayloadLength/s.limit + 1
	)

	if right.ID, err = refs.NewObjectID(); err != nil {
		return err
	}

	splitHeaders := make([]object.Header, 0, 3*verifyHeadersCount+splitHeadersCount+childCount)

	splitHeaders = append(splitHeaders, pObj.Headers...)
	splitHeaders = append(splitHeaders, []object.Header{
		{Value: &object.Header_Transform{Transform: &object.Transform{Type: object.Transform_Split}}},
		{Value: &object.Header_Link{Link: &object.Link{
			Type: object.Link_Parent,
			ID:   unit.Head.SystemHeader.ID,
		}}},
		{Value: &object.Header_Link{Link: left}},
		{Value: &object.Header_Link{Link: right}},
		{Value: hashHdr},
		{Value: &object.Header_Integrity{Integrity: new(object.IntegrityHeader)}},
		{Value: homoHashAccHdr},
		{Value: hashAccHdr},
		{Value: &object.Header_Integrity{Integrity: new(object.IntegrityHeader)}},
	}...)

	children := splitHeaders[srcHdrLen+2*verifyHeadersCount+splitHeadersCount+1:]
	pObj.Headers = splitHeaders[:srcHdrLen+2*verifyHeadersCount+splitHeadersCount]

	for tail := pObj.SystemHeader.PayloadLength; tail > 0; tail -= min(tail, s.limit) {
		size := min(tail, s.limit)

		resObj.Head.Headers = pObj.Headers[:len(pObj.Headers)-verifyHeadersCount-1]
		if err = readChunk(resObj, buf[:size], hAcc, homoHashAccHdr); err != nil {
			return err
		}

		resObj.Head.SystemHeader.PayloadLength = size
		resObj.Head.Payload = buf[:size]
		left.ID, resObj.Head.SystemHeader.ID = resObj.Head.SystemHeader.ID, right.ID

		if tail <= s.limit {
			right.ID = ObjectID{}

			temp := make([]object.Header, verifyHeadersCount+1) // +1 for homomorphic hash

			copy(temp, pObj.Headers[srcHdrLen:])

			hashAccHdr.PayloadChecksum = hAcc.Sum(nil)

			copy(pObj.Headers[srcHdrLen:srcHdrLen+verifyHeadersCount+1],
				pObj.Headers[len(pObj.Headers)-verifyHeadersCount:])

			resObj.Head.Headers = pObj.Headers[:srcHdrLen+verifyHeadersCount]

			if err = signWithToken(ctx, &Object{
				SystemHeader: pObj.SystemHeader,
				Headers:      resObj.Head.Headers,
			}); err != nil {
				return err
			}

			copy(pObj.Headers[srcHdrLen+2*(verifyHeadersCount+1):],
				pObj.Headers[srcHdrLen+verifyHeadersCount+1:srcHdrLen+verifyHeadersCount+splitHeadersCount])

			copy(pObj.Headers[srcHdrLen+verifyHeadersCount+1:], temp)

			resObj.Head.Headers = pObj.Headers[:len(pObj.Headers)]
		} else if right.ID, err = refs.NewObjectID(); err != nil {
			return err
		}

		if err := procHandlers(ctx, EmptyPayloadUnit(resObj.Head), handlers...); err != nil {
			return err
		}

		children = append(children, object.Header{Value: &object.Header_Link{Link: &object.Link{
			Type: object.Link_Child,
			ID:   resObj.Head.SystemHeader.ID,
		}}})
	}

	pObj.SystemHeader.PayloadLength = 0
	pObj.Headers = append(pObj.Headers[:srcHdrLen], children...)

	if err := readChunk(unit, nil, nil, nil); err != nil {
		return err
	}

	return procHandlers(ctx, EmptyPayloadUnit(pObj), handlers...)
}

func readChunk(unit ProcUnit, buf []byte, hAcc io.Writer, homoHashAcc *object.Header_HomoHash) (err error) {
	var csHdr *object.Header_PayloadChecksum

	if _, v := unit.Head.LastHeader(object.HeaderType(object.PayloadChecksumHdr)); v == nil {
		csHdr = new(object.Header_PayloadChecksum)

		unit.Head.Headers = append(unit.Head.Headers, object.Header{Value: csHdr})
	} else {
		csHdr = v.Value.(*object.Header_PayloadChecksum)
	}

	if _, err = io.ReadFull(unit.Payload, buf); err != nil && err != io.EOF {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = ErrPayloadEOF
		}

		return
	} else if hAcc != nil {
		if _, err = hAcc.Write(buf); err != nil {
			return
		}
	}

	if homoHashAcc != nil {
		if homoHashAcc.HomoHash, err = hash.Concat([]hash.Hash{homoHashAcc.HomoHash, hash.Sum(buf)}); err != nil {
			return
		}
	}

	h := sha256.Sum256(buf)
	csHdr.PayloadChecksum = h[:]

	return nil
}

func (s *headSigner) Transform(ctx context.Context, unit ProcUnit, handlers ...ProcUnitHandler) error {
	if s.verifier.Verify(ctx, unit.Head) != nil {
		if err := signWithToken(ctx, unit.Head); err != nil {
			return err
		}
	}

	return procHandlers(ctx, unit, handlers...)
}

func signWithToken(ctx context.Context, obj *Object) error {
	integrityHdr := new(object.IntegrityHeader)

	if pToken, ok := ctx.Value(PrivateSessionToken).(session.PrivateToken); !ok {
		return errNoToken
	} else if hdrData, err := verifier.MarshalHeaders(obj, len(obj.Headers)); err != nil {
		return err
	} else {
		cs := sha256.Sum256(hdrData)
		integrityHdr.SetHeadersChecksum(cs[:])
		if err = service.AddSignatureWithKey(pToken.PrivateKey(), integrityHdr); err != nil {
			return err
		}
	}

	obj.AddHeader(&object.Header{Value: &object.Header_Integrity{Integrity: integrityHdr}})

	return nil
}

func (s *fieldMoulder) Transform(ctx context.Context, unit ProcUnit, _ ...ProcUnitHandler) (err error) {
	token, ok := ctx.Value(PublicSessionToken).(*service.Token)
	if !ok {
		return errNoToken
	}

	unit.Head.AddHeader(&object.Header{
		Value: &object.Header_Token{
			Token: token,
		},
	})

	if unit.Head.SystemHeader.ID.Empty() {
		if unit.Head.SystemHeader.ID, err = refs.NewObjectID(); err != nil {
			return
		}
	}

	if unit.Head.SystemHeader.CreatedAt.UnixTime == 0 {
		unit.Head.SystemHeader.CreatedAt.UnixTime = time.Now().Unix()
	}

	if unit.Head.SystemHeader.CreatedAt.Epoch == 0 {
		unit.Head.SystemHeader.CreatedAt.Epoch = s.epochRecv.Epoch()
	}

	if unit.Head.SystemHeader.Version == 0 {
		unit.Head.SystemHeader.Version = 1
	}

	return nil
}

func (s *sgMoulder) Transform(ctx context.Context, unit ProcUnit, _ ...ProcUnitHandler) error {
	sgLinks := unit.Head.Links(object.Link_StorageGroup)

	group, err := unit.Head.StorageGroup()

	if nonEmptyList := len(sgLinks) > 0; (err == nil) != nonEmptyList {
		return ErrInvalidSGLinking
	} else if err != nil || !group.Empty() {
		return nil
	}

	sort.Sort(storagegroup.IDList(sgLinks))

	sgInfo, err := s.sgInfoRecv.GetSGInfo(ctx, unit.Head.SystemHeader.CID, sgLinks)
	if err != nil {
		return err
	}

	unit.Head.SetStorageGroup(sgInfo)

	return nil
}

func procHandlers(ctx context.Context, unit ProcUnit, handlers ...ProcUnitHandler) error {
	for i := range handlers {
		if err := handlers[i](ctx, unit); err != nil {
			return err
		}
	}

	return nil
}

func (*emptyReader) Read([]byte) (n int, err error) { return 0, io.EOF }

// EmptyPayloadUnit returns ProcUnit with Object from argument and empty payload reader
// that always returns (0, io.EOF).
func EmptyPayloadUnit(head *Object) ProcUnit { return ProcUnit{Head: head, Payload: new(emptyReader)} }

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

// GetChain builds a list of objects in the hereditary chain.
// In case of impossibility to do this, an error is returned.
func GetChain(srcObjs ...Object) ([]Object, error) {
	var (
		err       error
		first, id ObjectID
		res       = make([]Object, 0, len(srcObjs))
		m         = make(map[ObjectID]*Object, len(srcObjs))
	)

	// Fill map with all objects
	for i := range srcObjs {
		m[srcObjs[i].SystemHeader.ID] = &srcObjs[i]

		prev, err := lastLink(&srcObjs[i], object.Link_Previous)
		if err == nil && prev.Empty() { // then it is first
			id, err = lastLink(&srcObjs[i], object.Link_Next)
			if err != nil {
				return nil, errors.Wrap(err, "GetChain failed: missing first object next links")
			}

			first = srcObjs[i].SystemHeader.ID
		}
	}

	// Check first presence
	if first.Empty() {
		return nil, errChainNotFound
	}

	res = append(res, *m[first])

	// Iterate chain
	for count := 0; !id.Empty() && count < len(srcObjs); count++ {
		nextObj, ok := m[id]
		if !ok {
			return nil, errors.Errorf("GetChain failed: missing next object %s", id)
		}

		id, err = lastLink(nextObj, object.Link_Next)
		if err != nil {
			return nil, errors.Wrap(err, "GetChain failed: missing object next links")
		}

		res = append(res, *nextObj)
	}

	// Check last chain element has empty next (prevent cut chain)
	id, err = lastLink(&res[len(res)-1], object.Link_Next)
	if err != nil {
		return nil, errors.Wrap(err, "GetChain failed: missing object next links")
	} else if !id.Empty() {
		return nil, errCutChain
	}

	return res, nil
}

func deleteTransformer(o *Object, t object.Transform_Type) error {
	n, th := o.LastHeader(object.HeaderType(object.TransformHdr))
	if th == nil || th.Value.(*object.Header_Transform).Transform.Type != t {
		return errMissingTransformHdr
	}

	o.Headers = o.Headers[:n]

	return nil
}

func lastLink(o *Object, t object.Link_Type) (res ObjectID, err error) {
	for i := len(o.Headers) - 1; i >= 0; i-- {
		if v, ok := o.Headers[i].Value.(*object.Header_Link); ok {
			if v.Link.GetType() == t {
				res = v.Link.ID
				return
			}
		}
	}

	err = errors.Errorf("object.lastLink: links of type %s not found", t)

	return
}
