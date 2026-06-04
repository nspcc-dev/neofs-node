package object

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-node/internal/qstream"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// statusStreamWriter prepends the qstream.StatusOK byte before the first body
// byte. It lets the OK status be committed lazily, so that errors detected
// before any payload is produced are reported via writeStreamError instead.
type statusStreamWriter struct {
	w       io.Writer
	started bool
}

func (s *statusStreamWriter) Write(p []byte) (int, error) {
	if !s.started {
		s.started = true
		if _, err := s.w.Write([]byte{qstream.StatusOK}); err != nil {
			return 0, err
		}
	}
	return s.w.Write(p)
}

// writeStreamError reports an error before any response body has been written:
// the qstream.StatusError byte followed by a serialized status.Status.
func writeStreamError(w io.Writer, err error) {
	_, _ = w.Write([]byte{qstream.StatusError})
	if err == nil {
		return
	}
	if st := util.ToStatus(err); st != nil {
		if b, mErr := proto.Marshal(st); mErr == nil {
			_, _ = w.Write(b)
		}
	}
}

// streamWriter is the getsvc.ObjectWriter used for assembled/forwarded reads. It
// emits the same ad-hoc framing as copyStream: field 3 = object header, field 4
// = payload.
type streamWriter struct {
	io.Writer
}

func (w streamWriter) WriteHeader(hdr *object.Object) error {
	var b [32]byte

	pref := protowire.AppendTag(b[:0], 3, protowire.BytesType)
	hdrBin := hdr.CutPayload().Marshal()
	pref = protowire.AppendVarint(pref, uint64(len(hdrBin)))
	if _, err := w.Write(pref); err != nil {
		return err
	}
	if _, err := w.Write(hdrBin); err != nil {
		return err
	}

	pref = protowire.AppendTag(b[:0], 4, protowire.BytesType)
	pref = protowire.AppendVarint(pref, hdr.PayloadSize())
	_, err := w.Write(pref)
	return err
}

func (w streamWriter) WriteChunk(data []byte) error {
	_, err := w.Write(data)
	return err
}

// ServeGetStream handles a single GET over a raw QUIC bidirectional stream. The
// request (serialized object.GetRequest, terminated by the peer's FIN) is read
// from rw; the response is written back as described in the qstream package.
func (s *Server) ServeGetStream(ctx context.Context, rw io.ReadWriteCloser) {
	var (
		err         error
		recheckEACL bool
		t           = time.Now()
	)
	defer func() { s.pushOpExecResult(stat.MethodObjectGet, err, t) }()
	defer func() { _ = rw.Close() }()

	sw := &statusStreamWriter{w: rw}

	buf, err := io.ReadAll(io.LimitReader(rw, qstream.MaxRequestSize))
	if err != nil {
		writeStreamError(rw, err)
		return
	}
	if len(buf) == 0 {
		bad := new(apistatus.BadRequest)
		bad.SetMessage("empty request")
		err = bad
		writeStreamError(rw, err)
		return
	}

	req := new(protoobject.GetRequest)
	if err = proto.Unmarshal(buf, req); err != nil {
		bad := new(apistatus.BadRequest)
		bad.SetMessage("malformed request: " + err.Error())
		err = bad
		writeStreamError(rw, err)
		return
	}

	if err = icrypto.VerifyRequestSignatures(req); err != nil {
		writeStreamError(rw, err)
		return
	}

	if s.fsChain.LocalNodeUnderMaintenance() {
		err = apistatus.ErrNodeUnderMaintenance
		writeStreamError(rw, err)
		return
	}

	reqInfo, err := s.reqInfoProc.GetRequestToInfo(req)
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			bad := new(apistatus.BadRequest)
			bad.SetMessage(err.Error())
			err = bad
		}
		writeStreamError(rw, err)
		return
	}
	if !s.aclChecker.CheckBasicACL(reqInfo) {
		err = basicACLErr(reqInfo)
		writeStreamError(rw, err)
		return
	}
	if err = s.aclChecker.CheckEACL(ctx, req, reqInfo); err != nil {
		if !errors.Is(err, aclsvc.ErrNotMatched) {
			err = eACLErr(reqInfo, err)
			writeStreamError(rw, err)
			return
		}
		recheckEACL = true
		err = nil
	}

	p, err := convertGetPrmStream(s.signer, reqInfo.Container, req, buf, &getStream{
		w:            sw,
		srv:          s,
		reqInfo:      reqInfo,
		recheckEACL:  recheckEACL,
		signResponse: needSignGetResponse(req),
	})
	if err != nil {
		if !errors.Is(err, apistatus.Error) {
			bad := new(apistatus.BadRequest)
			bad.SetMessage(err.Error())
			err = bad
		}
		writeStreamError(rw, err)
		return
	}

	hdrRespBuf, hdrBuf := getBufferForHeadResponse()
	defer hdrRespBuf.Free()

	hdrLen := -1
	var stream io.ReadCloser
	defer func() {
		if stream != nil {
			_ = stream.Close()
		}
	}()
	p.WithBuffer(hdrBuf, func(ln int, st io.ReadCloser) { hdrLen, stream = ln, st })

	err = s.handlers.Get(ctx, p)
	if err != nil {
		if !sw.started {
			writeStreamError(rw, err)
		}
		// otherwise the truncated stream signals the error to the client.
		return
	}

	if hdrLen < 0 {
		// Response already streamed through sw, either by the forwarder
		// (continueQUIC) or by streamWriter for an assembled/EC object.
		return
	}

	idf, sigf, hdrf, err := iobject.GetNonPayloadFieldBounds(hdrBuf[:hdrLen])
	if err != nil {
		if !sw.started {
			writeStreamError(rw, err)
		}
		return
	}

	if recheckEACL { // previous check didn't match, but we have a header now.
		if eErr := s.aclChecker.CheckEACL(ctx, hdrBuf[hdrf.ValueFrom:hdrf.To], reqInfo); eErr != nil && !errors.Is(eErr, aclsvc.ErrNotMatched) {
			err = eACLErr(reqInfo, eErr)
			if !sw.started {
				writeStreamError(rw, err)
			}
			return
		}
	}

	pldFldOff := max(idf.To, sigf.To, hdrf.To)
	err = copyStream(sw, hdrBuf, hdrLen, stream, pldFldOff)
}

// copyStream writes the raw object response into w: field 3 (object header) then
// field 4 (payload size + streamed payload).
func copyStream(w io.Writer, hdrBuf []byte, hdrLen int, stream io.Reader, pldFldOff int) error {
	var pref [16]byte
	p := protowire.AppendTag(pref[:0], 3, protowire.BytesType)
	p = protowire.AppendVarint(p, uint64(pldFldOff))
	if _, err := w.Write(p); err != nil {
		return err
	}
	if _, err := w.Write(hdrBuf[:pldFldOff]); err != nil {
		return err
	}

	if pldFldOff >= hdrLen {
		p = protowire.AppendTag(pref[:0], 4, protowire.BytesType)
		p = protowire.AppendVarint(p, 0)
		_, err := w.Write(p)
		return err
	}

	num, typ, tagSz := protowire.ConsumeTag(hdrBuf[pldFldOff:hdrLen])
	if tagSz < 0 || num != 4 || typ != protowire.BytesType {
		return fmt.Errorf("bad payload field tag at offset %d", pldFldOff)
	}
	payloadSize, lenSz := protowire.ConsumeVarint(hdrBuf[pldFldOff+tagSz : hdrLen])
	if lenSz < 0 {
		return errors.New("bad payload length varint")
	}
	payloadValueStart := pldFldOff + tagSz + lenSz

	p = protowire.AppendTag(pref[:0], 4, protowire.BytesType)
	p = protowire.AppendVarint(p, payloadSize)
	if _, err := w.Write(p); err != nil {
		return err
	}
	if payloadValueStart < hdrLen {
		if _, err := w.Write(hdrBuf[payloadValueStart:hdrLen]); err != nil {
			return err
		}
	}
	if stream != nil {
		if _, err := io.Copy(w, stream); err != nil {
			return err
		}
	}
	return nil
}

// convertGetPrmStream builds get-service parameters for the raw QUIC GET path.
func convertGetPrmStream(signer ecdsa.PrivateKey, cnr container.Container, req *protoobject.GetRequest, buf []byte, stream *getStream) (getsvc.Prm, error) {
	body := req.GetBody()
	ma := body.GetAddress()
	if ma == nil { // includes nil body
		return getsvc.Prm{}, errors.New("missing object address")
	}

	var addr oid.Address
	if err := addr.FromProtoMessage(ma); err != nil {
		return getsvc.Prm{}, fmt.Errorf("invalid object address: %w", err)
	}

	cp, err := objutil.CommonPrmFromRequest(req)
	if err != nil {
		return getsvc.Prm{}, err
	}

	var p getsvc.Prm
	p.SetCommonParameters(cp)
	p.WithAddress(addr)
	p.WithContainer(cnr)
	p.WithRawFlag(body.Raw)
	p.SetObjectWriter(streamWriter{stream.w})
	if cp.LocalOnly() {
		return p, nil
	}

	var onceResign sync.Once
	meta := req.GetMetaHeader()
	if meta == nil {
		return getsvc.Prm{}, errors.New("missing meta header")
	}

	p.SetRequestForwarder(func(ctx context.Context, c client.MultiAddressClient) error {
		var resignErr error
		onceResign.Do(func() {
			req.MetaHeader = &protosession.RequestMetaHeader{
				Ttl:    meta.GetTtl() - 1,
				Origin: meta,
			}
			req.VerifyHeader, resignErr = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(signer), req, nil)
			if resignErr != nil {
				return
			}
			buf, resignErr = proto.MarshalOptions{}.MarshalAppend(buf[:0], req)
		})
		if resignErr != nil {
			return resignErr
		}
		return c.ForAnyQUICStream(ctx, func(ctx context.Context, conn *quic.Conn, _ string) error {
			return continueQUIC(ctx, stream.w, buf, conn)
		})
	})
	return p, nil
}

// continueQUIC forwards a GET to a peer node over QUIC and copies its raw
// response body into w.
func continueQUIC(ctx context.Context, w io.Writer, buf []byte, conn *quic.Conn) error {
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open forward stream: %w", err)
	}
	defer st.CancelRead(0)

	if dl, ok := ctx.Deadline(); ok {
		_ = st.SetDeadline(dl)
	}

	if _, err = st.Write(buf); err != nil {
		return fmt.Errorf("write forward request: %w", err)
	}
	if err = st.Close(); err != nil { // FIN the send side
		return fmt.Errorf("close forward request: %w", err)
	}

	var sb [1]byte
	if _, err = io.ReadFull(st, sb[:]); err != nil {
		return fmt.Errorf("read forward status: %w", err)
	}
	if sb[0] != qstream.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(st, qstream.MaxRequestSize))
		var stt protostatus.Status
		if proto.Unmarshal(body, &stt) == nil {
			if e := apistatus.ToError(&stt); e != nil {
				return e
			}
		}
		return fmt.Errorf("forwarded peer returned error status %d", sb[0])
	}

	_, err = io.Copy(w, st)
	return err
}
