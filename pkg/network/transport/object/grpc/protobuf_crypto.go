package object

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	refsv2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	fieldNumSignaturePubKey = 1
	fieldNumSignatureValue  = 2
	fieldNumSignatureScheme = 3
)

const (
	publicKeyLen              = 33
	walletConnectSignatureLen = keys.SignatureLen + 16 // + salt
	sigLenLimitHard           = 1 << 8
)

type cryptoBuffersT struct{ pk, ec, wc, ms, mh sync.Pool }

var cryptoBuffers = cryptoBuffersT{
	pk: sync.Pool{New: func() any { return make([]byte, publicKeyLen) }},
	ec: sync.Pool{New: func() any { return make([]byte, keys.SignatureLen) }},
	wc: sync.Pool{New: func() any { return make([]byte, walletConnectSignatureLen) }},
}

func (x *cryptoBuffersT) getPublicKey() []byte   { return x.pk.Get().([]byte)[:publicKeyLen] }
func (x *cryptoBuffersT) putPublicKey(b []byte)  { x.pk.Put(b) }
func (x *cryptoBuffersT) getECSignature() []byte { return x.ec.Get().([]byte)[:keys.SignatureLen] }
func (x *cryptoBuffersT) getWCSignature() []byte {
	return x.wc.Get().([]byte)[:walletConnectSignatureLen]
}
func (x *cryptoBuffersT) putSignature(b []byte) {
	if cap(b) == walletConnectSignatureLen {
		x.wc.Put(b)
		return
	}
	x.ec.Put(b)
}

type decodedSignature struct {
	pubKey    neofscrypto.PublicKey
	pubKeyBuf []byte

	value []byte
}

func (x decodedSignature) releaseBuffers() {
	if x.pubKeyBuf != nil {
		cryptoBuffers.putPublicKey(x.pubKeyBuf)
	}
	if x.value != nil {
		cryptoBuffers.putSignature(x.value)
	}
}

// TODO: may be needed for other RPC servers, move to shared codespace
func readSignatureField(r bytesReader) (res decodedSignature, err error) {
	defer func() {
		if err != nil {
			res.releaseBuffers()
		}
	}()

	ln, err := readLENFieldSize(r)
	if err != nil {
		return
	}

	r = limitBytesReader(r, ln)

	var num protowire.Number
	var typ protowire.Type
	var seenScheme bool
	var scheme neofscrypto.Scheme

	for {
		num, typ, err = readFieldTag(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return
		}

		switch num {
		default:
			err = discardField(r, typ)
			if err != nil {
				return
			}
		case fieldNumSignaturePubKey:
			if res.pubKeyBuf != nil {
				return res, fmt.Errorf("repeated field #%d (public key)", num)
			}
			if typ != protowire.BytesType {
				return res, fmt.Errorf("field #%d (public key) is not of bytes type", num)
			}
			res.pubKeyBuf = cryptoBuffers.getPublicKey()
			err = readLENFieldFull(r, res.pubKeyBuf)
			if err == nil && seenScheme {
				res.pubKey, err = decodePublicKeyByScheme(scheme, res.pubKeyBuf)
			}
			if err != nil {
				return res, fmt.Errorf("invalid field #%d (public key): %w", num, err)
			}
		case fieldNumSignatureValue:
			if res.value != nil {
				return res, fmt.Errorf("repeated field #%d (value)", num)
			}
			if typ != protowire.BytesType {
				return res, fmt.Errorf("field #%d (value) is not of bytes type", num)
			}
			if !seenScheme || scheme == neofscrypto.ECDSA_WALLETCONNECT {
				// get the biggest buffer if scheme is not known yet
				res.value = cryptoBuffers.getWCSignature()
			} else {
				res.value = cryptoBuffers.getECSignature()
			}
			n, err := readLENFieldAtMost(r, res.value)
			if err != nil {
				return res, fmt.Errorf("invalid field #%d (value): invalid LEN prefix: %w", num, err)
			}
			res.value = res.value[:n]
		case fieldNumSignatureScheme:
			if seenScheme {
				return res, fmt.Errorf("repeated field #%d (scheme)", num)
			}
			seenScheme = true
			if typ != protowire.VarintType {
				return res, fmt.Errorf("field #%d (scheme) is not of VARINT wire type", num)
			}
			i, err := binary.ReadUvarint(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return res, fmt.Errorf("invalid field #%d (scheme): %w", num, err)
			}
			switch refs.SignatureScheme(i) {
			default:
				return res, fmt.Errorf("unsupported field #%d (scheme) enum value", num)
			case refs.SignatureScheme_ECDSA_SHA512:
				scheme = neofscrypto.ECDSA_SHA512
			case refs.SignatureScheme_ECDSA_RFC6979_SHA256:
				scheme = neofscrypto.ECDSA_DETERMINISTIC_SHA256
			case refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
				scheme = neofscrypto.ECDSA_WALLETCONNECT
			}
		}
	}

	if res.value == nil {
		return res, fmt.Errorf("%w #%d (value)", errMissingField, fieldNumSignatureValue)
	}
	if res.pubKey == nil {
		if res.pubKeyBuf == nil {
			return res, fmt.Errorf("%w #%d (public key)", errMissingField, fieldNumSignaturePubKey)
		}
		res.pubKey, err = decodePublicKeyByScheme(scheme, res.pubKeyBuf)
		if err != nil {
			return
		}
	}

	return
}

func decodePublicKeyByScheme(s neofscrypto.Scheme, b []byte) (neofscrypto.PublicKey, error) {
	switch s {
	default:
		return nil, errUnsupportedSignatureScheme
	case neofscrypto.ECDSA_SHA512:
		var k neofsecdsa.PublicKey
		err := k.Decode(b)
		if err != nil {
			return nil, fmt.Errorf("invalid ECDSA public key: %w", err)
		}
		return &k, nil
	case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
		var k neofsecdsa.PublicKeyRFC6979
		err := k.Decode(b)
		if err != nil {
			return nil, fmt.Errorf("invalid ECDSA public key: %w", err)
		}
		return &k, nil
	case neofscrypto.ECDSA_WALLETCONNECT:
		var k neofsecdsa.PublicKeyWalletConnect
		err := k.Decode(b)
		if err != nil {
			return nil, fmt.Errorf("invalid ECDSA public key: %w", err)
		}
		return &k, nil
	}
}

func parseSignatureMessage(b []byte) (*refsv2.Signature, error) {
	var res refsv2.Signature

	f, err := seekField(b, fieldNumSignaturePubKey, protowire.BytesType)
	if err != nil && !errors.Is(err, errMissingField) {
		return nil, fmt.Errorf("invalid field #%d: %w", fieldNumSignaturePubKey, err)
	} else if err == nil {
		res.SetKey(b[f.valOff:][:f.valLen])
	}

	f, err = seekField(b, fieldNumSignatureValue, protowire.BytesType)
	if err != nil && !errors.Is(err, errMissingField) {
		return nil, fmt.Errorf("invalid field #%d: %w", fieldNumSignatureValue, err)
	} else if err == nil {
		res.SetSign(b[f.valOff:][:f.valLen])
	}

	f, err = seekField(b, fieldNumSignatureScheme, protowire.VarintType)
	if err != nil && !errors.Is(err, errMissingField) {
		return nil, fmt.Errorf("invalid field #%d: %w", fieldNumSignatureScheme, err)
	} else if err == nil {
		u, _ := protowire.ConsumeVarint(b[f.valOff:][:f.valLen])
		if u > math.MaxUint32 {
			return nil, fmt.Errorf("invalid field #%d (scheme): too big enum value %d", fieldNumSignatureScheme, u)
		}
		res.SetScheme(refsv2.SignatureScheme(u))
	}

	return &res, nil
}
