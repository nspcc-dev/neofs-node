package putsvc

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/klauspost/reedsolomon"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/tzhash/tz"
)

type reedSolomonEncoder struct {
	dst internal.Target

	dataShards   int
	parityShards int
	signer       neofscrypto.Signer

	srcHdr object.Object
	pldBuf bytes.Buffer
}

func newReedSolomonEncoder(dst internal.Target, dataShards, parityShards int, signer neofscrypto.Signer) internal.Target {
	return &reedSolomonEncoder{
		dst:          dst,
		dataShards:   dataShards,
		parityShards: parityShards,
		signer:       signer,
	}
}

// TODO: Place in SDK.
const (
	reedSolomonAttrIdx              = "__NEOFS__EC_RS_IDX"
	reedSolomonAttrSrcPayloadLen    = "__NEOFS__EC_RS_SRC_PAYLOAD_LEN"
	reedSolomonAttrSrcPayloadSHA256 = "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_SHA256"
	reedSolomonAttrSrcPayloadTZ     = "__NEOFS__EC_RS_SRC_PAYLOAD_HASH_TZ"
	reedSolomonAttrSrcID            = "__NEOFS__EC_RS_SRC_ID"
	reedSolomonAttrSrcSignature     = "__NEOFS__EC_RS_SRC_SIGNATURE"
)

func (x *reedSolomonEncoder) WriteHeader(hdr *object.Object) error {
	if cs, ok := x.srcHdr.PayloadChecksum(); ok && cs.Type() != checksum.SHA256 {
		return fmt.Errorf("unexpected payloaad checksum type %v instead of %v", cs.Type(), checksum.SHA256)
	}
	if cs, ok := x.srcHdr.PayloadHomomorphicHash(); ok && cs.Type() != checksum.TillichZemor {
		return fmt.Errorf("unexpected payloaad checksum type %v instead of %v", cs.Type(), checksum.TillichZemor)
	}

	x.srcHdr = *hdr
	x.pldBuf.Reset()
	return nil
}

func (x *reedSolomonEncoder) Write(p []byte) (int, error) {
	return x.pldBuf.Write(p)
}

func (x *reedSolomonEncoder) Close() (oid.ID, error) {
	if x.srcHdr.PayloadSize() == 0 {
		// TODO: avoid make
		return x.putShards(make([][]byte, x.dataShards+x.parityShards))
	}

	// TODO: Explore possibility to reset and reuse encoder for next object.
	// TODO: Explore reedsolomon.Option for performance improvement.
	// TODO: Compare with reedsolomon.StreamEncoder.
	enc, err := reedsolomon.New(x.dataShards, x.parityShards)
	if err != nil { // should never happen
		return oid.ID{}, fmt.Errorf("init Reed-Solomon(%d,%d) encoder: %w", x.dataShards, x.parityShards, err)
	}

	shards, err := enc.Split(x.pldBuf.Bytes())
	if err != nil {
		return oid.ID{}, fmt.Errorf("split data into Reed-Solomon(%d,%d) shards: %w", x.dataShards, x.parityShards, err)
	}

	if err := enc.Encode(shards); err != nil {
		return oid.ID{}, fmt.Errorf("calculate parity Reed-Solomon(%d,%d) shards: %w", x.dataShards, x.parityShards, err)
	}

	return x.putShards(shards)
}

func (x *reedSolomonEncoder) putShards(shards [][]byte) (oid.ID, error) {
	srcSig := x.srcHdr.Signature()
	srcPldSHA256, withSrcPldSHA256 := x.srcHdr.PayloadChecksum()
	srcPldTZ, withSrcPldTZ := x.srcHdr.PayloadHomomorphicHash()

	srcIdxAttr := object.NewAttribute(reedSolomonAttrIdx, "")
	srcIDAttr := object.NewAttribute(reedSolomonAttrSrcID, x.srcHdr.GetID().String())
	srcPldLenAttr := object.NewAttribute(reedSolomonAttrSrcPayloadLen, strconv.FormatUint(x.srcHdr.PayloadSize(), 10))
	var srcPldSHA256Attr, srcPldTZAttr, srcSigAttr object.Attribute
	if withSrcPldSHA256 {
		srcPldSHA256Attr = object.NewAttribute(reedSolomonAttrSrcPayloadSHA256, hex.EncodeToString(srcPldSHA256.Value()))
	}
	if withSrcPldTZ {
		srcPldTZAttr = object.NewAttribute(reedSolomonAttrSrcPayloadTZ, hex.EncodeToString(srcPldTZ.Value()))
	}
	if srcSig != nil {
		srcSigAttr = object.NewAttribute(reedSolomonAttrSrcSignature, base64.StdEncoding.EncodeToString(srcSig.Marshal()))
	}

	for i := range shards {
		shardHdr := x.srcHdr

		srcIdxAttr.SetValue(strconv.Itoa(i))

		attrs := append(shardHdr.Attributes(),
			srcIDAttr,
			srcPldLenAttr,
			srcIdxAttr,
		)
		if withSrcPldSHA256 {
			attrs = append(attrs, srcPldSHA256Attr)
		}
		if withSrcPldTZ {
			attrs = append(attrs, srcPldTZAttr)
		}
		if srcSig != nil {
			attrs = append(attrs, srcSigAttr)
		}
		shardHdr.SetAttributes(attrs...)

		shardHdr.SetPayloadSize(uint64(len(shards[i])))
		shardHdr.SetPayloadChecksum(object.CalculatePayloadChecksum(shards[i]))
		if withSrcPldTZ {
			shardHdr.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(shards[i])))
		}

		if err := shardHdr.SetIDWithSignature(x.signer); err != nil {
			return oid.ID{}, fmt.Errorf("finalize object header for Reed-Solomon(%d,%d) shard #%d: %w", x.dataShards, x.parityShards, i, err)
		}

		if err := x.dst.WriteHeader(&shardHdr); err != nil {
			return oid.ID{}, fmt.Errorf("write object header for Reed-Solomon(%d,%d) shard #%d: %w", x.dataShards, x.parityShards, i, err)
		}
		// TODO: Provide way to notice that payload buffer can be retained (but not modified) to avoid excessive copy.
		//   io.Writer prohibits retention. Instead, consider attaching payload to instance passed to WriteHeader().
		//  The caller may check that its len is PayloadSize() and not copy.
		if _, err := x.dst.Write(shards[i]); err != nil {
			return oid.ID{}, fmt.Errorf("write object payload for Reed-Solomon(%d,%d) shard #%d: %w", x.dataShards, x.parityShards, i, err)
		}
		if _, err := x.dst.Close(); err != nil {
			return oid.ID{}, fmt.Errorf("save object for Reed-Solomon(%d,%d) shard #%d: %w", x.dataShards, x.parityShards, i, err)
		}
	}

	return x.srcHdr.GetID(), nil
}
