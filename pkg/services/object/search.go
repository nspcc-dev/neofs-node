package object

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	igrpc "github.com/nspcc-dev/neofs-node/internal/grpc"
	inetmap "github.com/nspcc-dev/neofs-node/internal/netmap"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

func iterateSearchableContainerNodes(nodeSets [][]netmap.NodeInfo, repRules []uint, ecRules []iec.Rule, allNodes bool, f func(netmap.NodeInfo) bool) {
	for i := range nodeSets {
		var (
			nodeSet = nodeSets[i]
			ecIndex = i - len(repRules)
		)

		if !allNodes && ecIndex >= 0 {
			var (
				partsN    = int(ecRules[ecIndex].ParityPartNum + ecRules[ecIndex].DataPartNum)
				requiredN = int(ecRules[ecIndex].ParityPartNum + 1)
				searchN   = max(requiredN, len(nodeSet)-partsN+requiredN) // CBF 2 and alike.
			)

			if searchN < len(nodeSet) { // Stay safe in case of missing nodes.
				nodeSet = slices.Clone(nodeSet)
				rand.Shuffle(len(nodeSet), func(i, j int) {
					nodeSet[i], nodeSet[j] = nodeSet[j], nodeSet[i]
				})
				nodeSet = nodeSet[:requiredN]
			}
		}
		for _, node := range nodeSet {
			if !f(node) {
				return
			}
		}
	}
}

func (s *Server) forwardSearchRequest(ctx context.Context, req *protoobject.SearchV2Request, nodeSets [][]netmap.NodeInfo) (mem.BufferSlice, error) {
	bodyLen := req.Body.MarshaledSize()
	metaHdrLen := req.MetaHeader.MarshaledSize()
	verifHdrLen := req.VerifyHeader.MarshaledSize()

	reqLen := calculateSearchRequestLength(bodyLen, metaHdrLen, verifHdrLen)

	// default gRPC buffer pool used to serialize generated message structures
	bufferPool := mem.DefaultBufferPool()
	poolItem := bufferPool.Get(reqLen)
	defer bufferPool.Put(poolItem)
	buf := *poolItem

	gotLen := writeSearchRequestFields(buf, bodyLen, req.Body, metaHdrLen, req.MetaHeader, verifHdrLen, req.VerifyHeader)
	if gotLen != reqLen {
		return nil, newWrongRequestLengthError(reqLen, gotLen)
	}

	reqBuf := mem.SliceBuffer(buf)

	for _, nodeSet := range nodeSets {
		for _, nodeIdx := range islices.ShuffleIndexes(len(nodeSet)) {
			node, err := s.nodeClients.Get(ctx, nodeSet[nodeIdx])
			if err != nil {
				s.log.Debug("get conn to remote node",
					inetmap.ZapEndpoints(nodeSet[nodeIdx]), zap.Error(err))
				continue
			}

			var respBuf mem.BufferSlice

			err = node.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
				var err error
				respBuf, err = callUnary(ctx, conn, protoobject.ObjectService_SearchV2_FullMethodName, reqBuf)
				if err != nil {
					if igrpc.IsUnavailable(err) {
						return clientcore.ErrSkipConnection
					}
					return err
				}

				return nil
			})
			if err == nil {
				return respBuf, nil
			}
			if !errors.Is(err, clientcore.ErrAllConnectionsSkipped) {
				return nil, err
			}

			s.log.Info("remote node is unavailable", zap.String("op", "SEARCH"), inetmap.ZapEndpoints(nodeSet[nodeIdx]))
		}
	}

	return nil, nil
}

func calculateSearchRequestLength(bodyLen int, metaHdrLen int, verifHdrLen int) int {
	reqLen := 1 + protowire.SizeBytes(bodyLen)     // 1 for protobuf.TagBytes1
	reqLen += 1 + protowire.SizeBytes(metaHdrLen)  // 1 for iprotobuf.TagBytes2
	reqLen += 1 + protowire.SizeBytes(verifHdrLen) // 1 for iprotobuf.TagBytes3
	return reqLen
}

func writeSearchRequestFields(buf []byte, bodyLen int, body *protoobject.SearchV2Request_Body, metaHdrLen int, metaHdr *protosession.RequestMetaHeader, verifHdrLen int, verifHdr *protosession.RequestVerificationHeader) int {
	off := writeStablyMarshalledField(buf, protobuf.TagBytes1, bodyLen, body)
	off += writeStablyMarshalledField(buf[off:], protobuf.TagBytes2, metaHdrLen, metaHdr)
	off += writeStablyMarshalledField(buf[off:], protobuf.TagBytes3, verifHdrLen, verifHdr)
	return off
}

func (s *Server) writeLocalSearchObjectsRequest(buf []byte, bodyLen int, body *protoobject.SearchV2Request_Body, metaHdrLen int, verifHdrSigCount int) (int, error) {
	var originSig []byte
	var err error
	if verifHdrSigCount == oldRequestVerificationSignatureCount {
		originSig, err = neofsecdsa.Signer(s.signer).Sign(nil)
		if err != nil {
			return 0, fmt.Errorf("sign empty data: %w", err)
		}
	}

	// body
	buf[0] = protobuf.TagBytes1
	off := 1 + binary.PutUvarint(buf[1:], uint64(bodyLen))

	signedDataFrom := off

	body.MarshalStable(buf[off:])
	off += bodyLen

	bodySig, err := signECDSAWithSHA512(s.signer, buf[signedDataFrom:off])
	if err != nil {
		return 0, fmt.Errorf("sign body: %w", err)
	}

	// meta header
	buf[off] = protobuf.TagBytes2
	off++
	off += binary.PutUvarint(buf[off:], uint64(metaHdrLen))

	signedDataFrom = off

	off += copy(buf[off:], currentVersionResponseMetaHeader)
	buf[off] = protobuf.TagVarint3 // TTL
	off++
	buf[off] = 1
	off++

	metaHdrSig, err := signECDSAWithSHA512(s.signer, buf[signedDataFrom:off])
	if err != nil {
		return 0, fmt.Errorf("sign meta header: %w", err)
	}

	// verification header
	off += writeRequestVerificationHeader(buf[off:], verifHdrSigCount, s.pubKeyBytes, bodySig, metaHdrSig, originSig)

	return off, nil
}
