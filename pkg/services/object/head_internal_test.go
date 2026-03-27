package object

import (
	"bytes"
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func BenchmarkHandleHeadResponse(b *testing.B) {
	b.Run("404", func(b *testing.B) {
		metaHdr := newBlankMetaHeader()
		metaHdr.Status = &protostatus.Status{
			Code:    2049,
			Message: "object not found",
			Details: []*protostatus.Status_Detail{
				{Id: 1869765515, Value: []byte("foo")},
				{Id: 2095463591, Value: []byte("bar")},
			},
		}

		bench := func(b *testing.B, metaHdr *protosession.ResponseMetaHeader) {
			respBuf := messageToSingleMemBuffer(b, &protoobject.HeadResponse{
				MetaHeader:   metaHdr,
				VerifyHeader: newAnyVerificationHeader(),
			})
			b.ReportAllocs() // FIXME: drop
			for b.Loop() {
				_, err := handleHeadResponse(respBuf, oid.ID{})
				require.ErrorIs(b, err, apistatus.ErrObjectNotFound)
				// Originally it shows 2 allocs. Inspection showed that one is return
				// apistatus.ErrObjectNotFound, the other require.ErrorIs.
			}
		}

		b.Run("root", func(b *testing.B) {
			bench(b, metaHdr)
		})

		b.Run("nestedX5", func(b *testing.B) {
			bench(b, nestMetaHeader(metaHdr, 5))
		})
	})

	b.Run("non-404 failure", func(b *testing.B) {
		metaHdr := newBlankMetaHeader()
		metaHdr.Status = &protostatus.Status{
			Code:    2052, // already removed
			Message: "object already removed",
			Details: []*protostatus.Status_Detail{
				{Id: 1869765515, Value: []byte("foo")},
				{Id: 2095463591, Value: []byte("bar")},
			},
		}

		bench := func(b *testing.B, metaHdr *protosession.ResponseMetaHeader) {
			respBuf := messageToSingleMemBuffer(b, &protoobject.HeadResponse{
				MetaHeader:   metaHdr,
				VerifyHeader: newAnyVerificationHeader(),
			})
			b.ReportAllocs() // FIXME: drop
			for b.Loop() {
				hdr, err := handleHeadResponse(respBuf, oid.ID{})
				require.NoError(b, err)
				require.Nil(b, hdr)
				// Originally it shows 2 allocs. Inspection showed that one is return
				// apistatus.ErrObjectNotFound, the other require.ErrorIs.
			}
		}

		b.Run("root", func(b *testing.B) {
			bench(b, metaHdr)
		})

		b.Run("nestedX5", func(b *testing.B) {
			bench(b, nestMetaHeader(metaHdr, 5))
		})
	})

	b.Run("body", func(b *testing.B) {
		b.Run("header", func(b *testing.B) {
			obj := newTestObject(b)

			objMsg := obj.ProtoMessage()

			hdr, err := proto.Marshal(objMsg.Header)
			require.NoError(b, err)

			respBuf := messageToSingleMemBuffer(b, &protoobject.HeadResponse{
				Body: &protoobject.HeadResponse_Body{Head: &protoobject.HeadResponse_Body_Header{
					Header: &protoobject.HeaderWithSignature{
						Header:    objMsg.Header,
						Signature: objMsg.Signature,
					},
				}},
				MetaHeader:   newBlankMetaHeader(),
				VerifyHeader: newAnyVerificationHeader(),
			})

			id := obj.GetID()

			b.ReportAllocs() // FIXME: drop

			for b.Loop() {
				gotHdr, err := handleHeadResponse(respBuf, id)
				require.NoError(b, err)
				require.True(b, bytes.Equal(hdr, gotHdr))
			}
		})

		b.Run("split info", func(b *testing.B) {
			respBuf := messageToSingleMemBuffer(b, &protoobject.HeadResponse{
				Body: &protoobject.HeadResponse_Body{Head: &protoobject.HeadResponse_Body_SplitInfo{
					SplitInfo: newTestSplitInfo(),
				}},
				MetaHeader:   newBlankMetaHeader(),
				VerifyHeader: newAnyVerificationHeader(),
			})
			b.ReportAllocs() // FIXME: drop
			for b.Loop() {
				hdr, err := handleHeadResponse(respBuf, oid.ID{})
				require.NoError(b, err)
				require.Nil(b, hdr)
			}
		})
	})
}
