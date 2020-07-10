package object

import (
	"context"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/stretchr/testify/require"
)

func TestEpochResponsePreparer(t *testing.T) {
	epoch := uint64(33)

	s := &epochResponsePreparer{
		epochRecv: &testPutEntity{res: epoch},
	}

	ctx := context.TODO()

	t.Run("get", func(t *testing.T) {
		t.Run("head", func(t *testing.T) {
			obj := &Object{
				SystemHeader: SystemHeader{
					ID:  testObjectAddress(t).ObjectID,
					CID: testObjectAddress(t).CID,
				},
			}

			resp := makeGetHeaderResponse(obj)

			require.NoError(t, s.prepareResponse(ctx, new(object.GetRequest), resp))

			require.Equal(t, obj, resp.GetObject())
			require.Equal(t, epoch, resp.GetEpoch())
		})

		t.Run("chunk", func(t *testing.T) {
			chunk := testData(t, 10)

			resp := makeGetChunkResponse(chunk)

			require.NoError(t, s.prepareResponse(ctx, new(object.GetRequest), resp))

			require.Equal(t, chunk, resp.GetChunk())
			require.Equal(t, epoch, resp.GetEpoch())
		})
	})

	t.Run("put", func(t *testing.T) {
		addr := testObjectAddress(t)

		resp := makePutResponse(addr)
		require.NoError(t, s.prepareResponse(ctx, new(object.PutRequest), resp))

		require.Equal(t, addr, resp.GetAddress())
		require.Equal(t, epoch, resp.GetEpoch())
	})

	t.Run("head", func(t *testing.T) {
		obj := &Object{
			SystemHeader: SystemHeader{
				PayloadLength: 7,
				ID:            testObjectAddress(t).ObjectID,
				CID:           testObjectAddress(t).CID,
			},
		}

		resp := makeHeadResponse(obj)
		require.NoError(t, s.prepareResponse(ctx, new(object.HeadRequest), resp))

		require.Equal(t, obj, resp.GetObject())
		require.Equal(t, epoch, resp.GetEpoch())
	})

	t.Run("search", func(t *testing.T) {
		addrList := testAddrList(t, 5)

		resp := makeSearchResponse(addrList)
		require.NoError(t, s.prepareResponse(ctx, new(object.SearchRequest), resp))

		require.Equal(t, addrList, resp.GetAddresses())
		require.Equal(t, epoch, resp.GetEpoch())
	})

	t.Run("range", func(t *testing.T) {
		data := testData(t, 10)

		resp := makeRangeResponse(data)
		require.NoError(t, s.prepareResponse(ctx, new(GetRangeRequest), resp))

		require.Equal(t, data, resp.GetFragment())
		require.Equal(t, epoch, resp.GetEpoch())
	})

	t.Run("range hash", func(t *testing.T) {
		hashes := []Hash{
			hash.Sum(testData(t, 10)),
			hash.Sum(testData(t, 10)),
		}

		resp := makeRangeHashResponse(hashes)
		require.NoError(t, s.prepareResponse(ctx, new(object.GetRangeHashRequest), resp))

		require.Equal(t, hashes, resp.Hashes)
		require.Equal(t, epoch, resp.GetEpoch())
	})

	t.Run("delete", func(t *testing.T) {
		resp := makeDeleteResponse()
		require.NoError(t, s.prepareResponse(ctx, new(object.DeleteRequest), resp))

		require.IsType(t, new(object.DeleteResponse), resp)
		require.Equal(t, epoch, resp.GetEpoch())
	})
}
