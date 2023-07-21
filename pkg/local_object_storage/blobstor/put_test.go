package blobstor_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/stretchr/testify/require"
)

type mockWriter struct {
	common.Storage
	full bool
}

func (x *mockWriter) Type() string {
	if x.full {
		return "full"
	}
	return "free"
}

func (x *mockWriter) Put(common.PutPrm) (common.PutRes, error) {
	if x.full {
		return common.PutRes{}, common.ErrNoSpace
	}
	return common.PutRes{}, nil
}

func (x *mockWriter) SetCompressor(*compression.Config) {}

func TestBlobStor_Put_Overflow(t *testing.T) {
	bs := blobstor.New(blobstor.WithStorages(
		[]blobstor.SubStorage{
			{Storage: &mockWriter{full: true}},
			{Storage: &mockWriter{full: false}},
		},
	))

	_, err := bs.Put(common.PutPrm{})
	require.NoError(t, err)
}
