package protobuf_test

import (
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestMessageLengths(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  interface{ MarshaledSize() int }
		cnst int
	}{
		{name: "object ID", msg: oidtest.ID().ProtoMessage(), cnst: iprotobuf.ObjectIDLength},
		{name: "container ID", msg: cidtest.ID().ProtoMessage(), cnst: iprotobuf.ContainerIDLength},
		{name: "object address", msg: oidtest.Address().ProtoMessage(), cnst: iprotobuf.ObjectAddressLength},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.msg.MarshaledSize(), tc.cnst)
		})
	}
}
