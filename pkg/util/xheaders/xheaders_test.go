package xheaders

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	containertest "github.com/nspcc-dev/neofs-sdk-go/container/test"
	"github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
)

type cnrTestSrc struct {
	cnr container.Container
}

func (c cnrTestSrc) Get(id cid.ID) (container.Container, error) {
	return c.cnr, nil
}

func TestCheckRequestContainerRevision(t *testing.T) {
	containerWithRevision := func(rev uint64) container.Container {
		cnr := containertest.Container()
		cnrP := cnr.ProtoMessage()

		// test package generates REPS that cannot be parsed
		{
			for _, rep := range cnrP.PlacementPolicy.Replicas {
				rep.Count = 7 // smth that is lower that 8
				rep.Selector = ""
			}
		}

		cnrP.Revision = rev
		require.NoError(t, cnr.FromProtoMessage(cnrP))

		return cnr
	}
	metaHeaderWithRevision := func(revStr string) *session.RequestMetaHeader {
		return &session.RequestMetaHeader{
			XHeaders: []*session.XHeader{
				{
					Key:   client.XHeaderContainerRevision,
					Value: revStr,
				},
			},
		}
	}

	t.Run("nil header", func(t *testing.T) {
		err := CheckRequestContainerRevision(nil, cid.ID{}, cnrTestSrc{container.Container{}})
		require.NoError(t, err)
	})

	t.Run("no X-Headers", func(t *testing.T) {
		err := CheckRequestContainerRevision(&session.RequestMetaHeader{}, cid.ID{}, cnrTestSrc{container.Container{}})
		require.NoError(t, err)
	})

	t.Run("non-empty X-Headers but no revision", func(t *testing.T) {
		mh := &session.RequestMetaHeader{
			XHeaders: []*session.XHeader{
				{
					Key:   "an x-header",
					Value: "but wrong",
				},
			},
		}

		err := CheckRequestContainerRevision(mh, cid.ID{}, cnrTestSrc{container.Container{}})
		require.NoError(t, err)
	})

	t.Run("incorrect X-Header", func(t *testing.T) {
		const incorrectXHeader = "bla"
		mh := metaHeaderWithRevision(incorrectXHeader)

		err := CheckRequestContainerRevision(mh, cid.ID{}, cnrTestSrc{container.Container{}})
		require.ErrorContains(t, err, fmt.Sprintf("parsing container revision from '%s' X-Header:", incorrectXHeader))
	})

	t.Run("revisions do not match", func(t *testing.T) {
		var (
			mh  = metaHeaderWithRevision("123")
			cnr = containerWithRevision(321)
		)

		err := CheckRequestContainerRevision(mh, cid.ID{}, cnrTestSrc{cnr})
		require.ErrorContains(t, err, "code = 3076")
		require.ErrorContains(t, err, "container revision does not match: requested: 123, server's: 321")
		require.ErrorIs(t, err, apistatus.ErrContainerRevisionMismatch)
	})

	t.Run("happy path", func(t *testing.T) {
		var (
			mh  = metaHeaderWithRevision("456")
			cnr = containerWithRevision(456)
		)

		err := CheckRequestContainerRevision(mh, cid.ID{}, cnrTestSrc{cnr})
		require.NoError(t, err)
	})
}
