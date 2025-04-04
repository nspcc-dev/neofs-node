package control_test

import (
	"bytes"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

func TestHealthCheckResponse_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateHealthCheckResponseBody(),
		new(control.HealthCheckResponse_Body),
		func(m1, m2 protoMessage) bool {
			return equalHealthCheckResponseBodies(
				m1.(*control.HealthCheckResponse_Body),
				m2.(*control.HealthCheckResponse_Body),
			)
		},
	)
}

func generateHealthCheckResponseBody() *control.HealthCheckResponse_Body {
	body := new(control.HealthCheckResponse_Body)
	body.SetNetmapStatus(control.NetmapStatus_ONLINE)
	body.SetHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	return body
}

func equalHealthCheckResponseBodies(b1, b2 *control.HealthCheckResponse_Body) bool {
	return b1.GetNetmapStatus() == b2.GetNetmapStatus() &&
		b1.GetHealthStatus() == b2.GetHealthStatus()
}

func TestSetNetmapStatusRequest_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateSetNetmapStatusRequestBody(),
		new(control.SetNetmapStatusRequest_Body),
		func(m1, m2 protoMessage) bool {
			return equalSetnetmapStatusRequestBodies(
				m1.(*control.SetNetmapStatusRequest_Body),
				m2.(*control.SetNetmapStatusRequest_Body),
			)
		},
	)
}

func generateSetNetmapStatusRequestBody() *control.SetNetmapStatusRequest_Body {
	body := new(control.SetNetmapStatusRequest_Body)
	body.SetStatus(control.NetmapStatus_ONLINE)

	return body
}

func equalSetnetmapStatusRequestBodies(b1, b2 *control.SetNetmapStatusRequest_Body) bool {
	return b1.GetStatus() == b2.GetStatus()
}

func TestListShardsResponse_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateListShardsResponseBody(),
		new(control.ListShardsResponse_Body),
		func(m1, m2 protoMessage) bool {
			return equalListShardResponseBodies(
				m1.(*control.ListShardsResponse_Body),
				m2.(*control.ListShardsResponse_Body),
			)
		},
	)
}

func equalListShardResponseBodies(b1, b2 *control.ListShardsResponse_Body) bool {
	if len(b1.Shards) != len(b2.Shards) {
		return false
	}

	for i := range b1.Shards {
		info1 := b1.Shards[i].GetBlobstor()
		info2 := b2.Shards[i].GetBlobstor()

		if b1.Shards[i].GetMetabasePath() != b2.Shards[i].GetMetabasePath() ||
			b1.Shards[i].GetWritecachePath() != b2.Shards[i].GetWritecachePath() ||
			!bytes.Equal(b1.Shards[i].GetShard_ID(), b2.Shards[i].GetShard_ID()) ||
			!compareBlobstorInfo(info1, info2) {
			return false
		}
	}

	for i := range b1.Shards {
		for j := i + 1; j < len(b1.Shards); j++ {
			if b1.Shards[i].GetMetabasePath() == b2.Shards[j].GetMetabasePath() ||
				!compareBlobstorInfo(b1.Shards[i].Blobstor, b2.Shards[i].Blobstor) ||
				b1.Shards[i].GetWritecachePath() == b2.Shards[j].GetWritecachePath() ||
				bytes.Equal(b1.Shards[i].GetShard_ID(), b2.Shards[j].GetShard_ID()) {
				return false
			}
		}
	}

	return true
}

func compareBlobstorInfo(a, b []*control.BlobstorInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type ||
			a[i].Path != b[i].Path {
			return false
		}
	}
	return true
}

func generateListShardsResponseBody() *control.ListShardsResponse_Body {
	body := new(control.ListShardsResponse_Body)
	body.SetShards([]*control.ShardInfo{
		generateShardInfo(0),
		generateShardInfo(1),
	})

	return body
}

func TestSetShardModeRequest_Body_StableMarshal(t *testing.T) {
	testStableMarshal(t,
		generateSetShardModeRequestBody(),
		new(control.SetShardModeRequest_Body),
		func(m1, m2 protoMessage) bool {
			return equalSetShardModeRequestBodies(
				m1.(*control.SetShardModeRequest_Body),
				m2.(*control.SetShardModeRequest_Body),
			)
		},
	)
}

func generateSetShardModeRequestBody() *control.SetShardModeRequest_Body {
	body := new(control.SetShardModeRequest_Body)
	body.SetShardIDList([][]byte{{0, 1, 2, 3, 4}})
	body.SetMode(control.ShardMode_READ_WRITE)

	return body
}

func equalSetShardModeRequestBodies(b1, b2 *control.SetShardModeRequest_Body) bool {
	if b1.GetMode() != b2.GetMode() || len(b1.Shard_ID) != len(b2.Shard_ID) {
		return false
	}

	for i := range b1.Shard_ID {
		if !bytes.Equal(b1.Shard_ID[i], b2.Shard_ID[i]) {
			return false
		}
	}

	return true
}
