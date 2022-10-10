package control

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"

func getShardIDList(raw [][]byte) []*shard.ID {
	res := make([]*shard.ID, 0, len(raw))
	for i := range raw {
		res = append(res, shard.NewIDFromBytes(raw[i]))
	}
	return res
}
