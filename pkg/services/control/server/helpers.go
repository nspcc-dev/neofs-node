package control

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"

func (s *Server) getShardIDList(raw [][]byte) []*shard.ID {
	if len(raw) != 0 {
		res := make([]*shard.ID, 0, len(raw))
		for i := range raw {
			res = append(res, shard.NewIDFromBytes(raw[i]))
		}
		return res
	}

	info := s.s.DumpInfo()
	res := make([]*shard.ID, 0, len(info.Shards))
	for i := range info.Shards {
		res = append(res, info.Shards[i].ID)
	}
	return res
}
