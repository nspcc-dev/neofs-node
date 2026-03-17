package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"go.uber.org/zap"
)

// ID returns Shard identifier.
func (s *Shard) ID() common.ID {
	return s.info.ID
}

// UpdateID reads shard ID saved in the metabase and updates it if it is missing.
func (s *Shard) UpdateID() (err error) {
	if err = s.metaBase.Open(false); err != nil {
		return err
	}
	defer func() {
		cErr := s.metaBase.Close()
		if err == nil {
			err = cErr
		}
	}()
	id, err := s.metaBase.ReadShardID()
	if err != nil {
		return err
	}
	if len(id) != 0 {
		s.info.ID, err = common.NewIDFromBytes(id)
		if err != nil {
			return err
		}

		if s.metricsWriter != nil {
			s.metricsWriter.SetShardID(s.info.ID.String())
		}
	} else {
		blobShardID := s.blobStor.ShardID()
		if !blobShardID.IsZero() {
			s.info.ID = blobShardID

			if s.metricsWriter != nil {
				s.metricsWriter.SetShardID(s.info.ID.String())
			}
		}
	}

	var (
		sID = s.info.ID.String()
		l   = s.log.With(zap.String("shard_id", sID))
	)
	s.log = l
	s.gcCfg.log = s.gcCfg.log.With(zap.String("shard_id", sID))
	s.metaBase.SetLogger(l)
	s.blobStor.SetLogger(l)
	s.blobStor.SetShardID(s.info.ID)
	if s.hasWriteCache() {
		s.writeCache.SetLogger(l)
		s.writeCache.SetShardIDMetrics(sID)
	}

	if len(id) != 0 {
		return nil
	}
	return s.metaBase.WriteShardID(s.info.ID.Bytes())
}
