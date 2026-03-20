package shard

import (
	"errors"
	"fmt"
	"os"

	coreshard "github.com/nspcc-dev/neofs-node/pkg/core/shard"
	"go.uber.org/zap"
)

// ID returns Shard identifier.
func (s *Shard) ID() *coreshard.ID {
	return s.info.ID
}

// ResolveID resolves shard ID.
func (s *Shard) ResolveID() error {
	if s.blobStor == nil {
		return fmt.Errorf("blobstor is not configured")
	}

	resolvedID, generated, err := s.blobStor.ResolveShardID()
	if err != nil {
		return err
	}
	if resolvedID == nil {
		return fmt.Errorf("empty shard ID from blobstor resolver")
	}

	id := resolvedID
	if generated {
		if err = s.metaBase.Open(false); err == nil {
			// metabase can already have a persisted shard ID for this shard, use it
			// as soon as it is known to keep other components consistent
			metaID, err := s.metaBase.ReadShardID()
			closeErr := s.metaBase.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if len(metaID) != 0 {
				id = coreshard.NewFromBytes(metaID)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	s.info.ID = id
	idStr := id.String()
	s.log = s.log.With(zap.String("shard_id", idStr))
	s.gcCfg.log = s.gcCfg.log.With(zap.String("shard_id", idStr))
	s.metaBase.SetLogger(s.log)
	s.blobStor.SetLogger(s.log)
	if s.metricsWriter != nil {
		s.metricsWriter.SetShardID(idStr)
	}
	return nil
}
