package shard

import (
	"github.com/mr-tron/base58"
	"go.uber.org/zap"
)

// ID represents Shard identifier.
//
// Each shard should have the unique ID within
// a single instance of local storage.
type ID []byte

// NewIDFromBytes constructs ID from byte slice.
func NewIDFromBytes(v []byte) *ID {
	return (*ID)(&v)
}

func (id ID) String() string {
	return base58.Encode(id)
}

// ID returns Shard identifier.
func (s *Shard) ID() *ID {
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
		s.info.ID = NewIDFromBytes(id)

		if s.cfg.metricsWriter != nil {
			s.cfg.metricsWriter.SetShardID(s.info.ID.String())
		}
	}

	s.log = s.log.With(zap.String("shard_id", s.info.ID.String()))
	s.metaBase.SetLogger(s.log)
	s.blobStor.SetLogger(s.log)
	if s.hasWriteCache() {
		s.writeCache.SetLogger(s.log)
		s.writeCache.SetShardIDMetrics(s.info.ID.String())
	}

	if len(id) != 0 {
		return nil
	}
	return s.metaBase.WriteShardID(*s.info.ID)
}
