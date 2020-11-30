package shard

import (
	"github.com/pkg/errors"
)

// Open opens all Shard's components.
func (s *Shard) Open() error {
	for _, component := range []interface {
		Open() error
	}{
		s.blobStor,
		s.metaBase,
	} {
		if err := component.Open(); err != nil {
			return errors.Wrapf(err, "could not open %s", component)
		}
	}

	return nil
}

// Init initializes all Shard's components.
func (s *Shard) Init() error {
	for _, component := range []interface {
		Init() error
	}{
		s.blobStor,
		s.metaBase,
	} {
		if err := component.Init(); err != nil {
			return errors.Wrapf(err, "could not initialize %s", component)
		}
	}

	return nil
}

// Close releases all Shard's components.
func (s *Shard) Close() error {
	for _, component := range []interface {
		Close() error
	}{
		s.blobStor,
		s.metaBase,
	} {
		if err := component.Close(); err != nil {
			return errors.Wrapf(err, "could not close %s", component)
		}
	}

	return nil
}
