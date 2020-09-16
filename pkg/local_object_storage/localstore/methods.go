package localstore

import (
	"context"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (s *Storage) Put(obj *object.Object) error {
	addrBytes, err := obj.Address().ToV2().StableMarshal(nil)
	if err != nil {
		return errors.Wrap(err, "could not marshal object address")
	}

	objBytes, err := obj.MarshalStableV2()
	if err != nil {
		return errors.Wrap(err, "could not marshal the object")
	}

	metaBytes, err := metaToBytes(metaFromObject(obj))
	if err != nil {
		return errors.Wrap(err, "could not marshal object meta")
	}

	if err := s.blobBucket.Set(addrBytes, objBytes); err != nil {
		return errors.Wrap(err, "could no save object in BLOB storage")
	}

	if err := s.metaBucket.Set(addrBytes, metaBytes); err != nil {
		return errors.Wrap(err, "could not save object in meta storage")
	}

	return nil
}

func (s *Storage) Delete(addr *objectSDK.Address) error {
	addrBytes, err := addr.ToV2().StableMarshal(nil)
	if err != nil {
		return errors.Wrap(err, "could not marshal object address")
	}

	if err := s.blobBucket.Del(addrBytes); err != nil {
		s.log.Warn("could not remove object from BLOB storage",
			zap.Error(err),
		)
	}

	if err := s.metaBucket.Del(addrBytes); err != nil {
		return errors.Wrap(err, "could not remove object from meta storage")
	}

	return nil
}

func (s *Storage) Get(addr *objectSDK.Address) (*object.Object, error) {
	addrBytes, err := addr.ToV2().StableMarshal(nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal object address")
	}

	objBytes, err := s.blobBucket.Get(addrBytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not get object from BLOB storage")
	}

	return object.FromBytes(objBytes)
}

func (s *Storage) Head(addr *objectSDK.Address) (*ObjectMeta, error) {
	addrBytes, err := addr.ToV2().StableMarshal(nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal object address")
	}

	metaBytes, err := s.metaBucket.Get(addrBytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not get object from meta storage")
	}

	return metaFromBytes(metaBytes)
}

func (s *Storage) Iterate(filter FilterPipeline, handler func(*ObjectMeta) bool) error {
	if filter == nil {
		filter = NewFilter(&FilterParams{
			Name: "SKIPPING_FILTER",
			FilterFunc: func(context.Context, *ObjectMeta) *FilterResult {
				return ResultPass()
			},
		})
	}

	return s.metaBucket.Iterate(func(_, v []byte) bool {
		meta, err := metaFromBytes(v)
		if err != nil {
			s.log.Error("unmarshal meta bucket item failure",
				zap.Error(err),
			)
		} else if filter.Pass(context.TODO(), meta).Code() == CodePass {
			return !handler(meta)
		}

		return true
	})
}
