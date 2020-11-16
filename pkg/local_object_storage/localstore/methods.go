package localstore

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func addressBytes(a *objectSDK.Address) ([]byte, error) {
	return a.ToV2().StableMarshal(nil)
}

func objectBytes(o *object.Object) ([]byte, error) {
	return o.ToV2().StableMarshal(nil)
}

func (s *Storage) Put(obj *object.Object) error {
	addrBytes, err := addressBytes(obj.Address())
	if err != nil {
		return errors.Wrap(err, "could not marshal object address")
	}

	objBytes, err := objectBytes(obj)
	if err != nil {
		return errors.Wrap(err, "could not marshal the object")
	}

	if err := s.blobBucket.Set(addrBytes, objBytes); err != nil {
		return errors.Wrap(err, "could no save object in BLOB storage")
	}

	if err := s.metaBase.Put(object.NewRawFromObject(obj).CutPayload().Object()); err != nil {
		return errors.Wrap(err, "could not save object in meta storage")
	}

	return nil
}

func (s *Storage) Delete(addr *objectSDK.Address) error {
	addrBytes, err := addressBytes(addr)
	if err != nil {
		return errors.Wrap(err, "could not marshal object address")
	}

	if err := s.blobBucket.Del(addrBytes); err != nil {
		s.log.Warn("could not remove object from BLOB storage",
			zap.Error(err),
		)
	}

	if err := s.metaBase.Delete(addr); err != nil {
		return errors.Wrap(err, "could not remove object from meta storage")
	}

	return nil
}

func (s *Storage) Get(addr *objectSDK.Address) (*object.Object, error) {
	addrBytes, err := addressBytes(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal object address")
	}

	objBytes, err := s.blobBucket.Get(addrBytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not get object from BLOB storage")
	}

	obj := object.New()

	return obj, obj.Unmarshal(objBytes)
}

func (s *Storage) Head(addr *objectSDK.Address) (*object.Object, error) {
	return s.metaBase.Get(addr)
}

func (s *Storage) Select(fs objectSDK.SearchFilters) ([]*objectSDK.Address, error) {
	return s.metaBase.Select(fs)
}
