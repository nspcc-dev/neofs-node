package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

const (
	maxObjectSizeConfig   = "MaxObjectSize"
	basicIncomeRateConfig = "BasicIncomeRate"
)

// MaxObjectSize receives max object size configuration
// value through the Netmap contract call.
func (w *Wrapper) MaxObjectSize() (uint64, error) {
	objectSize, err := w.readUInt64Config(maxObjectSizeConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get epoch number", w)
	}

	return objectSize, nil
}

// BasicIncomeRate returns basic income rate configuration value from network
// config in netmap contract.
func (w *Wrapper) BasinIncomeRate() (uint64, error) {
	rate, err := w.readUInt64Config(basicIncomeRateConfig)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get basic income rate", w)
	}

	return rate, nil
}

func (w *Wrapper) readUInt64Config(key string) (uint64, error) {
	args := netmap.ConfigArgs{}
	args.SetKey([]byte(key))

	vals, err := w.client.Config(args, netmap.IntegerAssert)
	if err != nil {
		return 0, err
	}

	v := vals.Value()

	sz, ok := v.(int64)
	if !ok {
		return 0, errors.Errorf("(%T) invalid value type %T", w, v)
	}

	return uint64(sz), nil
}
