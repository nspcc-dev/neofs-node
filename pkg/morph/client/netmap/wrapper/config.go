package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

const maxObjectSizeConfig = "MaxObjectSize"

// MaxObjectSize receives max object size configuration
// value through the Netmap contract call.
func (w *Wrapper) MaxObjectSize() (uint64, error) {
	args := netmap.ConfigArgs{}
	args.SetKey([]byte(maxObjectSizeConfig))

	vals, err := w.client.Config(args, netmap.IntegerAssert)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get epoch number", w)
	}

	v := vals.Value()

	sz, ok := v.(int64)
	if !ok {
		return 0, errors.Errorf("(%T) invalid epoch value type %T", w, v)
	}

	return uint64(sz), nil
}
