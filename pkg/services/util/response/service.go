package response

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

// Service represents universal v2 service
// that sets response meta header values.
type Service struct {
	cfg *cfg
}

// Option is an option of Service constructor.
type Option func(*cfg)

type cfg struct {
	version *refs.Version

	state netmap.State
}

func defaultCfg() *cfg {
	return &cfg{
		version: pkg.SDKVersion().ToV2(),
	}
}

// NewService creates, initializes and returns Service instance.
func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

func setMeta(resp util.ResponseMessage, cfg *cfg) {
	meta := new(session.ResponseMetaHeader)
	meta.SetVersion(cfg.version)
	meta.SetTTL(1) // FIXME: TTL must be calculated
	meta.SetEpoch(cfg.state.CurrentEpoch())

	if origin := resp.GetMetaHeader(); origin != nil {
		// FIXME: what if origin is set by local server?
		meta.SetOrigin(origin)
	}

	resp.SetMetaHeader(meta)
}

// WithNetworkState returns option to set network state of Service.
func WithNetworkState(v netmap.State) Option {
	return func(c *cfg) {
		c.state = v
	}
}
