package control

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
)

// Server is an entity that serves
// Control service on storage node.
type Server struct {
	*cfg
}

// HealthChecker is component interface for calculating
// the current health status of a node.
type HealthChecker interface {
	// Must calculate and return current status of the node in NeoFS network map.
	//
	// If status can not be calculated for any reason,
	// control.netmapStatus_STATUS_UNDEFINED should be returned.
	NetmapStatus() control.NetmapStatus

	// Must calculate and return current health status of the node application.
	//
	// If status can not be calculated for any reason,
	// control.HealthStatus_HEALTH_STATUS_UNDEFINED should be returned.
	HealthStatus() control.HealthStatus
}

// NodeState is an interface of storage node network state.
type NodeState interface {
	SetNetmapStatus(control.NetmapStatus) error
}

// Option of the Server's constructor.
type Option func(*cfg)

type cfg struct {
	key *ecdsa.PrivateKey

	allowedKeys [][]byte

	healthChecker HealthChecker

	netMapSrc netmap.Source

	nodeState NodeState

	delObjHandler DeletedObjectHandler
}

func defaultCfg() *cfg {
	return &cfg{}
}

// New creates, initializes and returns new Server instance.
func New(opts ...Option) *Server {
	c := defaultCfg()

	for _, opt := range opts {
		opt(c)
	}

	return &Server{
		cfg: c,
	}
}

// WithKey returns option to set private key
// used for signing responses.
func WithKey(key *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.key = key
	}
}

// WithAuthorizedKeys returns option to add list of public
// keys that have rights to use Control service.
func WithAuthorizedKeys(keys [][]byte) Option {
	return func(c *cfg) {
		c.allowedKeys = append(c.allowedKeys, keys...)
	}
}

// WithHealthChecker returns option to set component
// to calculate node health status.
func WithHealthChecker(hc HealthChecker) Option {
	return func(c *cfg) {
		c.healthChecker = hc
	}
}

// WithNetMapSource returns option to set network map storage.
func WithNetMapSource(netMapSrc netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = netMapSrc
	}
}

// WithNodeState returns option to set node network state component.
func WithNodeState(state NodeState) Option {
	return func(c *cfg) {
		c.nodeState = state
	}
}

// WithDeletedObjectHandler returns option to function
// which is called on the objects being deleted.
func WithDeletedObjectHandler(h DeletedObjectHandler) Option {
	return func(c *cfg) {
		c.delObjHandler = h
	}
}
