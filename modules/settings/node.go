package settings

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	nodeSettings struct {
		dig.Out

		Address     multiaddr.Multiaddr
		PrivateKey  *ecdsa.PrivateKey
		NodeOpts    []string      `name:"node_options"`
		ShutdownTTL time.Duration `name:"shutdown_ttl"`

		NodeInfo bootstrap.NodeInfo
	}
)

const generateKey = "generated"

var errEmptyNodeSettings = errors.New("node settings could not be empty")

func newNodeSettings(v *viper.Viper, l *zap.Logger) (cfg nodeSettings, err error) {
	// check, that we have node settings in provided config
	if !v.IsSet("node") {
		err = errEmptyNodeSettings
		return
	}

	// try to load and setup ecdsa.PrivateKey
	key := v.GetString("node.private_key")
	switch key {
	case "":
		err = crypto.ErrEmptyPrivateKey
		return cfg, err
	case generateKey:
		if cfg.PrivateKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			return cfg, err
		}
	default:
		if cfg.PrivateKey, err = crypto.LoadPrivateKey(key); err != nil {
			return cfg, errors.Wrap(err, "cannot unmarshal private key")
		}
	}

	id := peers.IDFromPublicKey(&cfg.PrivateKey.PublicKey)
	pub := crypto.MarshalPublicKey(&cfg.PrivateKey.PublicKey)
	l.Debug("private key loaded successful",
		zap.String("file", v.GetString("node.private_key")),
		zap.Binary("public", pub),
		zap.Stringer("node-id", id))

	var (
		addr  string
		proto string
	)

	// fetch shutdown timeout from settings
	if cfg.ShutdownTTL = v.GetDuration("node.shutdown_ttl"); cfg.ShutdownTTL == 0 {
		return cfg, errEmptyShutdownTTL
	}

	// fetch address and protocol from settings
	if addr = v.GetString("node.address"); addr == "" {
		return cfg, errors.Wrapf(errEmptyAddress, "given '%s'", addr)
	} else if addr, err := prepareAddress(addr); err != nil {
		return cfg, err
	} else if proto = v.GetString("node.proto"); proto == "" {
		return cfg, errors.Wrapf(errEmptyProtocol, "given '%s'", proto)
	} else if cfg.Address, err = multiAddressFromProtoAddress(proto, addr); err != nil {
		return cfg, errors.Wrapf(err, "given '%s' '%s'", proto, addr)
	}

	// add well-known options
	items := map[string]string{
		"Capacity": "capacity",
		"Price":    "price",
		"Location": "location",
		"Country":  "country",
		"City":     "city",
	}

	// TODO: use const namings
	prefix := "node."

	for opt, path := range items {
		val := v.GetString(prefix + path)
		if len(val) == 0 {
			err = errors.Errorf("node option %s must be set explicitly", opt)
			return
		}

		cfg.NodeOpts = append(cfg.NodeOpts,
			fmt.Sprintf("/%s:%s",
				opt,
				val,
			),
		)
	}

	// add other options

	var (
		i   int
		val string
	)
loop:
	for ; ; i++ {
		val = v.GetString("node.options." + strconv.Itoa(i))
		if val == "" {
			break
		}

		for opt := range items {
			if strings.Contains(val, "/"+opt) {
				continue loop
			}
		}

		cfg.NodeOpts = append(cfg.NodeOpts, val)
	}

	cfg.NodeInfo = bootstrap.NodeInfo{
		Address: cfg.Address.String(),
		PubKey:  crypto.MarshalPublicKey(&cfg.PrivateKey.PublicKey),
		Options: cfg.NodeOpts,
	}

	l.Debug("loaded node options",
		zap.Strings("options", cfg.NodeOpts))

	return cfg, err
}
