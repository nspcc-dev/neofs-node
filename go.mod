module github.com/nspcc-dev/neofs-node

go 1.16

require (
	github.com/google/go-github/v39 v39.2.0
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/klauspost/compress v1.13.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multiaddr v0.4.0
	github.com/nspcc-dev/hrw v1.0.9
	github.com/nspcc-dev/neo-go v0.97.3
	github.com/nspcc-dev/neofs-api-go v1.30.0
	github.com/nspcc-dev/neofs-sdk-go v0.0.0-20211110152919-10b78e74afbd
	github.com/nspcc-dev/tzhash v1.4.0
	github.com/panjf2000/ants/v2 v2.4.0
	github.com/paulmach/orb v0.2.2
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	go.etcd.io/bbolt v1.3.6
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.18.1
	golang.org/x/term v0.0.0-20210429154555-c04ba851c2a4
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)

// Used for debug reasons
// replace github.com/nspcc-dev/neofs-api-go => ../neofs-api-go
