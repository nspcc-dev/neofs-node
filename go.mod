module github.com/nspcc-dev/neofs-node

go 1.14

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/klauspost/compress v1.11.3
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mr-tron/base58 v1.1.3
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/nspcc-dev/hrw v1.0.9
	github.com/nspcc-dev/neo-go v0.95.1
	github.com/nspcc-dev/neofs-api-go v1.27.1
	github.com/nspcc-dev/neofs-sdk-go v0.0.0-20210520210714-9dee13f0d556
	github.com/nspcc-dev/tzhash v1.4.0
	github.com/panjf2000/ants/v2 v2.3.0
	github.com/paulmach/orb v0.2.1
	github.com/prometheus/client_golang v1.6.0
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.6.1
	go.etcd.io/bbolt v1.3.5
	go.uber.org/atomic v1.5.1
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20200117160349-530e935923ad // indirect
	golang.org/x/lint v0.0.0-20191125180803-fdd1cda4f05f // indirect
	golang.org/x/net v0.0.0-20191105084925-a882066a44e0 // indirect
	golang.org/x/tools v0.0.0-20200123022218-593de606220b // indirect
	google.golang.org/grpc v1.29.1
	google.golang.org/protobuf v1.25.0
)

// Used for debug reasons
// replace github.com/nspcc-dev/neofs-api-go => ../neofs-api-go
