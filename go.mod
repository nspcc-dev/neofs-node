module github.com/nspcc-dev/neofs-node

go 1.23

require (
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cheggaaa/pb v1.0.29
	github.com/chzyer/readline v1.5.1
	github.com/flynn-archive/go-shlex v0.0.0-20150515145356-3f9db97f8568
	github.com/google/go-github/v39 v39.2.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/klauspost/compress v1.17.9
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multiaddr v0.12.2
	github.com/nspcc-dev/hrw/v2 v2.0.3
	github.com/nspcc-dev/locode-db v0.6.0
	github.com/nspcc-dev/neo-go v0.108.2-0.20250227114656-68d7e8e01cd8
	github.com/nspcc-dev/neofs-api-go/v2 v2.14.1-0.20240827150555-5ce597aa14ea
	github.com/nspcc-dev/neofs-contract v0.21.1-0.20250402160117-5558c10a5bc0
	github.com/nspcc-dev/neofs-sdk-go v1.0.0-rc.13.0.20250411080533-5b25f66796e2
	github.com/nspcc-dev/tzhash v1.8.2
	github.com/olekukonko/tablewriter v0.0.5
	github.com/panjf2000/ants/v2 v2.9.0
	github.com/prometheus/client_golang v1.20.2
	github.com/spf13/cast v1.6.0
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.18.2
	github.com/stretchr/testify v1.10.0
	go.etcd.io/bbolt v1.3.11
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20250210185358-939b2ce775ac
	golang.org/x/net v0.33.0
	golang.org/x/sync v0.11.0
	golang.org/x/sys v0.28.0
	golang.org/x/term v0.27.0
	google.golang.org/grpc v1.70.0
	google.golang.org/protobuf v1.36.5
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.14.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/consensys/bavard v0.1.13 // indirect
	github.com/consensys/gnark-crypto v0.14.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.4 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/holiman/uint256 v1.3.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/ipfs/go-cid v0.4.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nspcc-dev/dbft v0.3.2 // indirect
	github.com/nspcc-dev/go-ordered-json v0.0.0-20240830112754-291b000d1f3b // indirect
	github.com/nspcc-dev/neo-go/pkg/interop v0.0.0-20250127125426-50cb21333e4f // indirect
	github.com/nspcc-dev/rfc6979 v0.2.3 // indirect
	github.com/pelletier/go-toml/v2 v2.1.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/urfave/cli/v2 v2.27.4 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241202173237-19429a94021a // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	lukechampine.com/blake3 v1.2.1 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
)

retract (
	v1.22.1 // Contains retraction only.
	v1.22.0 // Published accidentally.
)
