package storagecfg

const configTemplate = `logger:
  level: info  # logger level: one of "debug", "info" (default), "warn", "error", "dpanic", "panic", "fatal"

node:
  wallet:
    path: {{ .Wallet.Path }}  # path to a NEO wallet; ignored if key is presented
    address: {{ .Wallet.Account }}  # address of a NEO account in the wallet; ignored if key is presented
    password: {{ .Wallet.Password }}  # password for a NEO account in the wallet; ignored if key is presented
  addresses:  # list of addresses announced by Storage node in the Network map
    - {{ .AnnouncedAddress }}
  attribute_0: UN-LOCODE:{{ .Attribute.Locode }}
  relay: {{ .Relay }}  # start Storage node in relay mode without bootstrapping into the Network map

grpc:
  num: 1  # total number of listener endpoints
  0:
    endpoint: {{ .Endpoint }}  # endpoint for gRPC server
    tls:{{if .TLSCert}}
      enabled: true  # enable TLS for a gRPC connection (min version is TLS 1.2)
      certificate: {{ .TLSCert }}  # path to TLS certificate
      key: {{ .TLSKey }}  # path to TLS key
    {{- else }}
      enabled: false # disable TLS for a gRPC connection
    {{- end}}

control:
  authorized_keys:  # list of hex-encoded public keys that have rights to use the Control Service
    {{- range .AuthorizedKeys }}
    - {{.}}{{end}}
  grpc:
    endpoint: {{.ControlEndpoint}}  # endpoint that is listened by the Control Service

fschain:
  dial_timeout: 20s  # timeout for FS chain NEO RPC client connection
  cache_ttl: 15s  # use TTL cache for FS chain GET operations
  endpoints:  # FS chain N3 RPC endpoints
    {{- range .MorphRPC }}
    - wss://{{.}}/ws{{end}}
{{if not .Relay }}
storage:
  shard_pool_size: 15  # size of per-shard worker pools used for PUT operations

  shard:
    default: # section with the default shard parameters
      metabase:
        perm: 0644  # permissions for metabase files(directories: +x for current user and group)

      blobstor:
        perm: 0644  # permissions for blobstor files(directories: +x for current user and group)
        depth: 2  # max depth of object tree storage in FS
        small_object_size: 102400  # 100KiB, size threshold for "small" objects which are stored in key-value DB, not in FS, bytes
        compress: true  # turn on/off Zstandard compression (level 3) of stored objects
        compression_exclude_content_types:
          - audio/*
          - video/*

        peapod:

      gc:
        remover_batch_size: 200  # number of objects to be removed by the garbage collector
        remover_sleep_interval: 5m  # frequency of the garbage collector invocation
    0:
      mode: "read-write"  # mode of the shard, must be one of the: "read-write" (default), "read-only"

      metabase:
        path: {{ .MetabasePath }}  # path to the metabase

      blobstor:
        path: {{ .BlobstorPath }}  # path to the blobstor
{{end}}`

const (
	neofsMainnetAddress   = "2cafa46838e8b564468ebd868dcafdd99dce6221"
	balanceMainnetAddress = "dc1ec98d9d0c5f9dfade16144defe08cffc5ca55"
	neofsTestnetAddress   = "b65d8243ac63983206d17e5221af0653a7266fa1"
	balanceTestnetAddress = "e0420c216003747626670d1424569c17c79015bf"
)

var n3config = map[string]struct {
	MorphRPC        []string
	RPC             []string
	NeoFSContract   string
	BalanceContract string
}{
	"testnet": {
		MorphRPC: []string{
			"rpc01.morph.testnet.fs.neo.org:51331",
			"rpc02.morph.testnet.fs.neo.org:51331",
			"rpc03.morph.testnet.fs.neo.org:51331",
			"rpc04.morph.testnet.fs.neo.org:51331",
			"rpc05.morph.testnet.fs.neo.org:51331",
			"rpc06.morph.testnet.fs.neo.org:51331",
			"rpc07.morph.testnet.fs.neo.org:51331",
		},
		RPC: []string{
			"rpc01.testnet.n3.nspcc.ru:21331",
			"rpc02.testnet.n3.nspcc.ru:21331",
			"rpc03.testnet.n3.nspcc.ru:21331",
			"rpc04.testnet.n3.nspcc.ru:21331",
			"rpc05.testnet.n3.nspcc.ru:21331",
			"rpc06.testnet.n3.nspcc.ru:21331",
			"rpc07.testnet.n3.nspcc.ru:21331",
		},
		NeoFSContract:   neofsTestnetAddress,
		BalanceContract: balanceTestnetAddress,
	},
	"mainnet": {
		MorphRPC: []string{
			"rpc1.morph.fs.neo.org:40341",
			"rpc2.morph.fs.neo.org:40341",
			"rpc3.morph.fs.neo.org:40341",
			"rpc4.morph.fs.neo.org:40341",
			"rpc5.morph.fs.neo.org:40341",
			"rpc6.morph.fs.neo.org:40341",
			"rpc7.morph.fs.neo.org:40341",
		},
		RPC: []string{
			"rpc1.n3.nspcc.ru:10331",
			"rpc2.n3.nspcc.ru:10331",
			"rpc3.n3.nspcc.ru:10331",
			"rpc4.n3.nspcc.ru:10331",
			"rpc5.n3.nspcc.ru:10331",
			"rpc6.n3.nspcc.ru:10331",
			"rpc7.n3.nspcc.ru:10331",
		},
		NeoFSContract:   neofsMainnetAddress,
		BalanceContract: balanceMainnetAddress,
	},
}
