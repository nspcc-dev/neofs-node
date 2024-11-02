## neofs-cli control shards set-mode

Set work mode of the shard

### Synopsis

Set work mode of the shard

```
neofs-cli control shards set-mode [flags]
```

### Options

```
      --address string     Address of wallet account
      --all                Process all shards
      --clear-errors       Set shard error count to 0
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for set-mode
      --id strings         List of shard IDs in base58 encoding
      --mode string        New shard mode ('degraded-read-only', 'read-only', 'read-write')
  -t, --timeout duration   Timeout for the operation (default 15s)
  -w, --wallet string      Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli control shards](neofs-cli_control_shards.md)	 - Operations with storage node's shards

