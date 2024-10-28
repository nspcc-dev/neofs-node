## neofs-cli control shards flush-cache

Flush objects from the write-cache to the main storage

### Synopsis

Flush objects from the write-cache to the main storage

```
neofs-cli control shards flush-cache [flags]
```

### Options

```
      --address string     Address of wallet account
      --all                Process all shards
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for flush-cache
      --id strings         List of shard IDs in base58 encoding
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

