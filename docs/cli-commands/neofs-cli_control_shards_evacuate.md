## neofs-cli control shards evacuate

Evacuate objects from shard

### Synopsis

Evacuate objects from shard to other shards

```
neofs-cli control shards evacuate [flags]
```

### Options

```
      --address string     Address of wallet account
      --all                Process all shards
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for evacuate
      --id strings         List of shard IDs in base58 encoding
      --no-errors          Skip invalid/unreadable objects
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

