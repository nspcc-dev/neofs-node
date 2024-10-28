## neofs-cli control shards restore

Restore objects from shard

### Synopsis

Restore objects from shard to a file

```
neofs-cli control shards restore [flags]
```

### Options

```
      --address string     Address of wallet account
      --endpoint string    Remote node control address (as 'multiaddr' or '<host>:<port>')
  -h, --help               help for restore
      --id string          Shard ID in base58 encoding
      --no-errors          Skip invalid/unreadable objects
      --path string        File to read objects from
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

