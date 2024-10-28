## neofs-cli netmap snapshot

Request current local snapshot of the network map

### Synopsis

Request current local snapshot of the network map

```
neofs-cli netmap snapshot [flags]
```

### Options

```
      --address string        Address of wallet account
  -g, --generate-key          Generate new private key
  -h, --help                  help for snapshot
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -t, --timeout duration      Timeout for the operation (default 15s)
      --ttl uint32            TTL value in request meta header (default 2)
  -w, --wallet string         Path to the wallet
  -x, --xhdr strings          Request X-Headers in form of Key=Value
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli netmap](neofs-cli_netmap.md)	 - Operations with Network Map

