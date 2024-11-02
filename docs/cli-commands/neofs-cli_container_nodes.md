## neofs-cli container nodes

Show nodes for container

### Synopsis

Show nodes taking part in a container at the current epoch.

```
neofs-cli container nodes [flags]
```

### Options

```
      --address string        Address of wallet account
      --cid string            Container ID.
      --from string           Path to file with encoded container
  -g, --generate-key          Generate new private key
  -h, --help                  help for nodes
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --short                 Shortens output of node info
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

* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers

