## neofs-cli container delete

Delete existing container

### Synopsis

Delete existing container.
Only owner of the container has a permission to remove container.

```
neofs-cli container delete [flags]
```

### Options

```
      --address string        Address of wallet account
      --await                 Block execution until container is removed. Increases default execution timeout to 60s
      --cid string            Container ID.
  -f, --force                 Skip validation checks (ownership, presence of LOCK objects)
  -h, --help                  help for delete
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the container DELETE session
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

