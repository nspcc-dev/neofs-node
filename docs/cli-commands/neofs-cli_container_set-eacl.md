## neofs-cli container set-eacl

Set new extended ACL table for container

### Synopsis

Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.

```
neofs-cli container set-eacl [flags]
```

### Options

```
      --address string        Address of wallet account
      --await                 block execution until EACL is persisted. Increases default execution timeout to 60s
      --cid string            Container ID.
  -f, --force                 skip validation checks (ownership, extensibility of the container ACL)
  -g, --generate-key          Generate new private key
  -h, --help                  help for set-eacl
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the container SETEACL session
      --table string          path to file with JSON or binary encoded EACL table
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

