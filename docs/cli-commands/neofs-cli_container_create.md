## neofs-cli container create

Create new container

### Synopsis

Create new container and register it in the NeoFS. 
It will be stored in FS chain when inner ring will accepts it.

```
neofs-cli container create [flags]
```

### Options

```
      --address string        Address of wallet account
  -a, --attributes strings    Comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2
      --await                 Block execution until container is persisted. Increases default execution timeout to 60s
      --basic-acl string      HEX-encoded basic ACL value or one of the keywords ['public-read-write', 'private', 'eacl-public-read','eacl-private', 'public-read', 'eacl-public-read-write', 'public-append', 'eacl-public-append']. To see the basic ACL details, run: 'neofs-cli acl basic print' (default "private")
      --disable-timestamp     Disable timestamp container attribute
  -f, --force                 Skip placement validity check
      --global-name           Name becomes a domain name, that is registered with the default zone in NNS contract. Requires name attribute.
  -h, --help                  help for create
      --name string           Container name attribute
  -p, --policy string         QL-encoded or JSON-encoded placement policy or path to file with it
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the container PUT session
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

