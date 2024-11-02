## neofs-cli acl basic print

Pretty print basic ACL from the HEX representation

### Synopsis

Pretty print basic ACL from the HEX representation or keyword.
Few roles have exclusive default access to set of operation, even if particular bit deny it.
Container have access to the operations of the data replication mechanism:
    Get, Head, Put, Search, Hash.
InnerRing members are allowed to data audit ops only:
    Get, Head, Hash, Search.

```
neofs-cli acl basic print [flags]
```

### Examples

```
neofs-cli acl basic print 0x1C8C8CCC
```

### Options

```
  -h, --help   help for print
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli acl basic](neofs-cli_acl_basic.md)	 - Operations with Basic Access Control Lists

