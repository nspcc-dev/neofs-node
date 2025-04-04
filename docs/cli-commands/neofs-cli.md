## neofs-cli

Command Line Tool to work with NeoFS

### Synopsis

NeoFS CLI provides all basic interactions with NeoFS and it's services.

It contains commands for interaction with NeoFS nodes using different versions
of neofs-api and some useful utilities for compiling ACL rules from JSON
notation, managing container access through protocol gates, querying network map
and much more!

```
neofs-cli [flags]
```

### Options

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -h, --help            help for neofs-cli
  -v, --verbose         Verbose output
      --version         Application version and NeoFS API compatibility
```

### SEE ALSO

* [neofs-cli accounting](neofs-cli_accounting.md)	 - Operations with accounts and balances
* [neofs-cli acl](neofs-cli_acl.md)	 - Operations with Access Control Lists
* [neofs-cli bearer](neofs-cli_bearer.md)	 - Operations with bearer token
* [neofs-cli completion](neofs-cli_completion.md)	 - Generate completion script
* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers
* [neofs-cli control](neofs-cli_control.md)	 - Operations with storage node
* [neofs-cli gendoc](neofs-cli_gendoc.md)	 - Generate documentation for this command
* [neofs-cli netmap](neofs-cli_netmap.md)	 - Operations with Network Map
* [neofs-cli object](neofs-cli_object.md)	 - Operations with Objects
* [neofs-cli session](neofs-cli_session.md)	 - Operations with session token
* [neofs-cli storagegroup](neofs-cli_storagegroup.md)	 - Operations with Storage Groups
* [neofs-cli util](neofs-cli_util.md)	 - Utility operations

