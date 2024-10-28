## neofs-cli gendoc

Generate documentation for this command

### Synopsis

Generate documentation for this command. If the template is not provided,
builtin cobra generator is used and each subcommand is placed in
a separate file in the same directory.

The last optional argument specifies the template to use with text/template.
In this case there is a number of helper functions which can be used:
  replace STR FROM TO -- same as strings.ReplaceAll
  join ARRAY SEPARATOR -- same as strings.Join
  split STR SEPARATOR -- same as strings.Split
  fullUse CMD -- slice of all command names starting from the parent
  listFlags CMD -- list of command flags


```
neofs-cli gendoc <dir> [flags]
```

### Options

```
      --depth int              If template is specified, unify all commands starting from depth in a single file. Default: 1. (default 1)
  -d, --disable-auto-gen-tag   Defines if gen tag of cobra will be printed.
  -e, --extension string       If the template is specified, string to append to the output file names
  -h, --help                   help for gendoc
  -t, --type string            Type for the documentation ('md' or 'man') (default "md")
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli](neofs-cli.md)	 - Command Line Tool to work with NeoFS

