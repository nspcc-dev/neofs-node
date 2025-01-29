## neofs-cli acl extended create

Create extended ACL from the text representation

### Synopsis

Create extended ACL from the text representation.

Rule consist of these blocks: <action> <operation> [<filter1> ...] [<target1> ...]

Action is 'allow' or 'deny'.

Operation is an object service verb: 'get', 'head', 'put', 'search', 'delete', 'getrange', or 'getrangehash'.

Filter consists of <typ>:<key><match><value>
  Typ is 'obj' for object applied filter or 'req' for request applied filter. 
  Key is a valid unicode string corresponding to object or request header key. 
    Well-known system object headers start with '$Object:' prefix.
    User defined headers start without prefix.
    Read more about filter keys at github.com/nspcc-dev/neofs-api/blob/master/proto-docs/acl.md#message-eaclrecordfilter
  Match is:
    '=' for string equality or, if no value, attribute absence;
    '!=' for string inequality;
    '>' | '>=' | '<' | '<=' for integer comparison.
  Value is a valid unicode string corresponding to object or request header value. Numeric filters must have base-10 integer values.

Target is 
  'user' for container owner, 
  'system' for Storage nodes in container and Inner Ring nodes,
  'others' for all other request senders, 
  'address:<adr1>,<adr2>,...' for exact request sender, where <adr> is a base58 25-byte address. Example: NSiVJYZej4XsxG5CUpdwn7VRQk8iiiDMPM.

When both '--rule' and '--file' arguments are used, '--rule' records will be placed higher in resulting extended ACL table.


```
neofs-cli acl extended create [flags]
```

### Examples

```
neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -f rules.txt --out table.json
neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -r 'allow get obj:Key=Value others' -r 'deny put others' -r 'deny put obj:$Object:payloadLength<4096 others' -r 'deny get obj:Quality>=100 others'
```

### Options

```
      --cid string         Container ID.
  -f, --file string        Read list of extended ACL table records from text file
  -h, --help               help for create
  -o, --out string         Save JSON formatted extended ACL table in file
  -r, --rule stringArray   Extended ACL table record to apply
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli acl extended](neofs-cli_acl_extended.md)	 - Operations with Extended Access Control Lists

