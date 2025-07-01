# Extended headers

## Overview

Extended headers are used for request/response. They may contain any user-defined headers
to be interpreted on application level.
Key name must be a unique valid UTF-8 string. Value can't be empty. Requests or
Responses with duplicated header names or headers with empty values are
considered invalid.

## Existing headers

There are some "well-known" headers starting with `__NEOFS__` prefix that
affect system behaviour:

* `__NEOFS__NETMAP_EPOCH` - netmap epoch to use for object placement calculation. The `value` is string
encoded `uint64` in decimal presentation. If set to '0' or omitted, the
current epoch only will be used.
* `__NEOFS__NETMAP_LOOKUP_DEPTH` - if object can't be found using current epoch's netmap, this header limits
how many past epochs the node can look up through. Depth is applied to a current epoch or the value
of `__NEOFS__NETMAP_EPOCH` attribute. The `value` is string encoded `uint64` in decimal presentation.
If set to '0' or not set, only the current epoch is used.

## `neofs-cli` commands with `--xhdr`

List of commands with support of extended headers:
* `container list-objects`
* `object delete/get/hash/head/lock/put/range/search`
* `storagegroup delete/get/list/put`

Example:
```shell
$ neofs-cli object put -r s01.neofs.devenv:8080 -w wallet.json --cid CID --file FILE --xhdr "__NEOFS__NETMAP_EPOCH=777"
```
