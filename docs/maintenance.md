# Maintenance mode of storage nodes

## Overview

Storage nodes turn to maintenance mode (MM) while internal procedures are required
to troubleshoot storage service issues. Data on MM nodes MAY be temporarily
unavailable depending on the procedures carried out at the site. Otherwise, such
nodes do not differ from fully functional ones. Nodes independently carry out
the procedure for turning on and off the MM.

## Reflection in the network map

To globally notify the network about the start of maintenance procedures, the node
MAY set the corresponding state in the network map. The ability to set MM state
is determined by the global network setting. If MM is disallowed at the moment,
the node will not be able to set this state. Network setting's change is not
retroactive: if MM becomes disallowed, the nodes with MM state are not excluded
from the network map.

To check the possibility of MM-switching at the moment, exec:
```shell
$ neofs-cli netmap netinfo
...
Maintenance mode allowed: true
...
```

To toggle the setting in the running NeoFS Sidechain, exec:
```shell
$ neofs-adm morph set-config MaintenanceModeAllowed=true|false
```

To switch the node to MM, exec:
```shell
$ neofs-cli control set-status --status maintenance [--force|-f]
```
`-f` flag allows to force local maintenance regardless of the network settings.

To stop the maintenance, use the same command but with any other supported state.

Note that the node starts maintenance instantly, while the network map state is changed
asynchronously: Inner Ring receives switch requests, asserts allowance according
to the current network settings, and updates the state of the candidate node for the
next network map. The change will take effect no earlier than the next epoch
in which a new version of the network map is released.

## Object service

Nodes under maintenance MAY fail operations of the NeoFS Object API. The node
maintained in the current repository always denies all object operations with
dedicated status `NODE_UNDER_MAINTENANCE`.

## Data replication

Data persistence after node maintenance is expected but not guaranteed.
In the basic case, the data replication mechanism would create backup replicas
of objects that should be stored on the MM-node. To reduce network load and
data operations, replicas on MM-nodes are a priori considered correct.
