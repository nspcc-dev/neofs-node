# REST Gateway

REST Gateway to access data in NeoFS using REST.

Source code and more information can be found in [project's GitHub repository](https://github.com/nspcc-dev/neofs-rest-gw)

## .env settings

### REST_GW_VERSION=0.4.0

Image version label to use for containers.

If you want to use locally built image, just set its label here. 
Instead of pulling from DockerHub, the local image will be used.

### REST_GW_IMAGE=nspccdev/neofs-rest-gw

Image label prefix to use for containers.

## Usage example

- List container for specific owner:

```shell
$ curl http://rest.neofs.devenv:8090/v1/containers?ownerId=NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM | jq
{
  "containers": [
    {
      "attributes": [
        {
          "key": "Timestamp",
          "value": "1663755230"
        }
      ],
      "basicAcl": "fbfbfff",
      "cannedAcl": "eacl-public-read-write",
      "containerId": "BKcAvz8awKKy9NGsGKi1Hoxxu9AjTGvjKMNMQamvdLmX",
      "containerName": "",
      "ownerId": "NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM",
      "placementPolicy": "REP 1 IN X\nCBF 1\nSELECT 1 FROM * AS X",
      "version": "v2.13"
    }
  ],
  "size": 1
}
```


- Get container info:

```shell
$ curl http://rest.neofs.devenv:8090/v1/containers/BKcAvz8awKKy9NGsGKi1Hoxxu9AjTGvjKMNMQamvdLmX | jq
{
  "attributes": [
    {
      "key": "Timestamp",
      "value": "1663755230"
    }
  ],
  "basicAcl": "fbfbfff",
  "cannedAcl": "eacl-public-read-write",
  "containerId": "BKcAvz8awKKy9NGsGKi1Hoxxu9AjTGvjKMNMQamvdLmX",
  "containerName": "",
  "ownerId": "NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM",
  "placementPolicy": "REP 1 IN X\nCBF 1\nSELECT 1 FROM * AS X",
  "version": "v2.13"
}
```

See all available routes http://rest.neofs.devenv:8090/v1/docs
