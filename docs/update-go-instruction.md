# Updating Golang version

## Update go.mod

`go mod edit -go=X.Y`

## Update CI

Change Golang versions for unit test in CI.
There is `go` section in `.github/workflows/go.yaml` file:
```yaml
jobs:
  test:
    runs-on: ubuntu-20.04
    strategy:
      matrix:
        go: [ 'X.Y.x', 'X.Y.x' ]
```

That section should contain two latest Golang minor versions
that are currently supported by Golang authors.

## Update docker images

Update all docker files that contain `golang` image in `./docker`
directory.

## Update Makefile

Update `GO_VERSION` variable in `./Makefile`.

## Apply language changes

Open PR that fixes/updates repository's code according to 
language improvements.
