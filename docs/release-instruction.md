# Release instructions

## Pre-release checks

These should run successfully:

* `make all`;
* `make test`;
* `make lint` (should not change any files);
* `make fmts` (should not change any files);
* `go mod tidy` (should not change any files);
* integration tests in [neofs-devenv](https://github.com/nspcc-dev/neofs-devenv).

## Make release commit

Use `vX.Y.Z` tag for releases and `vX.Y.Z-rc.N` for release candidates
following the [semantic versioning](https://semver.org/) standard.

Determine the revision number for the release:

```shell
$ export NEOFS_REVISION=X.Y.Z[-rc.N]
$ export NEOFS_TAG_PREFIX=v
```

Double-check the number:

```shell
$ echo ${NEOFS_REVISION}
```

Create release branch from the main branch of the origin repository:

```shell
$ git checkout -b release/${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
```

### Update versions

Write new revision number into the root `VERSION` file:

```shell
$ echo ${NEOFS_TAG_PREFIX}${NEOFS_REVISION} > VERSION
```

Update version in Debian package changelog file
```
$ cat debian/changelog
```

Update the supported version of `nspcc-dev/neofs-contract` module in root
`README.md` if needed.

### Writing changelog

Add an entry to the `CHANGELOG.md` following the style established there.

* copy `Unreleased` section (next steps relate to section below `Unreleased`)
* replace `Unreleased` link with the new revision number
* update `Unreleased...new` and `new...old` diff-links at the bottom of the file
* add optional codename and release date in the heading
* remove all empty sections such as `Added`, `Removed`, etc.
* make sure all changes have references to GitHub issues in `#123` format (if possible)
* clean up all `Unreleased` sections and leave them empty

### Make release commit

Stage changed files for commit using `git add`. Commit the changes:

```shell
$ git commit -s -m 'Release '${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
```

### Open pull request

Push release branch:

```shell
$ git push <remote> release/${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
```

Open pull request to the main branch of the origin repository so that the
maintainers check the changes. Remove release branch after the merge.

## Tag the release

Pull the main branch with release commit created in previous step. Tag the commit
with PGP signature.

```shell
$ git checkout master && git pull
$ git tag -s ${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
```

## Push the release tag

```shell
$ git push origin ${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
```

## Post-release

### Prepare and push images to a Docker Hub (if not automated)

Create Docker images for all applications and push them into Docker Hub
(requires [organization](https://hub.docker.com/u/nspccdev) privileges)

```shell
$ git checkout ${NEOFS_TAG_PREFIX}${NEOFS_REVISION}
$ make images
$ docker push nspccdev/neofs-storage:${NEOFS_REVISION}
$ docker push nspccdev/neofs-storage-testnet:${NEOFS_REVISION}
$ docker push nspccdev/neofs-ir:${NEOFS_REVISION}
$ docker push nspccdev/neofs-cli:${NEOFS_REVISION}
$ docker push nspccdev/neofs-adm:${NEOFS_REVISION}
```

### Make a proper GitHub release (if not automated)

Edit an automatically-created release on GitHub, copy things from `CHANGELOG.md`.
Build and tar release binaries with `make prepare-release`, attach them to
the release. Publish the release.

### Update NeoFS Developer Environment

Prepare pull-request in [neofs-devenv](https://github.com/nspcc-dev/neofs-devenv)
with new versions.

### Close GitHub milestone

Look up GitHub [milestones](https://github.com/nspcc-dev/neofs-node/milestones) and close the release one if exists.

### Rebuild NeoFS LOCODE database

If new release contains LOCODE-related changes, rebuild NeoFS LOCODE database via NeoFS CLI

```shell
$ neofs-cli util locode generate ...
```
