# Release instructions

## Pre-release checks

These should run successfully:

* `make all`;
* `make test`;
* `make lint` (should not change any files);
* `make fmts` (should not change any files);
* `go mod tidy` (should not change any files);
* integration tests in [neofs-devenv](https://github.com/nspcc-dev/neofs-dev-env).

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
$ echo ${NEOFS_REVISION} > VERSION
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

## Create a GitHub release and a tag

Use "Draft a new release" button in the "Releases" section. Create a new
`vX.Y.Z` tag for it following the semantic versioning standard. Put change log
for this release into the description. Do not attach any binaries at this step.
Set the "Set as the latest release" checkbox if this is the latest stable
release or "Set as a pre-release" if this is an unstable pre-release.
Press the "Publish release" button.

## Add automatically-built binaries

New release created at the previous step triggers automatic builds (if not,
start them manually from the Build GitHub workflow), so wait for them to
finish. Built binaries should be automatically attached to the release as an
asset, check it on the release page. If binaries weren't attached after building
workflow completion, then submit the bug, download currently supported binaries
from the building job artifacts, unpack archive and add them to the
previously created release via "Edit release" button.

Docker image builds are triggered automatically as well, check they're successful
or upload manualy if that's not the case.

## Post-release

### Close GitHub milestone

Look up GitHub [milestones](https://github.com/nspcc-dev/neofs-node/milestones) and close the release one if exists.

### Update NeoFS Developer Environment

Prepare pull-request in [neofs-devenv](https://github.com/nspcc-dev/neofs-dev-env)
with new versions.

### Announcements

Copy the GitHub release page link to:
 * Discord channel
 * Element (Matrix) channel

### Deployment

Deploy the updated version to the mainnet/testnet.
