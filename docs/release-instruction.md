# Release instructions

## Pre-release checks

These should run successfully:
 * `make all`;
 * `make test`;
 * `make lint` (should not change any files);
 * `make fmts` (should not change any files);
 * `go mod tidy` (should not change any files);
 * integration tests in [neofs-devenv](https://github.com/nspcc-dev/neofs-devenv).

## Writing changelog

Add an entry to the `CHANGELOG.md` following the style established there. Add an 
optional codename, version and release date in the heading. Write a paragraph
describing the most significant changes done in this release. Add
`Fixed`, `Added`, `Removed` and `Updated` sections with fixed bug descriptions
and changes. Describe each change in detail with a reference to GitHub issues if
possible. 

Update supported version of neofs-contract in `README.md` if there were 
changes between releases.

## Tag the release

Use `vX.Y.Z` tag for releases and `vX.Y.Z-rc.N` for release candidates
following the [semantic versioning](https://semver.org/) standard. Tag must be
created from the latest commit of the master branch.

## Push changes and release tag to GitHub

This step should bypass the default PR mechanism to get a correct result (so
that releasing requires admin privileges for the project), both the `master`
branch update and tag must be pushed simultaneously like this:

    $ git push origin master v0.12.0

## Prepare and push images to a Docker Hub

Create images for `neofs-storage`, `neofs-ir` and `neofs-cli`, `neofs-adm` applications
and push them into Docker Hub (so that releasing requires privileges in nspccdev
organization in Docker Hub)

    $ make images && make image-storage-testnet
    $ docker push nspccdev/neofs-storage:0.24.0
    $ docker push nspccdev/neofs-storage-testnet:0.24.0
    $ docker push nspccdev/neofs-ir:0.24.0
    $ docker push nspccdev/neofs-cli:0.24.0
    $ docker push nspccdev/neofs-adm:0.24.0

## Make a proper GitHub release

Edit an automatically-created release on GitHub, copy things from changelog.
Build and tar release binaries with `make prepare-release`, attach them to
the release. Publish the release.

## Close GitHub milestone

Close corresponding vX.Y.Z GitHub milestone.

## Post-release

Prepare pull-request for 
[neofs-devenv](https://github.com/nspcc-dev/neofs-devenv).

Rebuild NeoFS LOCODE database via CLI `util locode generate` command (if needed).