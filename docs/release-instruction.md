# Release instructions

## Pre-release checks

These should run successfully:
 * build,
 * unit-tests,
 * integration tests in 
 [neofs-devenv](https://github.com/nspcc-dev/neofs-devenv).

## Writing changelog

Add an entry to the `CHANGELOG.md` following the style established there. Add an 
optional codename, version and release date in the heading. Write a paragraph
describing the most significant changes done in this release. Add
`Fixed`, `Added`, `Removed` and `Updated` sections with fixed bug descriptions
and changes. Describe each change in detail with a reference to github issues if
possible. 

## Tag the release

Use `vX.Y.Z` tag for releases and `vX.Y.Z-rc.N` for release candidates
following the [semantic versioning](https://semver.org/) standard.

## Push changes and release tag to github

This step should bypass the default PR mechanism to get a correct result (so
that releasing requires admin privileges for the project), both the `master`
branch update and tag must be pushed simultaneously like this:

    $ git push origin master v0.12.0

## Prepare and push images to a dockerhub

Create images for `neofs-storage`, `neofs-ir` and `neofs-cli` applications
and push them into dockerhub (so that releasing requires privileges in nspccdev
organization in dockerhub)

    $ make images
    $ docker push nspccdev/neofs-storage:0.12.0
    $ docker push nspccdev/neofs-ir:0.12.0
    $ docker push nspccdev/neofs-cli:0.12.0

## Make a proper github release

Edit an automatically-created release on github, copy things from changelog. 
Make a release.

## Close github milestone

Close corresponding vX.Y.Z github milestone.

## Post-release

Prepare pull-request for 
[neofs-devenv](https://github.com/nspcc-dev/neofs-devenv).