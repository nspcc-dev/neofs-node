# Contribution guide

First, thank you for contributing! We love and encourage pull requests from
everyone. Please follow the guidelines:

- Check the open [issues](https://github.com/nspcc-dev/neofs-dev-env/issues) and
  [pull requests](https://github.com/nspcc-dev/neofs-dev-env/pulls) for existing
  discussions.

- Open an issue first, to discuss a new feature or enhancement.

- Open a pull request, and reference the relevant issue(s).

- Make sure your commits are logically separated and have good comments
  explaining the details of your change.

- After receiving feedback, amend your commits or add new ones as
  appropriate.

- **Have fun!**

## Development Workflow

Start by forking the `neofs-dev-env` repository, make changes in a branch and then
send a pull request. We encourage pull requests to discuss code changes. Here
are the steps in details:

### Setup your GitHub Repository
Fork [NeoFS node upstream](https://github.com/nspcc-dev/neofs-dev-env/fork) source
repository to your own personal repository. Copy the URL of your fork (you will
need it for the `git clone` command below).

```sh
$ git clone https://github.com/nspcc-dev/neofs-dev-env
```

### Set up git remote as ``upstream``
```sh
$ cd neofs-dev-env
$ git remote add upstream https://github.com/nspcc-dev/neofs-dev-env
$ git fetch upstream
$ git merge upstream/master
...
```

### Create your feature branch
Before making code changes, make sure you create a separate branch for these
changes. Maybe you will find it convenient to name branch in
`<type>/<Issue>-<changes_topic>` format.

```
$ git checkout -b feature/123-something_awesome
```

### Test your changes
After your code changes, make sure

- To add test cases for the new code.
- To run `make lint`
- To squash your commits into a single commit or a series of logically separated
  commits run `git rebase -i`. It's okay to force update your pull request.

### Commit changes
After verification, commit your changes. This is a [great
post](https://chris.beams.io/posts/git-commit/) on how to write useful commit
messages. Try following this template:

```
[#Issue] <component> Summary

Description

<Macros>

<Sign-Off>
```

```
$ git commit -am '[#123] Add some feature'
```

### Push to the branch
Push your locally committed changes to the remote origin (your fork)
```
$ git push origin feature/123-something_awesome
```

### Create a Pull Request
Pull requests can be created via GitHub. Refer to [this
document](https://help.github.com/articles/creating-a-pull-request/) for
detailed steps on how to create a pull request. After a Pull Request gets peer
reviewed and approved, it will be merged.

## DCO Sign off

All authors to the project retain copyright to their work. However, to ensure
that they are only submitting work that they have rights to, we are requiring
everyone to acknowledge this by signing their work.

Any copyright notices in this repository should specify the authors as "the
contributors".

To sign your work, just add a line like this at the end of your commit message:

```
Signed-off-by: Samii Sakisaka <samii@nspcc.ru>
```

This can easily be done with the `--signoff` option to `git commit`.

By doing this you state that you can certify the following (from [The Developer
Certificate of Origin](https://developercertificate.org/):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```
