arroba [![Circle CI](https://circleci.com/gh/snarfed/arroba.svg?style=svg)](https://circleci.com/gh/snarfed/arroba) [![Coverage Status](https://coveralls.io/repos/github/snarfed/arroba/badge.svg?branch=main)](https://coveralls.io/github/snarfed/arroba?branch=master)
===

Python implementation of [Bluesky](https://blueskyweb.xyz/) [PDS](https://atproto.com/guides/data-repos) and [AT Protocol](https://atproto.com/specs/atp), including data repository, Merkle search tree, and [com.atproto.sync XRPC methods](https://atproto.com/lexicons/com-atproto-sync).

_Arroba_ is the Spanish word for the [@ character](https://en.wikipedia.org/wiki/At_sign) ("at sign").

Install from [PyPI](https://pypi.org/project/arroba/) with `pip install arroba`.

License: This project is placed into the public domain.

* [Usage](#usage)
* [Changelog](#changelog)
* [Release instructions](#release-instructions)


## Usage

TODO

Single-user demo PDS based on arroba, for testing with the [ATProto federation sandbox](https://atproto.com/blog/federation-developer-sandbox). Environment variables:

* `APPVIEW_HOST`, default `api.bsky-sandbox.dev`
* `BGS_HOST`, default `bgs.bsky-sandbox.dev`
* `PLC_HOST`, default `plc.bsky-sandbox.dev`
* `PDS_HOST`, where you're running your PDS
* `REPO_DID`, repo user's DID, defaults to contents of `repo_did` file
* `REPO_HANDLE`, repo user's domain handle, defaults to `did:plc:*.json` file
* `REPO_PASSWORD`, repo user's password, defaults to contents of `repo_password` file
* `REPO_PRIVKEY`, repo user's private key in PEM format, defaults to contents of `privkey.pem` file
* `REPO_TOKEN`, static token to use as both `accessJwt` and `refreshJwt`, defaults to contents of `repo_token` file. Not required to be an actual JWT.


## Changelog

### 0.3 - 2023-08-29

Big milestone: arroba is successfully federating with the [ATProto sandbox](https://atproto.com/blog/federation-developer-sandbox)! See [app.py](https://github.com/snarfed/arroba/blob/main/app.py) for the minimal demo code needed to wrap arroba in a fully functional PDS.

* Add Google Cloud Datastore implementation of repo storage.
* Implement `com.atproto` XRPC methods needed to federate with sandbox, including most of `repo` and `sync`.
  * Notably, includes `subscribeRepos` server side over websocket.
* ...and much more.

### 0.2 - 2023-05-18

Implement repo and commit chain in new Repo class, including pluggable storage. This completes the first pass at all PDS data structures. Next release will include initial implementations of the `com.atproto.sync.*` XRPC methods.

### 0.1 - 2023-04-30

Initial release! Still very in progress. MST, Walker, and Diff classes are mostly complete and working. Repo, commits, and sync XRPC methods are still in progress.


Release instructions
---
Here's how to package, test, and ship a new release.

1. Run the unit tests.

    ```sh
    source local/bin/activate.csh
    python3 -m unittest discover
    ```
1. Bump the version number in `pyproject.toml` and `docs/conf.py`. `git grep` the old version number to make sure it only appears in the changelog. Change the current changelog entry in `README.md` for this new version from _unreleased_ to the current date.
1. Build the docs. If you added any new modules, add them to the appropriate file(s) in `docs/source/`. Then run `./docs/build.sh`. Check that the generated HTML looks fine by opening `docs/_build/html/index.html` and looking around.
1. `git commit -am 'release vX.Y'`
1. Upload to [test.pypi.org](https://test.pypi.org/) for testing.

    ```sh
    python3 -m build
    setenv ver X.Y
    twine upload -r pypitest dist/arroba-$ver*
    ```
1. Install from test.pypi.org.

    ```sh
    cd /tmp
    python3 -m venv local
    source local/bin/activate.csh
    # make sure we force pip to use the uploaded version
    pip3 uninstall arroba
    pip3 install --upgrade pip
    pip3 install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple arroba==$ver
    deactivate
    ```
1. Smoke test that the code trivially loads and runs.

    ```sh
    source local/bin/activate.csh
    python3
    # TODO: test code
    deactivate
    ```
1. Tag the release in git. In the tag message editor, delete the generated comments at bottom, leave the first line blank (to omit the release "title" in github), put `### Notable changes` on the second line, then copy and paste this version's changelog contents below it.

    ```sh
    git tag -a v$ver --cleanup=verbatim
    git push && git push --tags
    ```
1. [Click here to draft a new release on GitHub.](https://github.com/snarfed/arroba/releases/new) Enter `vX.Y` in the _Tag version_ box. Leave _Release title_ empty. Copy `### Notable changes` and the changelog contents into the description text box.
1. Upload to [pypi.org](https://pypi.org/)!

    ```sh
    twine upload dist/arroba-$ver*
    ```
1. [Wait for the docs to build on Read the Docs](https://readthedocs.org/projects/arroba/builds/), then check that they look ok.
1. On the [Versions page](https://readthedocs.org/projects/arroba/versions/), check that the new version is active, If it's not, activate it in the _Activate a Version_ section.
