arroba [![Circle CI](https://circleci.com/gh/snarfed/arroba.svg?style=svg)](https://circleci.com/gh/snarfed/arroba) [![Coverage Status](https://coveralls.io/repos/github/snarfed/arroba/badge.svg?branch=main)](https://coveralls.io/github/snarfed/arroba?branch=master)
===

Python implementation of [Bluesky](https://blueskyweb.xyz/) [PDS](https://atproto.com/guides/data-repos) and [AT Protocol](https://atproto.com/specs/atp), including data repository, Merkle search tree, and [com.atproto.sync XRPC methods](https://atproto.com/lexicons/com-atproto-sync).

You can build your own PDS on top of arroba with just a few lines of Python and run it in any WSGI server. You can build a more involved PDS with custom logic and behavior. Or you can build a different ATProto service, eg an [AppView, BGS](https://blueskyweb.xyz/blog/5-5-2023-federation-architecture), or something entirely new!

Install [from PyPI](https://pypi.org/project/arroba/) with `pip install arroba`.

_Arroba_ is the Spanish word for the [@ character](https://en.wikipedia.org/wiki/At_sign) ("at sign").

License: This project is placed in the public domain. You may also use it under the [CC0 License](https://creativecommons.org/publicdomain/zero/1.0/).

* [Usage](#usage)
* [Overview](#overview)
* [Configuration](#configuration)
* [Docs](https://arroba.readthedocs.io/)
* [Changelog](#changelog)
* [Release instructions](#release-instructions)


## Usage

Here's minimal example code for a multi-repo PDS on top of arroba and [Flask](https://flask.palletsprojects.com/):

```py
from flask import Flask
from google.cloud import ndb
from lexrpc.flask_server import init_flask

from arroba import server
from arroba.datastore_storage import DatastoreStorage
from arroba.xrpc_sync import send_new_commits

server.storage = DatastoreStorage()
server.repo.callback = lambda _: send_new_commits()  # to subscribeRepos

app = Flask('my-pds')
init_flask(server.server, app)

# for Google Cloud Datastore
ndb_client = ndb.Client()

def ndb_context_middleware(wsgi_app):
    def wrapper(environ, start_response):
        with ndb_client.context():
            return wsgi_app(environ, start_response)
    return wrapper

app.wsgi_app = ndb_context_middleware(app.wsgi_app)
```

See [`app.py`](https://github.com/snarfed/arroba/blob/main/app.py) for a more comprehensive example, including a CORS handler for `OPTIONS` preflight requests and a catch-all `app.bsky.*` XRPC handler that proxies requests to the AppView.


## Overview

Arroba consists of these parts:

* **Data structures**:
  * [`Repo`](https://arroba.readthedocs.io/en/stable/source/arroba.html#arroba.repo.Repo)
  * [`MST`](https://arroba.readthedocs.io/en/stable/source/arroba.html#arroba.mst.MST) (Merkle search tree)
* **Storage**:
  * [`Storage`](https://arroba.readthedocs.io/en/stable/source/arroba.html#arroba.storage.Storage) abstract base class
  * [`DatastoreStorage`](https://arroba.readthedocs.io/en/stable/source/arroba.html#arroba.datastore_storage.DatastoreStorage) (uses [Google Cloud Datastore](https://cloud.google.com/datastore/docs/))
  * [TODO: filesystem storage](https://github.com/snarfed/arroba/issues/5)
* **XRPC handlers**:
  * [`com.atproto.repo`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_repo)
  * [`com.atproto.server`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_server)
  * [`com.atproto.sync`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_sync)
* **Utilities**:
  * [`did`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.did): create and resolve [`did:plc`](https://atproto.com/specs/did-plc)s, [`did:web`](https://w3c-ccg.github.io/did-method-web/)s, and [domain handles](https://atproto.com/specs/handle)
  * [`diff`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.diff): find the deterministic minimal difference between two [`MST`](https://arroba.readthedocs.io/en/stable/source/arroba.html#arroba.mst.MST)s
  * [`util`](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.util): miscellaneous utilities for [TIDs](https://atproto.com/specs/record-key#record-key-type-tid), [AT URIs](https://atproto.com/specs/at-uri-scheme), [signing and verifying signatures](https://atproto.com/specs/repository#commit-objects), [generating JWTs](https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification), encoding/decoding, and more


## Configuration

Configure arroba with these environment variables:

* `APPVIEW_HOST`, default `api.bsky-sandbox.dev`
* `BGS_HOST`, default `bgs.bsky-sandbox.dev`
* `PLC_HOST`, default `plc.bsky-sandbox.dev`
* `PDS_HOST`, where you're running your PDS

Optional, only used in the [com.atproto.server XRPC handlers](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_server):

* `REPO_TOKEN`, static token to use as both `accessJwt` and `refreshJwt`, defaults to contents of `repo_token` file. Not required to be an actual JWT.

<!-- Only used in app.py:
* `REPO_DID`, repo user's DID, defaults to contents of `repo_did` file
* `REPO_HANDLE`, repo user's domain handle, defaults to `did:plc:*.json` file
* `REPO_PASSWORD`, repo user's password, defaults to contents of `repo_password` file
* `REPO_PRIVKEY`, repo user's private key in PEM format, defaults to contents of `privkey.pem` file
-->


## Changelog

### 0.5 - unreleased

* `datastore_storage`:
  * Bug fix for `DatastoreStorage.last_seq`, handle new NSID.
  * Add new `AtpRemoteBlob` class for storing "remote" blobs, available at public HTTP URLs, that we don't store ourselves.
* `did`:
  * `resolve_handle`: return None on bad domain, eg `.foo.com`.
  * `resolve_handle` bug fix: handle `charset` specifier in HTTPS method response `Content-Type`.
* `util`:
  * `new_key`: add `seed` kwarg to allow deterministic key generation.
* `xrpc_repo`:
  * `getRecord`: try to load record locally first; if not available, forward to AppView.
* `xrpc_sync`:
  * Implement `getBlob`, right now only based on "remote" blobs stored in `AtpRemoteBlob`s in datastore storage.

### 0.4 - 2023-09-19

* Migrate to [ATProto repo v3](https://atproto.com/blog/repo-sync-update). Specifically, the existing `subscribeRepos` sequence number is reused as the new `rev` field in commits. ([Discussion.](https://github.com/bluesky-social/atproto/discussions/1607)).
* Add new `did` module with utilities to create and resolve `did:plc`s and resolve `did:web`s.
* Add new `util.service_jwt` function that generates [ATProto inter-service JWTs](https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification).
* `Repo`:
  * Add new `signing_key`/`rotation_key` attributes. Generate store, and load both in `datastore_storage`.
  * Remove `format_init_commit`, migrate existing calls to `format_commit`.
* `Storage`:
  * Rename `read_from_seq` => `read_blocks_by_seq` (and in `MemoryStorage` and `DatastoreStorage`), add new `read_commits_by_seq` method.
  * Merge `load_repo` `did`/`handle` kwargs into `did_or_handle`.
* XRPCs:
  * Make `subscribeRepos` check storage for all new commits every time it wakes up.
    * As part of this, replace `xrpc_sync.enqueue_commit` with new `send_new_commits` function that takes no parameters.
  * Drop bundled `app.bsky`/`com.atproto` lexicons, use [lexrpc](https://lexrpc.readthedocs.io/)'s instead.

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
1. ```sh
   setenv ver X.Y
   git commit -am "release v$ver"
   ```
1. Upload to [test.pypi.org](https://test.pypi.org/) for testing.

    ```sh
    python3 -m build
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
