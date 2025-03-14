arroba [![Circle CI](https://circleci.com/gh/snarfed/arroba.svg?style=svg)](https://circleci.com/gh/snarfed/arroba) [![Coverage Status](https://coveralls.io/repos/github/snarfed/arroba/badge.svg?branch=main)](https://coveralls.io/github/snarfed/arroba?branch=master)
===

Python implementation of [Bluesky](https://blueskyweb.xyz/) [PDS](https://atproto.com/guides/data-repos) and [AT Protocol](https://atproto.com/specs/atp), including data repository, Merkle search tree, and [XRPC methods](https://atproto.com/lexicons/com-atproto-sync).

You can build your own PDS on top of arroba with just a few lines of Python and run it in any WSGI server. You can build a more involved PDS with custom logic and behavior. Or you can build a different ATProto service, eg an [AppView, relay (née BGS)](https://blueskyweb.xyz/blog/5-5-2023-federation-architecture), or something entirely new!

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
from arroba.xrpc_sync import send_events

# for Google Cloud Datastore
ndb_client = ndb.Client()

server.storage = DatastoreStorage(ndb_client=ndb_client)
server.repo.callback = lambda _: send_events()  # to subscribeRepos

app = Flask('my-pds')
init_flask(server.server, app)

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
* `RELAY_HOST`, default `bgs.bsky-sandbox.dev`
* `PLC_HOST`, default `plc.bsky-sandbox.dev`
* `PDS_HOST`, where you're running your PDS

Optional, only used in [com.atproto.repo](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_repo), [.server](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_server), and [.sync](https://arroba.readthedocs.io/en/stable/source/arroba.html#module-arroba.xrpc_sync) XRPC handlers:

* `REPO_TOKEN`, static token to use as both `accessJwt` and `refreshJwt`, defaults to contents of `repo_token` file. Not required to be an actual JWT. If not set, XRPC methods that require auth will return HTTP 501 Not Implemented.
* `ROLLBACK_WINDOW`, number of events to serve in the [`subscribeRepos` rollback window](https://atproto.com/specs/event-stream#sequence-numbers), as an integer. Defaults to no limit.
* `SUBSCRIBE_REPOS_BATCH_DELAY`, minimum time to wait between datastore queries in `com.atproto.sync.subscribeRepos`, in seconds, as a float. Defaults to 0 if unset.

<!-- Only used in app.py:
* `REPO_DID`, repo user's DID, defaults to contents of `repo_did` file
* `REPO_HANDLE`, repo user's domain handle, defaults to `did:plc:*.json` file
* `REPO_PASSWORD`, repo user's password, defaults to contents of `repo_password` file
* `REPO_PRIVKEY`, repo user's private key in PEM format, defaults to contents of `privkey.pem` file
-->


## Changelog

### 0.8 - 2025-03-13

_Breaking changes:_

* `repo`:
  * `apply_commit`, `apply_writes`: raise an exception if the repo is inactive.
* `storage`:
  * `create_repo`: remove `signing_key` and `rotation_key` kwargs, read them from input repo instead.
  * `load_repo`: don't raise an exception if the repo is tombstoned.
* `datastore_storage`:
  * Stop storing `AtpBlock.decoded` in the datastore, it's now just an in memory `@property`.
* `util`:
  * Rename `TombstonedRepo` to `InactiveRepo`.

_Non-breaking changes:_
* `datastore_storage`:
  * `DatastoreStorage`:
    * Add new `ndb_context_kwargs` constructor kwarg.
    * `apply_commit`: handle deactivated repos.
    * `create_repo`: propagate `Repo.status` into `AtpRepo`.
  * `AtpRemoteBlob`:
    * `get_or_create`: drop datastore transaction.
    * Add `width` and `height` properties, populated for images and videos, to be used in image/video embed `aspectRatio` ([snarfed/bridgy-fed#1571](https://github.com/snarfed/bridgy-fed/issues/1571)).
    * Check video length, raise `ValidationError` on [videos over 3 minutes](https://bsky.app/profile/bsky.app/post/3lk26lxn6sk2u).
* `did`:
  * Add new `get_signing_key`, `get_handle` functions.
  * `create_plc`: remove trailing slash from `services.atproto_pds.endpoint`.
* `storage`:
  * `Storage`: add new `write_blocks` method, implement in `MemoryStorage` and `DatastoreStorage`.
* `xrpc_repo`:
    * `describe_server`: include all `app.bsky` collections and others like `chat.bsky.actor.declaration`; fetch and include DID doc.
    * Implement `com.atproto.repo.importRepo`.
* `xrpc_sync`:
  * `get_blob`:
    * If we have more than one blob URL for the same CID, serve the latest one ([bridgy-fed#1650](https://github.com/snarfed/bridgy-fed/issues/1650).
    * Add HTTP `Cache-Control` to cache for 1h.
  * `list_repos`:
    * Bug fix: Use string TID for `rev`, not integer sequence number.
    * Bug fix: don't set status to `null` if the account is active.


### 0.7 - 2024-11-08

_Breaking changes:_

* Add much more lexicon schema validation for records and XRPC method input, output, and parameters.
* `storage`:
  * Switch `Storage.write` to return `Block` instead of `CID`.

_Non-breaking changes:_

* `did`:
  * Add new `update_plc` method.
  * `create_plc`: add new `also_known_as` kwarg.
  * `resolve_handle`: drop `Content-Type: text/plain` requirement for HTTPS method.
* `mst`:
  * Add new optional `start` kwarg to `load_all`.
* `repo`:
  * [Emit new #identity and #account events](https://github.com/snarfed/bridgy-fed/issues/1119) to `subscribeRepos` when creating new repos.
* `storage`:
  * Add new `deactivate_repo`, `activate_repo`, and `write_event` methods.
  * Add new optional `repo` kwarg to `read_blocks_by_seq` and `read_events_by_seq` to limit returned results to a single repo.
* `datastore_storage`:
  * Add new `max_size` and `accept_types` kwarg to `AtpRemoteBlob.get_or_create` for the blob's `maxSize` and `accept` parameters in its lexicon. If the fetched file doesn't satisfy those constraints, raises `lexrpc.ValidationError.`
  * `DatastoreStorage.read_blocks_by_seq`: use strong consistency for datastore query. May fix occasional `AssertionError` when serving `subscribeRepos`.
* `xrpc_sync`:
  * Switch `getBlob` from returning HTTP 302 to 301.
  * Implement `since` param in `getRepo`.
  * `subscribeRepos`: wait up to 60s on a skipped sequence number before giving up and emitting it as a gap.
* `util`:
  * `service_jwt`: add new `**claims` parameter for additional JWT claims, eg [`lxm`](https://github.com/bluesky-social/atproto/discussions/2687).


### 0.6 - 2024-06-24

_Breaking changes:_

* `datastore_storage`:
  * `DatastoreStorage`: add new required `ndb_client` kwarg to constructor, used to get new context in lexrpc websocket subscription handlers that run server methods like `subscribeRepos` in separate threads ([snarfed/lexrpc#8](https://github.com/snarfed/lexrpc/issues/8)).
  * `DatastoreStorage.read_blocks_by_seq`: if the ndb context gets closed while we're still running, log a warning and return. (This can happen in eg `flask_server` if the websocket client disconnects early.)
  * `AtpRemoteBlob`: if the blob URL doesn't return the `Content-Type` header, infer type from the URL, or fall back to `application/octet-stream` ([bridgy-fed#1073](https://github.com/snarfed/bridgy-fed/issues/1073)).
* `did`:
  * Cache `resolve_plc`, `resolve_web`, and `resolve_handle` for 6h, up to 5000 total results per call.
* `storage`: rename `Storage.read_commits_by_seq` to `read_events_by_seq` for new account tombstone support.
* `xrpc_sync`: rename `send_new_commits` to `send_events`, ditto.
* `xrpc_repo`: stop requiring auth for read methods: `getRecord`, `listRecords`, `describeRepo`.

_Non-breaking changes:_

* `did`:
  * Add `HANDLE_RE` regexp for handle validation.
* `storage`:
  * Add new `Storage.tombstone_repo` method, implemented in `MemoryStorage` and `DatastoreStorage`. [Used to delete accounts.](https://github.com/bluesky-social/atproto/discussions/2503#discussioncomment-9502339) ([bridgy-fed#783](https://github.com/snarfed/bridgy-fed/issues/783))
  * Add new `Storage.load_repos` method, implemented in `MemoryStorage` and `DatastoreStorage`. Used for `com.atproto.sync.listRepos`.
* `util`:
  * `service_jwt`: add optional `aud` kwarg.
* `xrpc_sync`:
  * `subscribeRepos`:
    * Add support for non-commit events, starting with account tombstones.
    * Add `ROLLBACK_WINDOW` environment variable to limit size of [rollback window](https://atproto.com/specs/event-stream#sequence-numbers). Defaults to no limit.
    * For commits with create or update operations, always include the record block, even if it already existed in the repo beforehand ([snarfed/bridgy-fed#1016](https://github.com/snarfed/bridgy-fed/issues/1016)).
    * Bug fix, populate the time each commit was created in `time` instead of the current time ([snarfed/bridgy-fed#1015](https://github.com/snarfed/bridgy-fed/issues/1015)).
  * Start serving `getRepo` queries with the `since` parameter. `since` still isn't actually implemented, but we now serve the entire repo instead of returning an error.
  * Implement `getRepoStatus` method.
  * Implement `listRepos` method.
  * `getRepo` bug fix: include the repo head commit block.
* `xrpc_repo`:
  * `getRecord`: encoded returned records correctly as [ATProto-flavored DAG-JSON](https://atproto.com/specs/data-model).
* `xrpc_*`: return `RepoNotFound` and `RepoDeactivated` errors when appropriate ([snarfed/bridgy-fed#1083](https://github.com/snarfed/bridgy-fed/issues/1083)).


### 0.5 - 2024-03-16

* Bug fix: base32-encode TIDs in record keys, `at://` URIs, commit `rev`s, etc. Before, we were using the integer UNIX timestamp directly, which happened to be the same 13 character length. Oops.
* Switch from `BGS_HOST` environment variable to `RELAY_HOST`. `BGS_HOST` is still supported for backward compatibility.
* `datastore_storage`:
  * Bug fix for `DatastoreStorage.last_seq`, handle new NSID.
  * Add new `AtpRemoteBlob` class for storing "remote" blobs, available at public HTTP URLs, that we don't store ourselves.
* `did`:
  * `create_plc`: strip padding from genesis operation signature (for [did-method-plc#54](https://github.com/did-method-plc/did-method-plc/pull/54), [atproto#1839](https://github.com/bluesky-social/atproto/pull/1839)).
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
    python -m unittest discover
    python -m unittest arroba.tests.mst_test_suite # more extensive, slower tests (deliberately excluded from autodiscovery)
    ```
1. Bump the version number in `pyproject.toml` and `docs/conf.py`. `git grep` the old version number to make sure it only appears in the changelog. Change the current changelog entry in `README.md` for this new version from _unreleased_ to the current date.
1. Build the docs. If you added any new modules, add them to the appropriate file(s) in `docs/source/`. Then run `./docs/build.sh`. Check that the generated HTML looks fine by opening `docs/_build/html/index.html` and looking around.
1. ```sh
   setenv ver X.Y
   git commit -am "release v$ver"
   ```
1. Upload to [test.pypi.org](https://test.pypi.org/) for testing.

    ```sh
    python -m build
    twine upload -r pypitest dist/arroba-$ver*
    ```
1. Install from test.pypi.org.

    ```sh
    cd /tmp
    python -m venv local
    source local/bin/activate.csh
    # make sure we force pip to use the uploaded version
    pip uninstall arroba
    pip install --upgrade pip
    pip install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple arroba==$ver
    deactivate
    ```
1. Smoke test that the code trivially loads and runs.

    ```sh
    source local/bin/activate.csh
    python

    from arroba import did
    did.resolve_handle('snarfed.org')

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
