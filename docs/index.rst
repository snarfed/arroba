arroba
------

Python implementation of `Bluesky <https://blueskyweb.xyz/>`__
`PDS <https://atproto.com/guides/data-repos>`__ and `AT
Protocol <https://atproto.com/specs/atp>`__, including data repository,
Merkle search tree, and `com.atproto.sync XRPC
methods <https://atproto.com/lexicons/com-atproto-sync>`__.

*Arroba* is the Spanish word for the `@
character <https://en.wikipedia.org/wiki/At_sign>`__ (“at sign”).

Install from `PyPI <https://pypi.org/project/arroba/>`__ with
``pip install arroba``.

License: This project is placed into the public domain.

-  `Usage <#usage>`__
-  `Changelog <#changelog>`__
-  `Release instructions <#release-instructions>`__

Usage
-----

See `app.py <https://github.com/snarfed/arroba/blob/main/app.py>`__
for the minimal wrapper code needed to run a fully functional PDS based
on arroba, for testing with the `ATProto federation
sandbox <https://atproto.com/blog/federation-developer-sandbox>`__.

Environment variables:

-  ``APPVIEW_HOST``, default ``api.bsky-sandbox.dev``
-  ``BGS_HOST``, default ``bgs.bsky-sandbox.dev``
-  ``PLC_HOST``, default ``plc.bsky-sandbox.dev``
-  ``PDS_HOST``, where you’re running your PDS
-  ``REPO_DID``, repo user’s DID, defaults to contents of ``repo_did``
   file
-  ``REPO_HANDLE``, repo user’s domain handle, defaults to
   ``did:plc:*.json`` file
-  ``REPO_PASSWORD``, repo user’s password, defaults to contents of
   ``repo_password`` file
-  ``REPO_PRIVKEY``, repo user’s private key in PEM format, defaults to
   contents of ``privkey.pem`` file
-  ``REPO_TOKEN``, static token to use as both ``accessJwt`` and
   ``refreshJwt``, defaults to contents of ``repo_token`` file. Not
   required to be an actual JWT.

More docs to come!

Changelog
---------

0.4 - 2023-09-19
~~~~~~~~~~~~~~~~

-  Migrate to `ATProto repo
   v3 <https://atproto.com/blog/repo-sync-update>`__. Specifically, the
   existing ``subscribeRepos`` sequence number is reused as the new
   ``rev`` field in commits.
   (`Discussion. <https://github.com/bluesky-social/atproto/discussions/1607>`__).
-  Add new ``did`` module with utilities to create and resolve
   ``did:plc``\ s and resolve ``did:web``\ s.
-  Add new ``util.service_jwt`` function that generates `ATProto
   inter-service
   JWTs <https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification>`__.
-  ``Repo``:

   -  Add new ``signing_key``/``rotation_key`` attributes. Generate
      store, and load both in ``datastore_storage``.
   -  Remove ``format_init_commit``, migrate existing calls to
      ``format_commit``.

-  ``Storage``:

   -  Rename ``read_from_seq`` => ``read_blocks_by_seq`` (and in
      ``MemoryStorage`` and ``DatastoreStorage``), add new
      ``read_commits_by_seq`` method.
   -  Merge ``load_repo`` ``did``/``handle`` kwargs into
      ``did_or_handle``.

-  XRPCs:

   -  Make ``subscribeRepos`` check storage for all new commits every
      time it wakes up.

      -  As part of this, replace ``xrpc_sync.enqueue_commit`` with new
         ``send_new_commits`` function that takes no parameters.

   -  Drop bundled ``app.bsky``/``com.atproto`` lexicons, use
      `lexrpc <https://lexrpc.readthedocs.io/>`__\ ’s instead.

.. _section-1:

0.3 - 2023-08-29
~~~~~~~~~~~~~~~~

Big milestone: arroba is successfully federating with the `ATProto
sandbox <https://atproto.com/blog/federation-developer-sandbox>`__! See
`app.py <https://github.com/snarfed/arroba/blob/main/app.py>`__ for the
minimal demo code needed to wrap arroba in a fully functional PDS.

-  Add Google Cloud Datastore implementation of repo storage.
-  Implement ``com.atproto`` XRPC methods needed to federate with
   sandbox, including most of ``repo`` and ``sync``.

   -  Notably, includes ``subscribeRepos`` server side over websocket.

-  …and much more.

.. _section-2:

0.2 - 2023-05-18
~~~~~~~~~~~~~~~~

Implement repo and commit chain in new Repo class, including pluggable
storage. This completes the first pass at all PDS data structures. Next
release will include initial implementations of the
``com.atproto.sync.*`` XRPC methods.

.. _section-3:

0.1 - 2023-04-30
~~~~~~~~~~~~~~~~

Initial release! Still very in progress. MST, Walker, and Diff classes
are mostly complete and working. Repo, commits, and sync XRPC methods
are still in progress.

Release instructions
--------------------

Here’s how to package, test, and ship a new release.

1.  Run the unit tests.

    .. code:: sh

       source local/bin/activate.csh
       python3 -m unittest discover

2.  Bump the version number in ``pyproject.toml`` and ``docs/conf.py``.
    ``git grep`` the old version number to make sure it only appears in
    the changelog. Change the current changelog entry in ``README.md``
    for this new version from *unreleased* to the current date.

3.  Build the docs. If you added any new modules, add them to the
    appropriate file(s) in ``docs/source/``. Then run
    ``./docs/build.sh``. Check that the generated HTML looks fine by
    opening ``docs/_build/html/index.html`` and looking around.

4.  ``git commit -am 'release vX.Y'``

5.  Upload to `test.pypi.org <https://test.pypi.org/>`__ for testing.

    .. code:: sh

       python3 -m build
       setenv ver X.Y
       twine upload -r pypitest dist/arroba-$ver*

6.  Install from test.pypi.org.

    .. code:: sh

       cd /tmp
       python3 -m venv local
       source local/bin/activate.csh
       # make sure we force pip to use the uploaded version
       pip3 uninstall arroba
       pip3 install --upgrade pip
       pip3 install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple arroba==$ver
       deactivate

7.  Smoke test that the code trivially loads and runs.

    .. code:: sh

       source local/bin/activate.csh
       python3
       # TODO: test code
       deactivate

8.  Tag the release in git. In the tag message editor, delete the
    generated comments at bottom, leave the first line blank (to omit
    the release “title” in github), put ``### Notable changes`` on the
    second line, then copy and paste this version’s changelog contents
    below it.

    .. code:: sh

       git tag -a v$ver --cleanup=verbatim
       git push && git push --tags

9.  `Click here to draft a new release on
    GitHub. <https://github.com/snarfed/arroba/releases/new>`__ Enter
    ``vX.Y`` in the *Tag version* box. Leave *Release title* empty. Copy
    ``### Notable changes`` and the changelog contents into the
    description text box.

10. Upload to `pypi.org <https://pypi.org/>`__!

    .. code:: sh

       twine upload dist/arroba-$ver*

11. `Wait for the docs to build on Read the
    Docs <https://readthedocs.org/projects/arroba/builds/>`__, then
    check that they look ok.

12. On the `Versions
    page <https://readthedocs.org/projects/arroba/versions/>`__, check
    that the new version is active, If it’s not, activate it in the
    *Activate a Version* section.
