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

TODO

Changelog
---------

0.2 - 2023-05-18
~~~~~~~~~~~~~~~~

Implement repo and commit chain in new Repo class, including pluggable
storage. This completes the first pass at all PDS data structures. Next
release will include initial implementations of the
``com.atproto.sync.*`` XRPC methods.

.. _section-1:

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
       pip3 uninstall arroba # make sure we force pip to use the uploaded version
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

       twine upload dist/arroba-$ver.tar.gz dist/arroba-$ver-py3-none-any.whl

11. `Wait for the docs to build on Read the
    Docs <https://readthedocs.org/projects/arroba/builds/>`__, then
    check that they look ok.

12. On the `Versions
    page <https://readthedocs.org/projects/arroba/versions/>`__, check
    that the new version is active, If it’s not, activate it in the
    *Activate a Version* section.
