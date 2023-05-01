arroba [![Circle CI](https://circleci.com/gh/snarfed/arroba.svg?style=svg)](https://circleci.com/gh/snarfed/arroba) [![Coverage Status](https://coveralls.io/repos/github/snarfed/arroba/badge.svg?branch=main)](https://coveralls.io/github/snarfed/arroba?branch=master)
===

Python implementation of [Bluesky](https://blueskyweb.xyz/)'s low level [AT Protocol](https://atproto.com/), including [data repository](https://atproto.com/guides/data-repos), [Merkle search tree](https://atproto.com/guides/data-repos), and [`com.atproto.sync` XRPC methods](https://atproto.com/lexicons/com-atproto-sync).

_Arroba_ is the Spanish word for the [`@` character, ie "at sign."](https://en.wikipedia.org/wiki/At_sign)

Install from [PyPI](https://pypi.org/project/arroba/) with `pip install arroba`.

License: This project is placed into the public domain.

* [Usage](#usage)
* [Release instructions](#release-instructions)
* [Changelog](#changelog)


## Usage

TODO


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
    pip3 uninstall arroba # make sure we force pip to use the uploaded version
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
    twine upload dist/arroba-$ver.tar.gz dist/arroba-$ver-py3-none-any.whl
    ```
1. [Wait for the docs to build on Read the Docs](https://readthedocs.org/projects/arroba/builds/), then check that they look ok.
1. On the [Versions page](https://readthedocs.org/projects/arroba/versions/), check that the new version is active, If it's not, activate it in the _Activate a Version_ section.


## Changelog

### 0.1 - unreleased

Initial release! Still very in progress.
