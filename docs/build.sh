#!/bin/bash
#
# Preprocesses docs and runs Sphinx (apidoc and build) to build the HTML docs.
#
# Requires:
#  brew install pandoc
#  pip install sphinx sphinx_rtd_theme  (in virtualenv)
set -e

absfile=`readlink -f $0`
cd `dirname $absfile`

# generates the module index file docs/source/arroba.rst, only used to
# bootstrap. we edit by hand since then so don't run any more or it will
# overwrite it.
# sphinx-apidoc -f -o source ../arroba ../arroba/tests

rm -f index.rst
cat > index.rst <<EOF
arroba
------

EOF

tail -n +4 ../README.md \
  | pandoc --from=markdown --to=rst \
  | sed -E 's/```/`/; s/`` </ </' \
  >> index.rst

source ../local/bin/activate

# Run sphinx in the virtualenv's python interpreter so it can import packages
# installed in the virtualenv.
python3 `which sphinx-build` -b html . _build/html
