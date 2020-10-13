#!/bin/bash

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)

cd $ROOT

(mkdir -p deps; cd deps; git clone https://github.com/RedisLabsModules/readies.git)

if [ "$PYTHON_VER" = 2 ]; then
	PIP=1 VENV=1 ./deps/readies/bin/getpy2
	python2 -m virtualenv venv2
	. ./venv2/bin/activate
else
	PIP=1 VENV=1 ./deps/readies/bin/getpy3
	python3 -m virtualenv venv3
	. ./venv3/bin/activate
fi

python -m pip install -r requirements.txt
python -m pip install --force-reinstall git+https://github.com/RedisLabs/rmtest.git
