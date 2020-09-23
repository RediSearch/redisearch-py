#!/bin/bash

set -e

if [[ $PYTHON_VER == 2 ]]; then
	PYTHON=python2
else
	PYTHON=python3
fi

$PYTHON --version

cd /build
redis-server --loadmodule /usr/lib/redis/modules/redisearch.so &
sleep 1
$PYTHON test/test.py
$PYTHON test/test_builder.py
