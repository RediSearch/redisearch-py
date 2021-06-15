#!/bin/bash

set -e

cd /build
python test/test.py
python test/test_builder.py
