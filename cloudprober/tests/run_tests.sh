#!/bin/bash

cd "$(dirname "$0")"

cp test_requirements.txt ../requirements.txt

../setup.sh

python -m unittest discover -p '*_test.py'

# Cleanup
rm ./*.pyc
rm ../requirements.txt
rm -r ../google

