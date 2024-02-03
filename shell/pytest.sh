#!/usr/bin/env bash

set -ex

PYTHONPATH=. pytest "$@"
