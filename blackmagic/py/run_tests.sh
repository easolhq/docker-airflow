#!/usr/bin/env bash

export NODE_PATH="${VIRTUAL_ENV}/lib/node_modules"
nose2 -C -v
