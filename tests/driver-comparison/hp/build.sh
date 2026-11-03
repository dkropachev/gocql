#!/bin/sh
# usage: HP_DIR=... build.sh LABEL  -- builds gate binaries for the working tree.
set -eu
. "$(dirname "$0")/env.sh"
hp_build "$1"
