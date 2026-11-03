#!/bin/sh
# usage: HP_DIR=... [HP_REF=commit] build.sh LABEL  -- builds gate binaries.
set -eu
. "$(dirname "$0")/env.sh"
hp_build "$1"
