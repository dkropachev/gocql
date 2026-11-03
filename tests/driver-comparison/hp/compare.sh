#!/bin/sh
# usage: HP_DIR=... compare.sh OLD NEW [ROUNDS]
# Builds NEW from the working tree (OLD must already be built), runs ABBA
# rounds (default 5 => 10 samples per side) and prints benchstat OLD vs NEW.
set -eu
. "$(dirname "$0")/env.sh"
old=$1 new=$2 rounds=${3:-5}
[ -x "$HP_GATES/$old.root.test" ] || { echo "missing $old binaries"; exit 1; }
hp_build "$new"
: >"$HP_GATES/$old.cmp.txt"; : >"$HP_GATES/$new.cmp.txt"
i=0; while [ $i -lt "$rounds" ]; do
	for l in $old $new $new $old; do hp_round $l "$HP_GATES/$l.cmp.txt"; done
	i=$((i + 1))
done
cd "$HP_GATES" && benchstat "$old=$old.cmp.txt" "$new=$new.cmp.txt"
