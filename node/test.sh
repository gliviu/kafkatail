#!/usr/bin/env bash

ACTUAL="$(kafkatail -v)"
EXPECTED="kafkatail $appversion"
if [[ $ACTUAL == $EXPECTED ]]; then echo PASSED; else echo FAILED: expected - "$EXPECTED", actual: "$ACTUAL"; exit 1; fi;
