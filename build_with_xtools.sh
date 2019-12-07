#!/usr/bin/env bash

export YB_LINUXBREW_DIR=/u/users/mbautin/x-tools/x86_64-unknown-linux-gnu
export PATH=$YB_LINUXBREW_DIR/bin:$PATH
export YB_THIRDPARTY_DIR=$HOME/code/yugabyte-db-thirdparty2
./yb_build.sh "$@" 2>&1 | tee ~/yb_build_with_xtools.log
