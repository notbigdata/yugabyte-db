# Copyright (c) Yugabyte, Inc.

import os
import logging

from yb.common_util import YB_SRC_ROOT, read_json_file


# We build PostgreSQL code in a separate directory (postgres_build) rsynced from the source tree to
# support out-of-source builds. Then, after generating the compilation commands, we rewriten them
# to work with original files (in src/postgres) so that CLangd can use them.

# We will write the list of raw compile_commands.json files before the above rewrite to this file.
# We need these files to run the PVS Studio analyzer on them.
RAW_COMPILE_COMMANDS_PATHS_FILE_NAME = 'raw_compile_commands_files.json'

# The "combined compilation commands" file contains all compilation commands for C++ code and
# PostgreSQL C code. This is the file that is symlinked in the YugabyteDB source root directory and
# used with Clangd.
COMBINED_COMPILE_COMMANDS_FILE_NAME = 'combined_compile_commands.json'


def create_compile_commands_symlink(combined_compile_commands_path, build_type):
    dest_link_path = os.path.join(YB_SRC_ROOT, 'compile_commands.json')
    if build_type != 'compilecmds':
        logging.info("Not creating a symlink at %s for build type %s",
                     dest_link_path, build_type)
        return

    if (not os.path.exists(dest_link_path) or
            os.path.realpath(dest_link_path) != os.path.realpath(combined_compile_commands_path)):
        if os.path.exists(dest_link_path):
            logging.info("Removing the old file/link at %s", dest_link_path)
            os.remove(dest_link_path)

        os.symlink(
            os.path.relpath(
                os.path.realpath(combined_compile_commands_path),
                os.path.realpath(YB_SRC_ROOT)),
            dest_link_path)
        logging.info("Created symlink at %s", dest_link_path)


def read_raw_compile_commands_file_paths(build_root):
    return read_json_file(os.path.join(build_root, RAW_COMPILE_COMMANDS_PATHS_FILE_NAME))
