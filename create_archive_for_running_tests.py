#!/usr/bin/env python

import subprocess
import glob
import os


def main():
    build_dir = 'build/release-gcc-dynamic-ninja'
    dest_file = os.path.join(build_dir, 'archive_for_tests_on_spark.tar.gz')
    if os.path.exists(dest_file):
        os.remove(dest_file)
    tar_args = [
        'tar',
        'cvzf',
        dest_file 
    ]
    dirs_relative_to_build = [
        'version_metadata.json',
        'lib',
        'bin',
        'postgres',
        'ent',
        'share',
    ]
    tar_args.extend([os.path.join(build_dir, d) for d in dirs_relative_to_build])
    tar_args.extend(sorted(glob.glob(os.path.join(build_dir, 'tests-*'))))

    tar_args.extend([
        'www',
        'build-support',
        'python',
        'java',
        'submodules',
        'version.txt',
        'managed/version.txt',
        'yb_build.sh',
        'bin',
        'managed/src/main/resources/version.txt'
    ])
    print(tar_args)
    subprocess.check_call(tar_args)
    
if __name__ == '__main__':
    main()
