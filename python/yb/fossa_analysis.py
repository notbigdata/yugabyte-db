#!/usr/bin/env python3
# Copyright (c) Yugabyte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

import os
import yaml
import subprocess

from yb.common_util import YB_SRC_ROOT, get_thirdparty_dir, get_download_cache_dir

from downloadutil.downloader import Downloader
from downloadutil.download_config import DownloadConfig


def main():
    parser = argparse.ArgumentParser(
        description='Run FOSSA analysis (open source license compliance).')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    download_config = DownloadConfig(
        verbose=True,
        cache_dir_path=get_download_cache_dir()
    )
    downloader = Downloader(download_config)

    fossa_yml_path = os.path.join(YB_SRC_ROOT, '.fossa.yml')
    with open(fossa_yml_path) as fossa_yml_file:
        fossa_yml_data = yaml.safe_load(fossa_yml_file)
    thirdparty_dir = get_thirdparty_dir()



if __name__ == '__main__':
    main()
