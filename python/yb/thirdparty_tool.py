#!/usr/bin/env python

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

"""
This is a command-line tool that allows to get the download URL for a prebuilt third-party
dependencies archive for a particular configuration, as well as to update these URLs based
on the recent releases in the https://github.com/yugabyte/yugabyte-db-thirdparty repository.
"""

import sys
import re
import os
import logging
import ruamel.yaml
import argparse

from github import Github
from github.GitRelease import GitRelease

from ruamel.yaml import YAML

from typing import Dict, List, Any, Optional
from datetime import datetime

from yb.common_util import init_env, YB_SRC_ROOT, read_file
from yb.os_detection import get_short_os_name


THIRDPARTY_ARCHIVES_REL_PATH = os.path.join('build-support', 'thirdparty_archives.yml')

NUM_TOP_COMMITS = 10

DOWNLOAD_URL_PREFIX = 'https://github.com/yugabyte/yugabyte-db-thirdparty/releases/download/'
TAG_RE = re.compile(
    r'^v(?P<timestamp>[0-9]+)-'
    r'(?P<sha_prefix>[0-9a-f]+)-'
    r'(?P<os>(?:ubuntu|centos|macos|alpine)[a-z0-9.]*)'
    r'(?P<is_linuxbrew>-linuxbrew)?'  # The final question mark is important.
    r'(?P<compiler_type>-(?:gcc|clang)[a-z0-9.]+)?'
    r'$')


def get_archive_name_from_tag(tag: str) -> str:
    return f'yugabyte-db-thirdparty-{tag}.tar.gz'


class YBDependenciesRelease:

    FIELDS_TO_PERSIST = ['os_type', 'compiler_type', 'tag']

    github_release: GitRelease
    sha: str

    timestamp: str
    url: str
    compiler_type: str
    os_type: str
    tag: str

    def __init__(self, github_release: GitRelease) -> None:
        self.github_release = github_release
        self.sha = self.github_release.target_commitish

    def validate_url(self) -> None:
        asset_urls = [asset.browser_download_url for asset in self.github_release.get_assets()]
        assert(len(asset_urls) == 2)
        non_checksum_urls = [url for url in asset_urls if not url.endswith('.sha256')]
        assert(len(non_checksum_urls) == 1)
        self.url = non_checksum_urls[0]
        if not self.url.startswith(DOWNLOAD_URL_PREFIX):
            raise ValueError(
                f"Expected archive download URL to start with {DOWNLOAD_URL_PREFIX}, found "
                f"{self.url}")

        url_suffix = self.url[len(DOWNLOAD_URL_PREFIX):]
        url_suffix_components = url_suffix.split('/')
        assert(len(url_suffix_components) == 2)
        tag = url_suffix_components[0]
        archive_basename = url_suffix_components[1]
        expected_basename = get_archive_name_from_tag(tag)
        if archive_basename != expected_basename:
            raise ValueError(
                f"Expected archive name based on tag: {expected_basename}, "
                f"actual name: {archive_basename}, url: {self.url}")
        tag_match = TAG_RE.match(tag)
        if not tag_match:
            raise ValueError(f"Could not parse tag: {tag}, does not match regex: {TAG_RE}")

        group_dict = tag_match.groupdict()

        sha_prefix = tag_match.group('sha_prefix')
        if not self.sha.startswith(sha_prefix):
            raise ValueError(
                f"SHA prefix {sha_prefix} extracted from tag {tag} is not a prefix of the "
                f"SHA corresponding to the release/tag: {self.sha}.")

        self.timestamp = group_dict['timestamp']
        self.os_type = group_dict['os']
        self.is_linuxbrew = bool(group_dict.get('is_linuxbrew'))

        compiler_type = group_dict.get('compiler_type')
        if compiler_type is None and self.os_type == 'macos':
            compiler_type = 'clang'
        if compiler_type is None:
            raise ValueError(
                f"Could not determine compiler type from tag {tag}. Matches: {group_dict}.")
        compiler_type = compiler_type.strip('-')
        self.tag = tag
        self.compiler_type = compiler_type

    def as_dict(self) -> Dict[str, str]:
        return {k: getattr(self, k) for k in self.FIELDS_TO_PERSIST}

    def get_sort_key(self) -> List[str]:
        return [getattr(self, k) for k in self.FIELDS_TO_PERSIST]


class ReleaseGroup:
    sha: str
    releases: List[YBDependenciesRelease]
    creation_timestamps: List[datetime]

    def __init__(self, sha: str) -> None:
        self.sha = sha
        self.releases = []
        self.creation_timestamps = []

    def add_release(self, release: GitRelease) -> None:
        if release.target_commitish != self.sha:
            raise ValueError(
                f"Adding a release with wrong SHA. Expected: {self.sha}, got: "
                f"{release.target_commitish}.")
        self.releases.append(YBDependenciesRelease(release))
        self.creation_timestamps.append(release.created_at)

    def get_max_creation_timestamp(self) -> datetime:
        return max(self.creation_timestamps)

    def get_min_creation_timestamp(self) -> datetime:
        return min(self.creation_timestamps)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--github-token-file',
        help='Read GitHub token from this file. Authenticated requests have a higher rate limit. '
             'If this is not specified, we will still use the GITHUB_TOKEN environment '
             'variable.')
    parser.add_argument(
        '--update', '-u', action='store_true',
        help=f'Update the third-party archive metadata in in {THIRDPARTY_ARCHIVES_REL_PATH}.')
    parser.add_argument(
        '--get-sha1',
        action='store_true',
        help='Show the Git SHA1 of the commit to use in the yugabyte-db-thirdparty repo '
             'in case we are building the third-party dependencies from scratch.')
    parser.add_argument(
        '--save-download-url-to-file',
        help='Determine the third-party archive download URL for the combination of criteria, '
             'including the compiler type, and write it to the file specified by this argument.')
    parser.add_argument(
        '--compiler-type',
        help='Compiler type, to help us decide which third-party archive to choose. '
             'The default value is determined by the YB_COMPILER_TYPE environment variable.',
        default=os.getenv('YB_COMPILER_TYPE'))

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args()


def get_archive_metadata_file_path() -> str:
    return os.path.join(YB_SRC_ROOT, THIRDPARTY_ARCHIVES_REL_PATH)


def get_github_token(token_file_path: Optional[str]) -> Optional[str]:
    github_token: Optional[str]
    if token_file_path:
        github_token = read_file(token_file_path).strip()
    else:
        github_token = os.getenv('GITHUB_TOKEN')
    if github_token is None:
        return github_token

    if len(github_token) != 40:
        raise ValueError(f"Invalid GitHub token length: {len(github_token)}, expected 40.")
    return github_token


def get_yaml() -> YAML:
    yaml = ruamel.yaml.YAML()
    yaml.indent(sequence=4, offset=2)
    return yaml


def update_archive_metadata_file(github_token_file: Optional[str]) -> None:
    archive_metadata_path = get_archive_metadata_file_path()
    logging.info(f"Updating third-party archive metadata file in {archive_metadata_path}")

    github_client = Github(get_github_token(github_token_file))
    repo = github_client.get_repo('yugabyte/yugabyte-db-thirdparty')

    releases_by_commit: Dict[str, ReleaseGroup] = {}
    for release in repo.get_releases():
        sha: str = release.target_commitish
        assert(isinstance(sha, str))
        if sha not in releases_by_commit:
            releases_by_commit[sha] = ReleaseGroup(sha)
        releases_by_commit[sha].add_release(release)

    latest_group_by_max = max(
        releases_by_commit.values(), key=ReleaseGroup.get_max_creation_timestamp)
    latest_group_by_min = max(
        releases_by_commit.values(), key=ReleaseGroup.get_min_creation_timestamp)
    if latest_group_by_max is not latest_group_by_min:
        raise ValueError(
            "Overlapping releases for different commits. No good way to identify latest release: "
            f"e.g. {latest_group_by_max.sha} and {latest_group_by_min.sha}.")

    latest_group = latest_group_by_max

    sha = latest_group.sha
    logging.info(
        f"Latest released yugabyte-db-thirdparty commit: f{sha}. "
        f"Released at: {latest_group.get_max_creation_timestamp()}.")

    new_metadata: Dict[str, Any] = {
        'sha': sha,
        'archives': []
    }
    releases_for_one_commit = latest_group.releases
    for yb_thirdparty_release in releases_for_one_commit:
        yb_thirdparty_release.validate_url()

    releases_for_one_commit.sort(key=YBDependenciesRelease.get_sort_key)

    for yb_thirdparty_release in releases_for_one_commit:
        new_metadata['archives'].append(yb_thirdparty_release.as_dict())

    yaml = get_yaml()
    with open(archive_metadata_path, 'w') as archive_metadata_file:
        yaml.dump(new_metadata, archive_metadata_file)
    logging.info(
        f"Wrote information for {len(releases_for_one_commit)} pre-built yugabyte-db-thirdparty "
        f"archives to {archive_metadata_path}.")


def load_metadata() -> Dict[str, Any]:
    yaml = get_yaml()
    with open(get_archive_metadata_file_path()) as archive_metadata_file:
        return yaml.load(archive_metadata_file)


def get_download_url(metadata: Dict[str, Any], compiler_type: str) -> str:
    short_os_name = get_short_os_name()

    candidates: List[str] = []
    for archive in metadata['archives']:
        if archive['os_type'] == short_os_name and archive['compiler_type'] == compiler_type:
            candidates.append(archive)

    if len(candidates) == 1:
        tag = candidates[0]['tag']
        return f'{DOWNLOAD_URL_PREFIX}{tag}/{get_archive_name_from_tag(tag)}'

    if not candidates:
        if short_os_name == 'centos7' and compiler_type == 'gcc':
            logging.info(
                "Assuming that the compiler type of 'gcc' means 'gcc5'. "
                "This will change when we update the compiler.")
            return get_download_url(metadata, 'gcc5')

        raise ValueError(
            f"Could not find a third-party release archive to download for OS type "
            f"{short_os_name} and compiler type {compiler_type}.")

    raise ValueError(
        f"Found too many third-party release archives to download for OS type "
        f"{short_os_name} and compiler type {compiler_type}: {candidates}.")


def main() -> None:
    init_env(verbose=False)
    args = parse_args()
    if args.update:
        update_archive_metadata_file(args.github_token_file)
        return
    metadata = load_metadata()
    if args.get_sha1:
        print(metadata['sha'])
        return

    if args.save_download_url_to_file:
        if not args.compiler_type:
            raise ValueError("Compiler type not specified")
        url = get_download_url(metadata, args.compiler_type)
        if url is None:
            raise RuntimeError("Could not determine download URL")
        logging.info(f"Download URL for the third-party dependencies: {url}")
        with open(args.save_download_url_to_file, 'w') as output_file:
            output_file.write(url)


if __name__ == '__main__':
    main()
