#!/usr/bin/env python3

import os
import logging
import subprocess
import argparse
import re
import sys
import atexit

from typing import List

from yb.common_util import \
    ensure_file_exists, \
    ensure_directory_exists, \
    is_macos, \
    format_bash_command
from yb.dedup_thread_stacks import dedup_thread_stack_line_iterator

# Example message on macOS:
# *** SIGILL (@0x10bfd4c14) received by PID 8848 (TID 0x10f1335c0) stack trace: ***

# Example message on Linux:
# *** SIGSEGV (@0x0) received by PID 32218 (TID 0x7f8544027980) from PID 0; stack trace: ***

# Regex passed to grep -E
GREP_E_REGEX = 'SIG[A-Z]+ .* received by PID [0-9]+ '

# Characters allowed in a Linux core pattern:
#
# %%  A single % character.
# %c  Core file size soft resource limit of crashing process (since
#     Linux 2.6.24).
# %d  Dump mode—same as value returned by prctl(2) PR_GET_DUMPABLE
#     (since Linux 3.7).
# %e  The process or thread's comm value, which typically is the
#     same as the executable filename (without path prefix, and
#     truncated to a maximum of 15 characters), but may have been
#     modified to be something different; see the discussion of
#     /proc/[pid]/comm and /proc/[pid]/task/[tid]/comm in proc(5).
# %E  Pathname of executable, with slashes ('/') replaced by
#     exclamation marks ('!') (since Linux 3.0).
# %g  Numeric real GID of dumped process.
# %h  Hostname (same as nodename returned by uname(2)).
# %i  TID of thread that triggered core dump, as seen in the PID
#     namespace in which the thread resides (since Linux 3.18).
# %I  TID of thread that triggered core dump, as seen in the
#     initial PID namespace (since Linux 3.18).
# %p  PID of dumped process, as seen in the PID namespace in which
#     the process resides.
# %P  PID of dumped process, as seen in the initial PID namespace
#     (since Linux 3.12).
# %s  Number of signal causing dump.
# %t  Time of dump, expressed as seconds since the Epoch,
#     1970-01-01 00:00:00 +0000 (UTC).
# %u  Numeric real UID of dumped process.

# We replace everything except %p with *, and then replace %p with the pid we're looking for.
LINUX_CORE_PATTERN_PARTS_TO_REPLACE_WITH_ASTERISK_RE = re.compile(r'%[cdeEghiIPstu]')

g_files_to_delete_at_exit = []


def delete_files_at_exit_callback():
    """
    This runs at the end of the script to delete the files that we decided to delete.
    """
    for file_path in g_files_to_delete_at_exit:
        logging.info("Deleting file: %s", file_path)
        try:
            os.remove(file_path)
        except IOError as ex:
            logging.info("Failed deleting file %s: %s", file_path, ex)


def defer_file_deletion(file_path):
    global g_files_to_delete_at_exit
    g_files_to_delete_at_exit.append(file_path)


g_linux_core_pattern = None


def get_linux_core_pattern():
    global g_linux_core_pattern
    if g_linux_core_pattern is not None:
        return g_linux_core_pattern

    sysctl_path = '/sbin/sysctl'
    core_pattern_file_path = '/proc/sys/kernel/core_pattern'

    core_pattern = None
    if os.path.exists(sysctl_path):
        get_core_pattern_cmd = [sysctl_path, '--values', 'kernel.core_pattern']
        try:
            core_pattern = subprocess.check_output(get_core_pattern_args)
        except Exception as ex:
            logging.info(
                "Failed to get core pattern using command: %s",
                format_bash_command(get_core_pattern_cmd))

    if not core_pattern and os.path.exists(core_pattern_file_path):
        with open(core_pattern_file_path) as core_pattern_file:
            core_pattern = core_pattern_file.read().strip()

    if not core_pattern:
        raise IOError(
            "Failed to determine core pattern, either by running sysctl or reading from %s",
            core_pattern_file_path)

    g_linux_core_pattern = core_pattern


def get_core_file_glob(pid: int) -> str:
    """
    Transforming core pattern to a glob-style wildcard so we can search for core files generated
    by a specific pid.

    """
    core_pattern = get_linux_core_pattern()
    default_core_dir = os.environ.get('TEST_TMPDIR', os.getcwd())
    return LINUX_CORE_PATTERN_PARTS_TO_REPLACE_WITH_ASTERISK_RE.sub(
        '*', core_pattern
    ).replace('%%', '%').replace('%p', str(pid))


class DebuggerInvocation:
    core_path: str
    executable_path: str
    thread_id: str

    def __init__(self, core_path, executable_path):
        self.core_path = core_path
        self.executable_path = executable_path

        if is_macos():
            self.debugger_cmd_args = [
                'lldb',
                self.executable_path,
                '-c',
                self.core_path
            ]
            self.debugger_input = 'thread backtrace all'
        else:
            self.debugger_cmd_args = [
                'gdb', '-q', '-n',
                '-ex', 'bt',
                '-ex', 'thread apply all bt',
                '-batch', self.executable_path, self.core_path
            ]

            self.debugger_input = ''

    def invoke(self, file_path_to_append_to=None):
        ensure_file_exists(self.core_path)
        ensure_file_exists(self.executable_path)

        # The core might have been generated by yb-master or yb-tserver launched as part of an
        # ExternalMiniCluster.
        #
        # TODO(mbautin): don't run gdb/lldb the second time in case we get the binary name right the
        #                first time around.

        output_lines = [
            "",
            "Found a core file at '%s'." % self.core_path,
            "Analyzing using the command: %s" % format_bash_command(self.debugger_cmd_args)]

        debugger_process = subprocess.Popen(
            self.debugger_cmd_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        (stdout_bytes, stderr_bytes) = debugger_process.communicate(
            input=self.debugger_input.encode('utf-8'))
        stdout_str = stdout_bytes.decode('utf-8').rstrip()
        stderr_str = stderr_bytes.decode('utf-8').rstrip()

        if debugger_process.returncode != 0:
            output_lines.append("Debugger exited with code %d", debugger_process.returncode)

        if stderr_str.strip():
            output_lines.append("Standard error output from the debugger:")
            output_lines.extend(stderr_str.split("\n"))

        output_lines.append("Standard output from the debugger (deduplicated stacks):")
        output_lines.extend(dedup_thread_stack_line_iterator(stdout_str.split("\n")))
        output_lines.append("")
        all_lines_str = "\n".join(output_lines)
        if file_path_to_append_to is not None:
            with open(file_path_to_append_to, 'a') as file_to_append:
                file_to_append.write(all_lines_str)
        sys.stdout.write(all_lines_str)


class CoreFileAnalyzer:
    rel_test_binary: str
    core_path: str
    executable_path: str
    debugger_cmd: str
    debugger_input: List[str]

    def __init__(self):
        pass

    def parse_args(self):
        pass


#     def analyze_existing_core_file(self) -> None:
#         """
# analyze_existing_core_file() {
#   expect_num_args 2 "$@"
#   local core_path=$1
#   local executable_path=$2
#   ensure_file_exists "$core_path"
#   ensure_file_exists "$executable_path"
#   ( echo "Found a core file at '$core_path', backtrace:" | tee -a "$append_output_to" ) >&2

#   # The core might have been generated by yb-master or yb-tserver launched as part of an
#   # ExternalMiniCluster.
#   # TODO(mbautin): don't run gdb/lldb the second time in case we get the binary name right the
#   #                first time around.

#   local debugger_cmd debugger_input

#   set_debugger_input_for_core_stack_trace
#   local debugger_output=$( echo "$debugger_input" | "${debugger_cmd[@]}" 2>&1 )
#   if [[ "$debugger_output" =~ Core\ was\ generated\ by\ .(yb-master|yb-tserver) ]]; then
#     # The test program may be launching masters and tablet servers through an ExternalMiniCluster,
#     # and the core file was generated by one of those masters and tablet servers.
#     executable_path="$BUILD_ROOT/bin/${BASH_REMATCH[1]}"
#     set_debugger_input_for_core_stack_trace
#   fi

#   set +e
#   (
#     set -x
#     echo "$debugger_input" |
#       "${debugger_cmd[@]}" 2>&1 |
#       grep -Ev "^\[New LWP [0-9]+\]$" |
#       "$YB_SRC_ROOT"/build-support/dedup_thread_stacks.py |
#       tee -a "$append_output_to"
#   ) >&2
#   set -e
#   echo >&2
#   echo >&2
# }

#     """

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description='Analyze a core file from a unit test')
        parser.add_argument(
            '--build-root',
            required=True,
            help='Build root (e.g. ~/code/yugabyte/build/debug-gcc-dynamic-community)')
        parser.add_argument(
            '--rel-test-binary',
            required=True,
            help='Test binary path relative to the build root')
        parser.add_argument(
            '--core-path',
            help='Core file path to analyze')
        parser.add_argument(
            '--test-log-path',
            required=True,
            help='If this is defined and is not empty, any output generated by this function is '
                 'appended to the file at this path. In addition, we may use this log file to '
                 'determine the pid of the process that generated the core.')
        args = parser.parse_args()
        self.build_root = args.build_root
        ensure_directory_exists(self.build_root)

        self.core_path = args.core_path
        self.rel_test_binary = args.rel_test_binary
        self.test_log_path = args.test_log_path

    def process_core_file(self):

        """
# Looks for a core dump file in the specified directory, shows a stack trace using gdb, and removes
# the core dump file to save space.
# Inputs:
#   rel_test_binary
#     Test binary path relative to $BUILD_ROOT
#   core_dir
#     The directory to look for a core dump file in.
#   test_log_path
#     If this is defined and is not empty, any output generated by this function is appended to the
#     file at this path.
process_core_file() {
  expect_num_args 0 "$@"
  expect_vars_to_be_set \
    rel_test_binary
  local abs_test_binary_path=$BUILD_ROOT/$rel_test_binary

  local append_output_to=/dev/null
  if [[ -n ${test_log_path:-} ]]; then
    append_output_to=$test_log_path
  fi

  local thread_for_backtrace="all"
  if is_mac; then
    # Finding core files on Mac OS X.
    if [[ -f ${test_log_path:-} ]]; then
      set +e
      local signal_received_line=$(
        egrep 'SIG[A-Z]+.*received by PID [0-9]+' "$test_log_path" | head -1
      )
      set -e
      local core_pid=""
      # Parsing a line like this:
      # *** SIGSEGV (@0x0) received by PID 46158 (TID 0x70000028d000) stack trace: ***
      if [[ $signal_received_line =~ received\ by\ PID\ ([0-9]+)\ \(TID\ (0x[0-9a-fA-F]+)\) ]]; then
        core_pid=${BASH_REMATCH[1]}
        # TODO: find a way to only show the backtrace of the relevant thread.  We can't just pass
        # the thread id to "thread backtrace", apparently it needs the thread index of some sort
        # that is not identical to thread id.
        # thread_for_backtrace=${BASH_REMATCH[2]}
      fi
      if [[ -n $core_pid ]]; then
        local core_path=$core_dir/core.$core_pid
        log "Determined core_path=$core_path"
        if [[ ! -f $core_path ]]; then
          log "Core file not found at presumed location '$core_path'."
        fi
      else
        if egrep -q 'SIG[A-Z]+.*received by PID' "$test_log_path"; then
          log "Warning: could not determine test pid from '$test_log_path'" \
              "(matched log line: $signal_received_line)"
        fi
        return
      fi
    else
      log "Warning: test_log_path is not set, or file does not exist ('${test_log_path:-}')," \
          "cannot look for core files on Mac OS X"
      return
    fi
  else
    # Finding core files on Linux.
    if [[ ! -d $core_dir ]]; then
      echo "$FUNCNAME: directory '$core_dir' does not exist" >&2
      exit 1
    fi
    local core_pattern
    if [[ -x /sbin/sysctl ]]; then
      core_pattern=$( /sbin/sysctl --values kernel.core_pattern )
    fi

    local core_path=$core_dir/core
    if [[ ! -f $core_path ]]; then
      # If there is just one file named "core.[0-9]+" in the core directory, pick it up and assume
      # it belongs to the test. This is necessary on some systems, e.g. CentOS workstations with no
      # special core location overrides.
      local core_candidates=()
      for core_path in "$core_dir"/core.*; do
        if [[ -f $core_path && $core_path =~ /core[.][0-9]+$ ]]; then
          core_candidates+=( "$core_path" )
        fi
      done
      if [[ ${#core_candidates[@]} -eq 1 ]]; then
        core_path=${core_candidates[0]}
      elif [[ ${#core_candidates[@]} -gt 1 ]]; then
        log "Found too many core files in '$core_dir', not analyzing: ${core_candidates[@]}"
        return
      fi
    fi
  fi

  if [[ -f $core_path ]]; then
    local core_binary_path=$abs_test_binary_path
    analyze_existing_core_file "$core_path" "$core_binary_path"
    rm -f "$core_path"  # to save disk space
  fi
}
        """
        assert self.rel_test_binary is not None
        abs_test_binary_path = os.path.join(self.build_root, self.rel_test_binary)

        append_output_to = self.test_log_path

        self.thread_for_backtrace = 'all'
        debugger_invocations = []
        if not os.path.exists(self.test_log_path):
            logging.info(
                "Test log does not exist, cannot find pids of failed processes: %s",
                self.test_log_path)
            return

        grep_cmd_args = [
            'grep',
              '-Eo',
              GREP_E_REGEX,
              self.test_log_path]
        grep_process = subprocess.Popen(
            grep_cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        (grep_out_bytes, grep_err_bytes) = grep_process.communicate()
        grep_out_str = grep_out_bytes.decode('utf-8')
        grep_err_str = grep_err_bytes.decode('utf-8')
        if grep_process.returncode not in [0, 1]:
            raise ValueError(
                "Unexpected return code from the grep command: %d. "
                "Command: %s. "
                "Standard error:\n%s\n(end of standard error)" % (
                    grep_process.returncode, grep_cmd_args, grep_err_str))

        for line in grep_out_str.split('\n'):
            line = line.strip()
            if not line:
                continue
            line_parts = line.split()
            if not line_parts:
                continue
            core_pid_str = line_parts[-1]
            try:
                core_pid = int(core_pid_str)
            except ValueError as int_parsing_ex:
                logging.info(
                    "Failed to parse '%s' as integer from line: [[ %s ]]; skipping. "
                    "Error: %s",
                    core_pid_str,  line, int_parsing_ex)
                continue

            core_path = get_core_path(core_pid):
                if os.path.exists(core_path):
                    defer_file_deletion(core_path)
                    debugger_invocations.append(DebuggerInvocation(
                        core_path=core_path,
                        executable_path=abs_test_binary_path
                    ))

        for debugger_invocation in debugger_invocations:
            debugger_invocation.invoke()



def main():
    atexit.register(delete_files_at_exit_callback)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(filename)s:%(lineno)d] %(asctime)s %(levelname)s: %(message)s")
    core_analyzer = CoreFileAnalyzer()
    core_analyzer.parse_args()
    core_analyzer.process_core_file()

if __name__ == '__main__':
    main()
