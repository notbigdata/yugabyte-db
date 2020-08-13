#!/usr/bin/env python3

"""
Deduplicates similar stack frames. Used for post-processing thread stacks dumped from a core file.
"""

import re
import sys

from typing import Iterator


THREAD_HEADER_RE = re.compile('^Thread (\d+) \(LWP (\d+)\):$')
HEX_NUMBER_RE_STR = r'\b0x[0-9a-fA-f]+\b'
STACK_FRAME_RE = re.compile(r'^#(\d+)\s+(.*)$')
PARAM_VALUE_RE = re.compile(r'=' + HEX_NUMBER_RE_STR)


class StackTrace:

    def __init__(self, thread_id, lwp_id):
        self.thread_id = thread_id
        self.lwp_id = lwp_id
        self.frames = []
        self.raw_frames = []

    def append_line(self, line):
        self.raw_frames.append(line)
        self.frames.append(line)

    def append_frame(self, index, line):
        if index == len(self.frames):
            self.raw_frames.append(line)
            line = PARAM_VALUE_RE.sub('=0x...', line)
            self.frames.append(line)
            return True
        return False

    def frames_key(self):
        return "\n".join(self.frames)


class ThreadStackCollector:

    def __init__(self):
        self.stacks = []
        self.current_stack = None

    def stack_finished(self):
        if self.current_stack:
            self.stacks.append(self.current_stack)
            self.current_stack = None

    def process_line(self, line):
        """
        :param line: a line from gdb output
        :return: True if the line was appended to the current stack trace, False if its format was
                 not recognized and the caller needs to handle (print) the line itself.
        """
        if line == '':
            if self.current_stack:
                self.current_stack.append_line(line)
                self.stack_finished()
                return True
            return False

        header_match = THREAD_HEADER_RE.match(line)
        if header_match:
            self.stack_finished()
            thread_id = int(header_match.group(1))
            lwp_id = int(header_match.group(2))
            self.current_stack = StackTrace(thread_id, lwp_id)
            return True

        frame_match = STACK_FRAME_RE.match(line)
        if frame_match:
            index = int(frame_match.group(1))
            if self.current_stack:
                return self.current_stack.append_frame(index, line)
            else:
                return False

        self.stack_finished()
        return False

    def grouped_stacks_line_iterator(self):
        groups = {}
        for stack in self.stacks:
            key = stack.frames_key()
            if key not in groups:
                groups[key] = []
            groups[key].append(stack)

        line_groups = []

        for key, stacks in groups.items():
            stacks = sorted(stacks, key=lambda stack: stack.thread_id)
            header = "Thread " + ", ".join(
                    "%d (LWP %d)" % (stack.thread_id, stack.lwp_id) for stack in stacks)
            min_thread_id = min(stack.thread_id for stack in stacks)
            if len(stacks) == 1:
                # This is a unique stack trace, so we can show argument values without introducing
                # any confusion.
                frames = stacks[0].raw_frames
            else:
                frames = stacks[0].frames

            line_groups.append((min_thread_id, [header] + frames))

        # Sort stack trace groups by the minimum thread id in the group.
        line_groups.sort(key=lambda t: t[0])
        for _, lines in line_groups:
            for line in lines:
                yield line
            # An empty line separator.
            yield ""


def dedup_thread_stack_line_iterator(line_iterator: Iterator[str]) -> Iterator[str]:
    collector = ThreadStackCollector()

    for line in line_iterator:
        line = line.rstrip()
        if not collector.process_line(line):
            yield line

    for line in collector.grouped_stacks_line_iterator():
        yield line


if __name__ == '__main__':
    for line in dedup_thread_stack_line_iterator(sys.stdin):
        print(line)