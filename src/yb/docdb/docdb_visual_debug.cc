// Copyright (c) Yugabyte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations

#include "yb/docdb/docdb_visual_debug.h"

namespace yb {
namespace docdb {

VirtualScreen::VirtualScreen(int width, int height)
    : rows_(height),
      width_(width),
      height_(height) {
  for (auto& s : rows_) {
    s.reserve(width_);
    for (int i = 0; i < width_; ++i) {
      s.push_back(' ');
    }
  }
}

void VirtualScreen::PutChar(int row, int column, char c) {
  if (0 <= row && row < height_ && 0 <= column && column < width_) {
    rows_[row][column] = c;
  }
}

void VirtualScreen::PutString(
    const int top_row, const int left_column, const std::string& s) {
  int i = top_row;
  int j = left_column;
  for (char c : s) {
    if (c == '\n') {
      i++;
      j = left_column;
      continue;
    }
    PutChar(i, j, c);
    j++;
  }
}

}  // namespace docdb
}  // namespace yb
