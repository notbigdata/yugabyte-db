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

#include "yb/util/env.h"
#include "yb/util/path_util.h"

namespace yb {
namespace docdb {

// ------------------------------------------------------------------------------------------------
// VirtualScreen
// ------------------------------------------------------------------------------------------------

VirtualScreen::VirtualScreen(int width, int height)
    : rows_(height),
      actual_length_(height),
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
    actual_num_rows_ = std::max(actual_num_rows_, row + 1);
    actual_length_[row] = std::max(actual_length_[row], column + 1);
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

Status VirtualScreen::SaveToFile(const string& file_path) const {
  std::stringstream out;
  for (int i = 0; i < actual_num_rows_; ++i) {
    out << rows_[i].substr(0, actual_length_[i]) << '\n';
  }
  return WriteStringToFile(Env::Default(), out.str(), file_path);
}

// ------------------------------------------------------------------------------------------------
// TextBasedAnimation
// ------------------------------------------------------------------------------------------------

TextBasedAnimation::TextBasedAnimation(const std::string& base_dir)
    : base_dir_(base_dir) {
}

Status TextBasedAnimation::AddFrame(const VirtualScreen& screen) {
  if (!dir_created_) {
    if (!Env::Default()->DirExists(base_dir_)) {
      RETURN_NOT_OK(Env::Default()->CreateDir(base_dir_));
    }
    dir_created_ = true;
  }
  string file_name = JoinPathSegments(base_dir_, StringPrintf("frame_%05d", n_frames_));
  n_frames_++;
  return screen.SaveToFile(file_name);
}

}  // namespace docdb
}  // namespace yb
