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
// VirtualScreenIf
// ------------------------------------------------------------------------------------------------

void VirtualScreenIf::PutString(int top_row, int left_col, const std::string& s, int wrap_indent) {
  int i = top_row;
  int j = left_col;
  for (char c : s) {
    if (c == '\n') {
      i++;
      j = left_col;
      continue;
    }
    if (wrap_indent >= 0 && j == width_) {
      i++;
      j = left_col + wrap_indent;
    }
    PutChar(i, j, c);
    j++;
  }
}

VirtualWindow VirtualScreenIf::SubWindow(int top_row, int left_col, int height, int width) {
  return VirtualWindow(this, top_row, left_col, height, width);
}

VirtualWindow VirtualScreenIf::TopSection(int num_rows) {
  return SubWindow(0, 0, num_rows, width_);
}

VirtualWindow VirtualScreenIf::BottomSection(int num_rows) {
  return SubWindow(height_ - num_rows, 0, num_rows, width_);
}

VirtualWindow VirtualScreenIf::LeftHalf() {
  return LeftSection(width_ / 2);
}

VirtualWindow VirtualScreenIf::RightHalf() {
  return RightSection(width_ - width_ / 2);
}

VirtualWindow VirtualScreenIf::LeftSection(int num_cols) {
  return SubWindow(0, 0, height_, num_cols); 
}

VirtualWindow VirtualScreenIf::RightSection(int num_cols) {
  return SubWindow(0, width_ - num_cols, height_, num_cols);
}

// ------------------------------------------------------------------------------------------------
// VirtualWindow
// ------------------------------------------------------------------------------------------------

void VirtualWindow::PutChar(int row, int col, char c) {
  if (0 <= row && row < height_ && 0 <= col && col < width_) {
    parent_->PutChar(top_row_ + row, left_col_ + col, c);
  }
}

VirtualWindow VirtualWindow::SubWindow(int top_row, int left_col, int height, int width) {
  CHECK_GE(top_row, 0);
  CHECK_GE(left_col, 0);
  CHECK_LT(top_row, height_);
  CHECK_LT(left_col, width_);
  CHECK_LE(top_row + height, height_);
  CHECK_LE(left_col + width, width_);
  return VirtualWindow(
      parent_,
      top_row_ + top_row,
      left_col_ + left_col,
      height,
      width);
}

// ------------------------------------------------------------------------------------------------
// VirtualScreen
// ------------------------------------------------------------------------------------------------

VirtualScreen::VirtualScreen(int height, int width)
    : VirtualScreenIf(height, width),
      rows_(height),
      actual_length_(height) {
  for (auto& s : rows_) {
    s.reserve(width_);
    for (int i = 0; i < width_; ++i) {
      s.push_back(' ');
    }
  }
}

void VirtualScreen::PutChar(int row, int col, char c) {
  if (0 <= row && row < height_ && 0 <= col && col < width_) {
    rows_[row][col] = c;
    actual_num_rows_ = std::max(actual_num_rows_, row + 1);
    actual_length_[row] = std::max(actual_length_[row], col + 1);
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

Status TextBasedAnimation::AddFrame(const VirtualScreen& screen, std::string json_str) {
  if (!dir_created_) {
    RETURN_NOT_OK(Env::Default()->CreateDirs(base_dir_));
    dir_created_ = true;
  }
  string file_name = JoinPathSegments(base_dir_, StringPrintf("frame_%05d", n_frames_));
  n_frames_++;
  RETURN_NOT_OK(screen.SaveToFile(file_name));
  return WriteStringToFile(Env::Default(), json_str, file_name + ".json");
}

}  // namespace docdb
}  // namespace yb
