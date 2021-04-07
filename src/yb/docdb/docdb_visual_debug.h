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

#ifndef YB_DOCDB_DOCDB_VISUAL_DEBUGGER_H
#define YB_DOCDB_DOCDB_VISUAL_DEBUGGER_H

#include <string>
#include <vector>

#include "yb/util/format.h"
#include "yb/util/status.h"

namespace yb {
namespace docdb {

class VirtualWindow;
class VirtualScreenIf {
 public:
  VirtualScreenIf(int height, int width) : height_(height), width_(width) {};

  virtual ~VirtualScreenIf() {}
  virtual void PutChar(int row, int column, char c) = 0;

  // wrap_indent < 0 means no wrap (just trim the text)
  void PutString(int top_row, int left_col, const std::string& s, int wrap_indent = -1);

  template <class... Args>
  void PutFormat(int row, int column, const std::string& format, Args&&... args) {
    PutString(row, column, Format(format, std::forward<Args>(args)...));
  }

  int height() const { return height_; }
  int width() const { return width_; }

  virtual VirtualWindow SubWindow(int top_row, int left_col, int height, int width);

  VirtualWindow TopSection(int num_rows);
  VirtualWindow BottomSection(int num_rows);
  VirtualWindow LeftSection(int num_cols);
  VirtualWindow RightSection(int num_cols);

  VirtualWindow LeftHalf();
  VirtualWindow RightHalf();

 protected:
  int height_, width_;
};

class VirtualWindow : public VirtualScreenIf {
 public:
  VirtualWindow(
      VirtualScreenIf* parent,
      int top_row,
      int left_col,
      int height,
      int width)
      : VirtualScreenIf(height, width),
        parent_(parent),
        top_row_(top_row),
        left_col_(left_col) {}
  VirtualWindow(const VirtualWindow& other) = default;
  VirtualWindow& operator= (const VirtualWindow& other) = default;

  void PutChar(int row, int column, char c) override;
  VirtualWindow SubWindow(int top_row, int left_col, int height, int width) override;
 private:
  VirtualScreenIf* parent_;
  int top_row_, left_col_;
};

class VirtualScreen : public VirtualScreenIf {
 public:
  VirtualScreen(int height, int width);
  void PutChar(int row, int column, char c) override;

  CHECKED_STATUS SaveToFile(const string& file_path) const;

 private:
  std::vector<std::string> rows_;
  std::vector<int> actual_length_;
  int actual_num_rows_ = 0;
};

class TextBasedAnimation {
 public:
  TextBasedAnimation(const std::string& base_dir);
  CHECKED_STATUS AddFrame(const VirtualScreen& screen, std::string json_str);

 private:
  std::string base_dir_;
  bool dir_created_ = false;
  int n_frames_ = 0;
};

struct DocDbDebugSnapshot {
  std::vector<std::pair<std::string, std::string>> regular_kvs_, intent_kvs_;
};

}  // namespace docdb
}  // namespace yb

#endif  // YB_DOCDB_DOCDB_VISUAL_DEBUGGER_H
