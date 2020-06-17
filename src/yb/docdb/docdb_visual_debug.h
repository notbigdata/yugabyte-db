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

class VirtualScreen {
 public:
  VirtualScreen(int width, int height);
  void PutChar(int row, int column, char c);
  void PutString(
      int row, int column, const std::string& s, int max_width = -1);

  template <class... Args>
  void PutFormat(int row, int column, const std::string& format, Args&&... args) {
    PutString(row, column, Format(format, std::forward<Args>(args)...));
  }

  void PutLines(int row, int column, const std::vector<string>& lines);

  CHECKED_STATUS SaveToFile(const string& file_path) const;

  int width() const { return width_; }
  int height() const { return height_; }

 private:
  std::vector<std::string> rows_;
  std::vector<int> actual_length_;
  int width_;
  int height_;
  int actual_num_rows_ = 0;
};

class TextBasedAnimation {
 public:
  TextBasedAnimation(const std::string& base_dir);
  CHECKED_STATUS AddFrame(const VirtualScreen& screen);

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
