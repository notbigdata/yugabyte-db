// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#ifndef YB_COMMON_ERROR_MESSAGES_H
#define YB_COMMON_ERROR_MESSAGES_H

namespace yb {

// This error message is used when the same request is submitted to a tablet multiple times, most
// frequently as a result of timeouts and retries. For historical reasons we compare against this
// exact string value on the pggate side, so it should not be changed.
static constexpr const char* kDuplicateRequestErrorMsg = "Duplicate request";

};  // namespace yb

#endif  // YB_COMMON_ERROR_MESSAGES_H
