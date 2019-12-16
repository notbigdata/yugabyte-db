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

#ifndef YB_MASTER_CATALOG_PROTO_HELPERS_H
#define YB_MASTER_CATALOG_PROTO_HELPERS_H

#include "yb/master/master.pb.h"

// Macros to access index information in CATALOG.
//
// NOTES from file master.proto for SysTablesEntryPB.
// - For index table: [to be deprecated and replaced by "index_info"]
//     optional bytes indexed_table_id = 13; // Indexed table id of this index.
//     optional bool is_local_index = 14 [ default = false ];  // Whether this is a local index.
//     optional bool is_unique_index = 15 [ default = false ]; // Whether this is a unique index.
// - During transition period, we have to consider both fields and the following macros help
//   avoiding duplicate protobuf version check thru out our code.

#define PROTO_GET_INDEXED_TABLE_ID(tabpb) \
  (tabpb.has_index_info() ? tabpb.index_info().indexed_table_id() \
                          : tabpb.indexed_table_id())

#define PROTO_GET_IS_LOCAL(tabpb) \
  (tabpb.has_index_info() ? tabpb.index_info().is_local() \
                          : tabpb.is_local_index())

#define PROTO_GET_IS_UNIQUE(tabpb) \
  (tabpb.has_index_info() ? tabpb.index_info().is_unique() \
                          : tabpb.is_unique_index())

#define PROTO_IS_INDEX(tabpb) \
  (tabpb.has_index_info() || !tabpb.indexed_table_id().empty())

#define PROTO_IS_TABLE(tabpb) \
  (!tabpb.has_index_info() && tabpb.indexed_table_id().empty())

#define PROTO_PTR_IS_INDEX(tabpb) \
  (tabpb->has_index_info() || !tabpb->indexed_table_id().empty())

#define PROTO_PTR_IS_TABLE(tabpb) \
  (!tabpb->has_index_info() && tabpb->indexed_table_id().empty())

#if (0)
// Once the deprecated fields are obsolete, the above macros should be defined as the following.
#define PROTO_GET_INDEXED_TABLE_ID(tabpb) (tabpb.index_info().indexed_table_id())
#define PROTO_GET_IS_LOCAL(tabpb) (tabpb.index_info().is_local())
#define PROTO_GET_IS_UNIQUE(tabpb) (tabpb.index_info().is_unique())
#define PROTO_IS_INDEX(tabpb) (tabpb.has_index_info())
#define PROTO_IS_TABLE(tabpb) (!tabpb.has_index_info())
#define PROTO_PTR_IS_INDEX(tabpb) (tabpb->has_index_info())
#define PROTO_PTR_IS_TABLE(tabpb) (!tabpb->has_index_info())

#endif

#define RETURN_NAMESPACE_NOT_FOUND(s, resp)                                       \
  do {                                                                            \
    if (PREDICT_FALSE(!s.ok())) {                                                 \
      if (s.IsNotFound()) {                                                       \
        return SetupError(                                                        \
            resp->mutable_error(), MasterErrorPB::NAMESPACE_NOT_FOUND, s);        \
      }                                                                           \
      return s;                                                                   \
    }                                                                             \
  } while (false)

#endif  // YB_MASTER_CATALOG_PROTO_HELPERS_H