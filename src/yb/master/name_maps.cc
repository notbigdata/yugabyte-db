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

#include "yb/master/name_maps.h"
#include "yb/master/catalog_entity_info.h"

namespace yb {
namespace master {

namespace {

size_t GetNameMapperIndex(YQLDatabase db_type) {
  switch (db_type) {
    case YQL_DATABASE_UNKNOWN: break;
    case YQL_DATABASE_CQL: return 1;
    case YQL_DATABASE_PGSQL: return 2;
    case YQL_DATABASE_REDIS: return 3;
  }
  CHECK(false) << "Unexpected db type " << db_type;
  return 0;
}

}  // anonymous namespace


NamespaceInfoMap& NamespaceNameMapper::operator[](YQLDatabase db_type) {
  return typed_maps_[GetNameMapperIndex(db_type)];
}

const NamespaceInfoMap& NamespaceNameMapper::operator[](YQLDatabase db_type) const {
  return typed_maps_[GetNameMapperIndex(db_type)];
}

void NamespaceNameMapper::clear() {
  for (auto& m : typed_maps_) {
    m.clear();
  }
}

}  // namespace master
}  // namespace yb