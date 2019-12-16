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

#ifndef YB_MASTER_NAME_MAPS_H
#define YB_MASTER_NAME_MAPS_H

#include <unordered_map>

#include "yb/common/entity_ids.h"
#include "yb/common/common.pb.h"
#include "yb/gutil/ref_counted.h"

namespace yb {
namespace master {

class NamespaceInfo;
class TableInfo;
class TabletInfo;
class UDTypeInfo;

typedef std::unordered_map<NamespaceName, scoped_refptr<NamespaceInfo> > NamespaceInfoMap;

class NamespaceNameMapper {
 public:
  NamespaceInfoMap& operator[](YQLDatabase db_type);
  const NamespaceInfoMap& operator[](YQLDatabase db_type) const;
  void clear();

 private:
  std::array<NamespaceInfoMap, 4> typed_maps_;
};

typedef std::unordered_map<TabletId, scoped_refptr<TabletInfo>> TabletInfoMap;
typedef std::unordered_map<TableId, scoped_refptr<TableInfo>> TableInfoMap;
typedef std::pair<NamespaceId, TableName> TableNameKey;
typedef std::unordered_map<
    TableNameKey, scoped_refptr<TableInfo>, boost::hash<TableNameKey>> TableInfoByNameMap;

typedef std::unordered_map<UDTypeId, scoped_refptr<UDTypeInfo>> UDTypeInfoMap;
typedef std::pair<NamespaceId, UDTypeName> UDTypeNameKey;
typedef std::unordered_map<
    UDTypeNameKey, scoped_refptr<UDTypeInfo>, boost::hash<UDTypeNameKey>> UDTypeInfoByNameMap;

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_NAME_MAPS_H