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

#ifndef YB_MASTER_CATALOG_MANAGER_INTERNAL_INTERFACE_H
#define YB_MASTER_CATALOG_MANAGER_INTERNAL_INTERFACE_H

#include "yb/util/status.h"
#include "yb/master/name_maps.h"
#include "yb/master/catalog_manager_locking.h"

namespace yb {
namespace master {

class SysCatalogTable;

// An internal interface of the catalog manager that is exposed to its components.
class CatalogManagerInternalIf {
 public:
  CatalogManagerInternalIf() {}
  virtual ~CatalogManagerInternalIf() {};

  virtual CHECKED_STATUS CheckOnline() const = 0;
  virtual int64_t GetLeaderReadyTermUnlocked() const = 0;
  virtual SysCatalogTable* sys_catalog() const = 0;
  virtual CatalogManagerMutexType* internal_data_mutex() const = 0;
  virtual TableInfoByNameMap& table_info_by_name_map() const =  0;
  virtual NamespaceNameMapper& namespace_name_mapper() const = 0;
};

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_CATALOG_MANAGER_INTERNAL_INTERFACE_H