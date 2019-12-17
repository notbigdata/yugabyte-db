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

#include <boost/optional.hpp>

#include <string>

#include "yb/util/status.h"
#include "yb/util/version_tracker.h"

#include "yb/master/name_maps.h"
#include "yb/master/catalog_manager_locking.h"
#include "yb/master/master.pb.h"

namespace yb {
namespace master {

class SysCatalogTable;

// An internal interface of the catalog manager that it exposes to its subcomponents, such as
// PermissionsManager and TableCreationManager.
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
  virtual VersionTracker<TableInfoMap>& table_ids_map() = 0;
  virtual void HandleNewTableId(const TableId& table_id) = 0;
  virtual std::string GenerateId(boost::optional<const SysRowEntry::Type> entity_type) = 0;
  virtual scoped_refptr<TableInfo> NewTableInfo(TableId id) = 0;
  virtual CHECKED_STATUS FindNamespace(
      const NamespaceIdentifierPB& ns_identifier,
      scoped_refptr<NamespaceInfo>* ns_info) const = 0;
  virtual scoped_refptr<TableInfo> GetTableInfo(const TableId& table_id) = 0;
};

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_CATALOG_MANAGER_INTERNAL_INTERFACE_H