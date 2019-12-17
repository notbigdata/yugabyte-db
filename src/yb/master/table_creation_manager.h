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

#ifndef YB_MASTER_TABLE_CREATION_MANAGER_H
#define YB_MASTER_TABLE_CREATION_MANAGER_H

#include "yb/util/status.h"

#include "yb/common/schema.h"
#include "yb/common/entity_ids.h"
#include "yb/common/partition.h"
#include "yb/common/common.pb.h"

#include "yb/rpc/rpc_fwd.h"

#include "yb/master/master.pb.h"
#include "yb/master/catalog_manager_internal_interface.h"

namespace yb {
namespace master {

class TableCreationManager {
 public:
  TableCreationManager(CatalogManagerInternalIf* catalog_manager_if);

  // Creates the table and associated tablet objects in-memory and updates the appropriate
  // catalog manager maps.
  CHECKED_STATUS CreateTableInMemory(const CreateTableRequestPB& req,
                                     const Schema& schema,
                                     const PartitionSchema& partition_schema,
                                     const bool create_tablets,
                                     const NamespaceId& namespace_id,
                                     const vector<Partition>& partitions,
                                     IndexInfoPB* index_info,
                                     vector<TabletInfo*>* tablets,
                                     CreateTableResponsePB* resp,
                                     scoped_refptr<TableInfo>* table);

  // Helper for creating the initial TableInfo state
  // Leaves the table "write locked" with the new info in the
  // "dirty" state field.
  scoped_refptr<TableInfo> CreateTableInfo(const CreateTableRequestPB& req,
                                           const Schema& schema,
                                           const PartitionSchema& partition_schema,
                                           const NamespaceId& namespace_id,
                                           IndexInfoPB* index_info);

  // Create a new Table with the specified attributes.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  CHECKED_STATUS CreateTable(const CreateTableRequestPB* req,
                             CreateTableResponsePB* resp,
                             rpc::RpcContext* rpc);

  CHECKED_STATUS CreateTabletsFromTable(
      const vector<Partition>& partitions,
      const scoped_refptr<TableInfo>& table,
      std::vector<TabletInfo*>* tablets);

  // Validates that the passed-in table replication information respects the overall cluster level
  // configuration. This should essentially not be more broader reaching than the cluster. As an
  // example, if the cluster is confined to AWS, you cannot have tables in GCE.
  CHECKED_STATUS ValidateTableReplicationInfo(const ReplicationInfoPB& replication_info);

  // Helper for creating the initial TabletInfo state.
  // Leaves the tablet "write locked" with the new info in the
  // "dirty" state field.
  TabletInfo *CreateTabletInfo(TableInfo* table, const PartitionPB& partition);

  // Helper for creating copartitioned table.
  CHECKED_STATUS CreateCopartitionedTable(const CreateTableRequestPB req,
                                          CreateTableResponsePB* resp,
                                          rpc::RpcContext* rpc,
                                          Schema schema,
                                          NamespaceId namespace_id);

  // Get the information about an in-progress create operation.
  CHECKED_STATUS IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                   IsCreateTableDoneResponsePB* resp);

  // Check if the transaction status table creation is done.
  //
  // This is called at the end of IsCreateTableDone if the table has transactions enabled.
  CHECKED_STATUS IsTransactionStatusTableCreated(IsCreateTableDoneResponsePB* resp);

  // Check if the metrics snapshots table creation is done.
  //
  // This is called at the end of IsCreateTableDone.
  CHECKED_STATUS IsMetricsSnapshotsTableCreated(IsCreateTableDoneResponsePB* resp);


 private:
  CatalogManagerInternalIf* catalog_manager_internal_;
  CatalogManagerMutexType& catalog_manager_mtx_;
};

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_TABLE_CREATION_MANAGER_H