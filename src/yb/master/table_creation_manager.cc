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

#include "yb/master/table_creation_manager.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/catalog_manager-internal.h"

#include <stdlib.h>

#include <algorithm>
#include <bitset>
#include <functional>
#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <boost/optional.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "yb/common/common_flags.h"
#include "yb/common/partial_row.h"
#include "yb/common/partition.h"
#include "yb/common/roles_permissions.h"
#include "yb/common/wire_protocol.h"
#include "yb/consensus/consensus.h"
#include "yb/consensus/consensus.proxy.h"
#include "yb/consensus/consensus_peers.h"
#include "yb/consensus/quorum_util.h"
#include "yb/gutil/atomicops.h"
#include "yb/gutil/map-util.h"
#include "yb/gutil/mathlimits.h"
#include "yb/gutil/stl_util.h"
#include "yb/gutil/strings/escaping.h"
#include "yb/gutil/strings/join.h"
#include "yb/gutil/strings/substitute.h"
#include "yb/gutil/sysinfo.h"
#include "yb/gutil/walltime.h"
#include "yb/master/catalog_manager_util.h"
#include "yb/master/cluster_balance.h"
#include "yb/master/master.h"
#include "yb/master/master.pb.h"
#include "yb/master/master.proxy.h"
#include "yb/master/master_util.h"
#include "yb/master/sys_catalog_constants.h"
#include "yb/master/system_tablet.h"
#include "yb/master/sys_catalog.h"
#include "yb/master/ts_descriptor.h"
#include "yb/master/ts_manager.h"
#include "yb/master/async_rpc_tasks.h"
#include "yb/master/yql_auth_roles_vtable.h"
#include "yb/master/yql_auth_role_permissions_vtable.h"
#include "yb/master/yql_auth_resource_role_permissions_index.h"
#include "yb/master/yql_columns_vtable.h"
#include "yb/master/yql_empty_vtable.h"
#include "yb/master/yql_keyspaces_vtable.h"
#include "yb/master/yql_local_vtable.h"
#include "yb/master/yql_peers_vtable.h"
#include "yb/master/yql_tables_vtable.h"
#include "yb/master/yql_aggregates_vtable.h"
#include "yb/master/yql_functions_vtable.h"
#include "yb/master/yql_indexes_vtable.h"
#include "yb/master/yql_triggers_vtable.h"
#include "yb/master/yql_types_vtable.h"
#include "yb/master/yql_views_vtable.h"
#include "yb/master/yql_partitions_vtable.h"
#include "yb/master/yql_size_estimates_vtable.h"
#include "yb/master/catalog_manager_bg_tasks.h"
#include "yb/master/catalog_loaders.h"
#include "yb/master/sys_catalog_initialization.h"
#include "yb/master/tasks_tracker.h"
#include "yb/master/encryption_manager.h"
#include "yb/master/catalog_proto_helpers.h"

#include "yb/tserver/ts_tablet_manager.h"
#include "yb/rpc/messenger.h"

#include "yb/tablet/operations/change_metadata_operation.h"
#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_metadata.h"

#include "yb/tserver/tserver_admin.proxy.h"

#include "yb/util/crypt.h"
#include "yb/util/debug/trace_event.h"
#include "yb/util/flag_tags.h"
#include "yb/util/logging.h"
#include "yb/util/math_util.h"
#include "yb/util/monotime.h"
#include "yb/util/random_util.h"
#include "yb/util/rw_mutex.h"
#include "yb/util/stopwatch.h"
#include "yb/util/thread.h"
#include "yb/util/thread_restrictions.h"
#include "yb/util/threadpool.h"
#include "yb/util/trace.h"
#include "yb/util/tsan_util.h"
#include "yb/util/uuid.h"

#include "yb/client/client.h"
#include "yb/client/meta_cache.h"
#include "yb/client/table_creator.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_table_name.h"

#include "yb/tserver/remote_bootstrap_client.h"

#include "yb/yql/redis/redisserver/redis_constants.h"
#include "yb/yql/pgwrapper/pg_wrapper.h"
#include "yb/util/shared_lock.h"

using namespace std::literals;

DEFINE_bool(master_enable_metrics_snapshotter, false, "Should metrics snapshotter be enabled");

DEFINE_bool(
    hide_pg_catalog_table_creation_logs, false,
    "Whether to hide detailed log messages for PostgreSQL catalog table creation. "
    "This cuts down test logs significantly.");
TAG_FLAG(hide_pg_catalog_table_creation_logs, hidden);

namespace yb {
namespace master {

namespace {

class IndexInfoBuilder {
 public:
  explicit IndexInfoBuilder(IndexInfoPB* index_info) : index_info_(*index_info) {
  }

  void ApplyProperties(const TableId& indexed_table_id, bool is_local, bool is_unique) {
    index_info_.set_indexed_table_id(indexed_table_id);
    index_info_.set_version(0);
    index_info_.set_is_local(is_local);
    index_info_.set_is_unique(is_unique);
  }

  CHECKED_STATUS ApplyColumnMapping(const Schema& indexed_schema, const Schema& index_schema) {
    for (size_t i = 0; i < index_schema.num_columns(); i++) {
      const auto& col_name = index_schema.column(i).name();
      const auto indexed_col_idx = indexed_schema.find_column(col_name);
      if (PREDICT_FALSE(indexed_col_idx == Schema::kColumnNotFound)) {
        return STATUS(NotFound, "The indexed table column does not exist", col_name);
      }
      auto* col = index_info_.add_columns();
      col->set_column_id(index_schema.column_id(i));
      col->set_indexed_column_id(indexed_schema.column_id(indexed_col_idx));
    }
    index_info_.set_hash_column_count(index_schema.num_hash_key_columns());
    index_info_.set_range_column_count(index_schema.num_range_key_columns());

    for (size_t i = 0; i < indexed_schema.num_hash_key_columns(); i++) {
      index_info_.add_indexed_hash_column_ids(indexed_schema.column_id(i));
    }
    for (size_t i = indexed_schema.num_hash_key_columns(); i < indexed_schema.num_key_columns();
        i++) {
      index_info_.add_indexed_range_column_ids(indexed_schema.column_id(i));
    }
    return Status::OK();
  }

 private:
  IndexInfoPB& index_info_;
};

}  // namespace

TableCreationManager::TableCreationManager(CatalogManagerInternalIf* catalog_manager_if)
    : catalog_manager_if_(catalog_manager_if),
      catalog_mutex_(catalog_manager_if->mutex()) {
}


Status TableCreationManager::CreateTableInMemory(
    const CreateTableRequestPB& req,
    const Schema& schema,
    const PartitionSchema& partition_schema,
    const bool create_tablets,
    const NamespaceId& namespace_id,
    const std::vector<Partition>& partitions,
    IndexInfoPB* index_info,
    std::vector<TabletInfo*>* tablets,
    CreateTableResponsePB* resp,
    scoped_refptr<TableInfo>* table) {
  // Verify we have catalog manager lock.
  if (!lock_.is_locked()) {
    return STATUS(IllegalState, "We don't have the catalog manager lock!");
  }

  // Add the new table in "preparing" state.
  *table = CreateTableInfo(req, schema, partition_schema, namespace_id, index_info);
  const TableId& table_id = (*table)->id();
  auto table_ids_map_checkout = table_ids_map_.CheckOut();
  (*table_ids_map_checkout)[table_id] = *table;
  // Do not add Postgres tables to the name map as the table name is not unique in a namespace.
  if (req.table_type() != PGSQL_TABLE_TYPE) {
    table_names_map_[{namespace_id, req.name()}] = *table;
  }

  if (create_tablets) {
    RETURN_NOT_OK(CreateTabletsFromTable(partitions, *table, tablets));
  }

  if (resp != nullptr) {
    resp->set_table_id(table_id);
  }

  HandleNewTableId(table_id);

  return Status::OK();
}

Status CatalogManager::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                         IsCreateTableDoneResponsePB* resp) {
  RETURN_NOT_OK(CheckOnline());

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists.
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = STATUS(NotFound, "The object does not exist", req->table().ShortDebugString());
    return SetupError(resp->mutable_error(), MasterErrorPB::OBJECT_NOT_FOUND, s);
  }

  TRACE("Locking table");
  auto l = table->LockForRead();
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(l.get(), resp));

  // 2. Verify if the create is in-progress.
  TRACE("Verify if the table creation is in progress for $0", table->ToString());
  resp->set_done(!table->IsCreateInProgress());

  // 3. Set any current errors, if we are experiencing issues creating the table. This will be
  // bubbled up to the MasterService layer. If it is an error, it gets wrapped around in
  // MasterErrorPB::UNKNOWN_ERROR.
  RETURN_NOT_OK(table->GetCreateTableErrorStatus());

  // 4. For index table, check if alter schema is done on the indexed table also.
  if (resp->done() && PROTO_IS_INDEX(l->data().pb)) {
    IsAlterTableDoneRequestPB alter_table_req;
    IsAlterTableDoneResponsePB alter_table_resp;
    alter_table_req.mutable_table()->set_table_id(PROTO_GET_INDEXED_TABLE_ID(l->data().pb));
    const Status s = IsAlterTableDone(&alter_table_req, &alter_table_resp);
    if (!s.ok()) {
      resp->mutable_error()->Swap(alter_table_resp.mutable_error());
      return s;
    }
    resp->set_done(alter_table_resp.done());
  }

  // If this is a transactional table we are not done until the transaction status table is created.
  // However, if we are currently initializing the system catalog snapshot, we don't create the
  // transactions table.
  if (!FLAGS_create_initial_sys_catalog_snapshot &&
      resp->done() && l->data().pb.schema().table_properties().is_transactional()) {
    RETURN_NOT_OK(IsTransactionStatusTableCreated(resp));
  }

  // We are not done until the metrics snapshots table is created.
  if (FLAGS_master_enable_metrics_snapshotter && resp->done() &&
      !(table->GetTableType() == TableType::YQL_TABLE_TYPE &&
        table->namespace_id() == kSystemNamespaceId &&
        table->name() == kMetricsSnapshotsTableName)) {
    RETURN_NOT_OK(IsMetricsSnapshotsTableCreated(resp));
  }

  return Status::OK();
}

scoped_refptr<TableInfo> CatalogManager::CreateTableInfo(const CreateTableRequestPB& req,
                                                         const Schema& schema,
                                                         const PartitionSchema& partition_schema,
                                                         const NamespaceId& namespace_id,
                                                         IndexInfoPB* index_info) {
  DCHECK(schema.has_column_ids());
  TableId table_id = !req.table_id().empty() ? req.table_id() : GenerateId(SysRowEntry::TABLE);
  scoped_refptr<TableInfo> table = NewTableInfo(table_id);
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB *metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTablesEntryPB::PREPARING);
  metadata->set_name(req.name());
  metadata->set_table_type(req.table_type());
  metadata->set_namespace_id(namespace_id);
  metadata->set_version(0);
  metadata->set_next_column_id(ColumnId(schema.max_col_id() + 1));
  // TODO(bogdan): add back in replication_info once we allow overrides!
  // Use the Schema object passed in, since it has the column IDs already assigned,
  // whereas the user request PB does not.
  SchemaToPB(schema, metadata->mutable_schema());
  partition_schema.ToPB(metadata->mutable_partition_schema());
  // For index table, set index details (indexed table id and whether the index is local).
  if (req.has_index_info()) {
    metadata->mutable_index_info()->CopyFrom(req.index_info());

    // Set the deprecated fields also for compatibility reasons.
    metadata->set_indexed_table_id(req.index_info().indexed_table_id());
    metadata->set_is_local_index(req.index_info().is_local());
    metadata->set_is_unique_index(req.index_info().is_unique());

    // Setup index info.
    if (index_info != nullptr) {
      index_info->set_table_id(table->id());
      metadata->mutable_index_info()->CopyFrom(*index_info);
    }
  } else if (req.has_indexed_table_id()) {
    // Read data from the deprecated field and update the new fields.
    metadata->mutable_index_info()->set_indexed_table_id(req.indexed_table_id());
    metadata->mutable_index_info()->set_is_local(req.is_local_index());
    metadata->mutable_index_info()->set_is_unique(req.is_unique_index());

    // Set the deprecated fields also for compatibility reasons.
    metadata->set_indexed_table_id(req.indexed_table_id());
    metadata->set_is_local_index(req.is_local_index());
    metadata->set_is_unique_index(req.is_unique_index());

    // Setup index info.
    if (index_info != nullptr) {
      index_info->set_table_id(table->id());
      metadata->mutable_index_info()->CopyFrom(*index_info);
    }
  }

  if (req.is_pg_shared_table()) {
    metadata->set_is_pg_shared_table(true);
  }

  return table;
}

// Create a new table.
// See README file in this directory for a description of the design.
Status CatalogManager::CreateTable(const CreateTableRequestPB* orig_req,
                                   CreateTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  RETURN_NOT_OK(CheckOnline());

  const bool is_pg_table = orig_req->table_type() == PGSQL_TABLE_TYPE;
  const bool is_pg_catalog_table = is_pg_table && orig_req->is_pg_catalog_table();
  if (!is_pg_catalog_table || !FLAGS_hide_pg_catalog_table_creation_logs) {
    LOG(INFO) << "CreateTable from " << RequestorString(rpc)
                << ":\n" << orig_req->DebugString();
  } else {
    LOG(INFO) << "CreateTable from " << RequestorString(rpc) << ": " << orig_req->name();
  }

  const bool is_transactional = orig_req->schema().table_properties().is_transactional();
  // If this is a transactional table, we need to create the transaction status table (if it does
  // not exist already).
  if (is_transactional && (!is_pg_catalog_table || !FLAGS_create_initial_sys_catalog_snapshot)) {
    Status s = CreateTransactionsStatusTableIfNeeded(rpc);
    if (!s.ok()) {
      return s.CloneAndPrepend("Error while creating transaction status table");
    }
  } else {
    VLOG(1)
        << "Not attempting to create a transaction status table:\n"
        << "  " << EXPR_VALUE_FOR_LOG(is_transactional) << "\n "
        << "  " << EXPR_VALUE_FOR_LOG(is_pg_catalog_table) << "\n "
        << "  " << EXPR_VALUE_FOR_LOG(FLAGS_create_initial_sys_catalog_snapshot);
  }

  if (is_pg_catalog_table) {
    return CreatePgsqlSysTable(orig_req, resp, rpc);
  }

  Status s;
  const char* const object_type = PROTO_PTR_IS_TABLE(orig_req) ? "table" : "index";

  // Copy the request, so we can fill in some defaults.
  CreateTableRequestPB req = *orig_req;

  // Lookup the namespace and verify if it exists.
  TRACE("Looking up namespace");
  scoped_refptr<NamespaceInfo> ns;
  RETURN_NAMESPACE_NOT_FOUND(FindNamespace(req.namespace_(), &ns), resp);
  if (ns->database_type() != GetDatabaseTypeForTable(req.table_type())) {
    Status s = STATUS(NotFound, "Namespace not found");
    return SetupError(resp->mutable_error(), MasterErrorPB::NAMESPACE_NOT_FOUND, s);
  }
  NamespaceId namespace_id = ns->id();

  // Validate schema.
  Schema client_schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &client_schema));
  RETURN_NOT_OK(ValidateCreateTableSchema(client_schema, resp));

  // checking that referenced user-defined types (if any) exist.
  {
    SharedLock<LockType> l(lock_);
    for (int i = 0; i < client_schema.num_columns(); i++) {
      for (const auto &udt_id : client_schema.column(i).type()->GetUserDefinedTypeIds()) {
        if (FindPtrOrNull(udtype_ids_map_, udt_id) == nullptr) {
          Status s = STATUS(InvalidArgument, "Referenced user-defined type not found");
          return SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
        }
      }
    }
  }
  // TODO (ENG-1860) The referenced namespace and types retrieved/checked above could be deleted
  // some time between this point and table creation below.
  Schema schema = client_schema.CopyWithColumnIds();
  if (schema.table_properties().HasCopartitionTableId()) {
    return CreateCopartitionedTable(req, resp, rpc, schema, namespace_id);
  }

  // If neither hash nor range schema have been specified by the protobuf request, we assume the
  // table uses a hash schema, and we use the table_type and hash_key to determine the hashing
  // scheme (redis or multi-column) that should be used.
  if (!req.partition_schema().has_hash_schema() && !req.partition_schema().has_range_schema()) {
    if (req.table_type() == REDIS_TABLE_TYPE) {
      req.mutable_partition_schema()->set_hash_schema(PartitionSchemaPB::REDIS_HASH_SCHEMA);
    } else if (schema.num_hash_key_columns() > 0) {
      req.mutable_partition_schema()->set_hash_schema(PartitionSchemaPB::MULTI_COLUMN_HASH_SCHEMA);
    } else {
      Status s = STATUS(InvalidArgument, "Unknown table type or partitioning method");
      return SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
    }
  }

  // Get cluster level placement info.
  ReplicationInfoPB replication_info;
  {
    auto l = cluster_config_->LockForRead();
    replication_info = l->data().pb.replication_info();
  }
  // Calculate number of tablets to be used.
  int num_tablets = req.num_tablets();
  if (num_tablets <= 0) {
    // Use default as client could have gotten the value before any tserver had heartbeated
    // to (a new) master leader.
    TSDescriptorVector ts_descs;
    master_->ts_manager()->GetAllLiveDescriptorsInCluster(
        &ts_descs, replication_info.live_replicas().placement_uuid(), blacklistState.tservers_);
    num_tablets = ts_descs.size() * (is_pg_table ? FLAGS_ysql_num_shards_per_tserver
                                                 : FLAGS_yb_num_shards_per_tserver);
    LOG(INFO) << "Setting default tablets to " << num_tablets << " with "
              << ts_descs.size() << " primary servers";
  }

  // Create partitions.
  PartitionSchema partition_schema;
  vector<Partition> partitions;
  s = PartitionSchema::FromPB(req.partition_schema(), schema, &partition_schema);
  if (req.partition_schema().has_hash_schema()) {
    switch (partition_schema.hash_schema()) {
      case YBHashSchema::kPgsqlHash:
        // TODO(neil) After a discussion, PGSQL hash should be done appropriately.
        // For now, let's not doing anything. Just borrow the multi column hash.
        FALLTHROUGH_INTENDED;
      case YBHashSchema::kMultiColumnHash: {
        // Use the given number of tablets to create partitions and ignore the other schema options
        // in the request.
        RETURN_NOT_OK(partition_schema.CreatePartitions(num_tablets, &partitions));
        break;
      }
      case YBHashSchema::kRedisHash: {
        RETURN_NOT_OK(partition_schema.CreatePartitions(num_tablets, &partitions,
                                                        kRedisClusterSlots));
        break;
      }
    }
  } else if (req.partition_schema().has_range_schema()) {
    vector<YBPartialRow> split_rows;
    RETURN_NOT_OK(partition_schema.CreatePartitions(split_rows, schema, &partitions));
    DCHECK_EQ(1, partitions.size());
  } else {
    DFATAL_OR_RETURN_NOT_OK(STATUS(InvalidArgument, "Invalid partition method"));
  }

  // Validate the table placement rules are a subset of the cluster ones.
  s = ValidateTableReplicationInfo(req.replication_info());
  if (PREDICT_FALSE(!s.ok())) {
    return SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
  }

  // For index table, populate the index info.
  scoped_refptr<TableInfo> indexed_table;
  IndexInfoPB index_info;

  if (req.has_index_info()) {
    // Current message format.
    TRACE("Looking up indexed table");
    index_info.CopyFrom(req.index_info());
    indexed_table = GetTableInfo(index_info.indexed_table_id());
    if (indexed_table == nullptr) {
      return STATUS(NotFound, "The indexed table does not exist");
    }

    // Assign column-ids that have just been computed and assigned to "index_info".
    if (!is_pg_table) {
      DCHECK_EQ(index_info.columns().size(), schema.num_columns())
        << "Number of columns are not the same between index_info and index_schema";
      // int colidx = 0;
      for (int colidx = 0; colidx < schema.num_columns(); colidx++) {
        index_info.mutable_columns(colidx)->set_column_id(schema.column_id(colidx));
      }
    }
  } else if (req.has_indexed_table_id()) {
    // Old client message format when rolling upgrade (Not having "index_info").
    TRACE("Looking up indexed table");
    indexed_table = GetTableInfo(req.indexed_table_id());
    if (indexed_table == nullptr) {
      return STATUS(NotFound, "The indexed table does not exist");
    }
    IndexInfoBuilder index_info_builder(&index_info);
    index_info_builder.ApplyProperties(req.indexed_table_id(),
        req.is_local_index(), req.is_unique_index());
    if (orig_req->table_type() != PGSQL_TABLE_TYPE) {
      Schema indexed_schema;
      RETURN_NOT_OK(indexed_table->GetSchema(&indexed_schema));
      RETURN_NOT_OK(index_info_builder.ApplyColumnMapping(indexed_schema, schema));
    }
  }

  TSDescriptorVector all_ts_descs;
  master_->ts_manager()->GetAllLiveDescriptors(&all_ts_descs);
  s = CheckValidReplicationInfo(replication_info, all_ts_descs, partitions, resp);
  if (!s.ok()) {
    return s;
  }

  scoped_refptr<TableInfo> table;
  vector<TabletInfo*> tablets;
  {
    std::lock_guard<decltype(lock_)> l(lock_);
    TRACE("Acquired catalog manager lock");

    // Verify that the table does not exist.
    table = FindPtrOrNull(table_names_map_, {namespace_id, req.name()});

    if (table != nullptr) {
      s = STATUS_SUBSTITUTE(AlreadyPresent,
              "Object '$0.$1' already exists", ns->name(), table->name());
      // If the table already exists, we set the response table_id field to the id of the table that
      // already exists. This is necessary because before we return the error to the client (or
      // success in case of a "CREATE TABLE IF NOT EXISTS" request) we want to wait for the existing
      // table to be available to receive requests. And we need the table id for that.
      resp->set_table_id(table->id());
      return SetupError(resp->mutable_error(), MasterErrorPB::OBJECT_ALREADY_PRESENT, s);
    }

    RETURN_NOT_OK(CreateTableInMemory(req, schema, partition_schema, true /* create_tablets */,
                                      namespace_id, partitions, &index_info,
                                      &tablets, resp, &table));
  }

  if (PREDICT_FALSE(FLAGS_simulate_slow_table_create_secs > 0)) {
    LOG(INFO) << "Simulating slow table creation";
    SleepFor(MonoDelta::FromSeconds(FLAGS_simulate_slow_table_create_secs));
  }
  TRACE("Inserted new table and tablet info into CatalogManager maps");

  // NOTE: the table and tablets are already locked for write at this point,
  // since the CreateTableInfo/CreateTabletInfo functions leave them in that state.
  // They will get committed at the end of this function.
  // Sanity check: the tables and tablets should all be in "preparing" state.
  CHECK_EQ(SysTablesEntryPB::PREPARING, table->metadata().dirty().pb.state());
  for (const TabletInfo *tablet : tablets) {
    CHECK_EQ(SysTabletsEntryPB::PREPARING, tablet->metadata().dirty().pb.state());
  }

  // Write Tablets to sys-tablets (in "preparing" state).
  s = sys_catalog_->AddItems(tablets, leader_ready_term_);
  if (PREDICT_FALSE(!s.ok())) {
    return AbortTableCreation(table.get(), tablets,
                              s.CloneAndPrepend(
                                  Substitute("An error occurred while inserting to sys-tablets: $0",
                                             s.ToString())),
                              resp);
  }
  TRACE("Wrote tablets to system table");

  // Update the on-disk table state to "running".
  table->mutable_metadata()->mutable_dirty()->pb.set_state(SysTablesEntryPB::RUNNING);
  s = sys_catalog_->AddItem(table.get(), leader_ready_term_);
  if (PREDICT_FALSE(!s.ok())) {
    return AbortTableCreation(table.get(), tablets,
                              s.CloneAndPrepend(
                                  Substitute("An error occurred while inserting to sys-tablets: $0",
                                             s.ToString())),
                              resp);
  }
  TRACE("Wrote table to system table");

  // For index table, insert index info in the indexed table.
  if ((req.has_index_info() || req.has_indexed_table_id()) && !is_pg_table) {
    s = AddIndexInfoToTable(indexed_table, index_info);
    if (PREDICT_FALSE(!s.ok())) {
      return AbortTableCreation(table.get(), tablets,
                                s.CloneAndPrepend(
                                    Substitute("An error occurred while inserting index info: $0",
                                               s.ToString())),
                                resp);
    }
  }

  // Commit the in-memory state.
  table->mutable_metadata()->CommitMutation();

  for (TabletInfo *tablet : tablets) {
    tablet->mutable_metadata()->CommitMutation();
  }

  if (req.has_creator_role_name()) {
    const NamespaceName& keyspace_name = req.namespace_().name();
    const TableName& table_name = req.name();
    RETURN_NOT_OK(permissions_manager_->GrantPermissions(
        req.creator_role_name(),
        get_canonical_table(keyspace_name, table_name),
        table_name,
        keyspace_name,
        all_permissions_for_resource(ResourceType::TABLE),
        ResourceType::TABLE,
        resp));
  }

  LOG(INFO) << "Successfully created " << object_type << " " << table->ToString()
            << " per request from " << RequestorString(rpc);
  background_tasks_->Wake();

  if (FLAGS_master_enable_metrics_snapshotter &&
      !(req.table_type() == TableType::YQL_TABLE_TYPE &&
        namespace_id == kSystemNamespaceId &&
        req.name() == kMetricsSnapshotsTableName)) {
    Status s = CreateMetricsSnapshotsTableIfNeeded(rpc);
    if (!s.ok()) {
      return s.CloneAndPrepend("Error while creating metrics snapshots table");
    }
  }

  return Status::OK();
}

}  // namespace master
}  // namespace yb
