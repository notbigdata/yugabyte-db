#ifndef YB_MASTER_TABLE_CREATION_MANAGER_H
#define YB_MASTER_TABLE_CREATION_MANAGER_H

#include "yb/util/status.h"

#include "yb/common/schema.h"
#include "yb/common/entity_ids.h"
#include "yb/common/common.pb.h"

#include "yb/master/master.pb.h"
#include "yb/master/catalog_manager_internal_interface.h"

namespace yb {
namespace master {

class TableCreationManager {
 public:
  TableCreationManager(CatalogManagerInternalIf* catalog_manager_if) {
  }

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

 private:
  CatalogManagerInternalIf* catalog_manager_if_;
};

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_TABLE_CREATION_MANAGER_H