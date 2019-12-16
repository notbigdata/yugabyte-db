#ifndef YB_MASTER_CATALOG_MANAGER_LOCKING_H
#define YB_MASTER_CATALOG_MANAGER_LOCKING_H

#include "yb/util/rw_mutex.h"

namespace yb {
namespace master {

typedef rw_spinlock CatalogManagerMutexType;

}  // namespace master
}  // namespace yb

#endif  // YB_MASTER_CATALOG_MANAGER_LOCKING_H
