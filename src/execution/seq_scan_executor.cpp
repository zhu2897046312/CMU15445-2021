//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
 : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
    const auto table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
     while (table_iter_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&(*table_iter_), &table_info_->schema_).GetAs<bool>()) {
    table_iter_++;
  }

  if (table_iter_ == table_info_->table_->End()) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto isolation_level = txn->GetIsolationLevel();
  auto lock_manager = exec_ctx_->GetLockManager();
  if (lock_manager != nullptr) {
    if (isolation_level != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsSharedLocked(table_iter_->GetRid()) && !txn->IsExclusiveLocked(table_iter_->GetRid()) &&
          !lock_manager->LockShared(txn, table_iter_->GetRid())) {
        return false;
      }
    }
  }

  std::vector<Value> values;
  for (auto &column : plan_->OutputSchema()->GetColumns()) {
    values.emplace_back(column.GetExpr()->Evaluate(&(*table_iter_), &table_info_->schema_));
  }

  if (lock_manager != nullptr && isolation_level == IsolationLevel::READ_COMMITTED) {
    if (!lock_manager->Unlock(txn, table_iter_->GetRid())) {
      return false;
    }
  }

  *tuple = Tuple(values, plan_->OutputSchema());
  *rid = table_iter_->GetRid();
  table_iter_++;
  return true;
 }

}  // namespace bustub
