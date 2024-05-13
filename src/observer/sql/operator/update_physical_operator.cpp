#include "common/log/log.h"
#include "sql/operator/update_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/update_stmt.h"
#include "sql/operator/aggregate_physical_operator.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table,Field field, Value value)
:table_(table),field_(field),value_(value)
{}
RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();

  std::vector<Record> insert_record;
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s",strrc(rc));
      return rc;
    }
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    // 定位列名索引
    const std::vector<FieldMeta> *table_field_metas = table_->table_meta().field_metas();
    const char                   *target_field_name = field_.field_name();
    int meta_num     = table_field_metas->size();
    int target_index = -1;
    for (int i = 0; i < meta_num; i++) {
      FieldMeta   fieldmeta  = (*table_field_metas)[i];
      const char *field_name = fieldmeta.name();
      if (0 == strcmp(field_name, target_field_name)) {
        target_index = i;
        break;
      }
    }
    // 获取目标字段的类型
    AttrType target_field_type = table_field_metas->at(target_index).type();
    // 检查设置的新值与目标字段类型是否匹配
    if (value_.attr_type() != target_field_type) {
      LOG_WARN("Value type does not match target field type");
      return RC::INVALID_ARGUMENT; // 或者选择适当的错误代码
    }
    RC rc = trx_->delete_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s",strrc(rc));
      return rc;
    }
    // 重新构造record
    // 1.Values
    int                cell_num = row_tuple->cell_num();
    std::vector<Value> values(cell_num);
    for (size_t i = 0; i < cell_num; ++i)
      row_tuple->cell_at(i, values[i]);
    if (target_index!=-1) values[target_index] = value_;
    Record NewRecord;
    table_->make_record(cell_num, values.data(), NewRecord);
    rc = trx_->insert_record(table_, NewRecord);
  }
  return RC::RECORD_EOF;
}


RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

