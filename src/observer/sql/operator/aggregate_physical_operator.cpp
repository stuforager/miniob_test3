
#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/delete_stmt.h"
#include <iostream>


void AggregatePhysicalOperator:: add_aggregation(const AggrOp aggregation)
{
  aggregations_.push_back(aggregation);
}
RC AggregatePhysicalOperator::open(Trx *trx)
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

RC AggregatePhysicalOperator::next()
{
 if(result_tuple_.cell_num()>0){
    return RC::RECORD_EOF;
 }

 RC rc=RC::SUCCESS;
 PhysicalOperator*oper=children_[0].get();

 std::vector<Value> result_cells(aggregations_.size(),Value());

 while(RC::SUCCESS==(rc=oper->next())){
    Tuple *tuple=oper->current_tuple();
    for(int cell_idx=0;cell_idx<(int)aggregations_.size();cell_idx++){
        const AggrOp aggregation =aggregations_[cell_idx];

        Value cell;
        AttrType attr_type =AttrType::INTS;
        switch (aggregation)
        {
          case AggrOp::AGGR_SUM:
             rc=tuple->cell_at(cell_idx,cell);
             attr_type=cell.attr_type();
             if(attr_type==AttrType::INTS or attr_type==AttrType::FLOATS){
                result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());
                std::cout<<result_cells[cell_idx].get_float();
             }
            break;
          case AggrOp::AGGR_MAX:
              rc = tuple->cell_at(cell_idx, cell);
                    attr_type = cell.attr_type();
                    if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {
                        // 如果当前单元格值大于之前保存的最大值，则更新最大值
                        if (cell.get_float() > result_cells[cell_idx].get_float()) {
                            result_cells[cell_idx].set_float(cell.get_float());
                        }
                    }
                    break;
          case AggrOp::AGGR_MIN:
              rc = tuple->cell_at(cell_idx, cell);
                    attr_type = cell.attr_type();
                    if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {
                        // 如果当前单元格值小于之前保存的最小值，则更新最小值
                        if (cell.get_float() < result_cells[cell_idx].get_float() ) {
                            result_cells[cell_idx].set_float(cell.get_float());
                        }
                    }
                    break;
          case AggrOp::AGGR_COUNT:
              result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
                    break;
          case AggrOp::AGGR_AVG:
                rc = tuple->cell_at(cell_idx, cell);
                    attr_type = cell.attr_type();
                    if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {
                        // 实现求和逻辑
                        result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
                        // 记录元组个数
                        result_cells[result_cells.size() - 1].set_int(result_cells[result_cells.size() - 1].get_int() + 1);
                    }
                    break;
              
        
        default:
            return RC::UNIMPLENMENT;
        }
    }
     if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
    }

    // 计算平均值
    for (int i = 0; i < result_cells.size(); ++i) {
        if (aggregations_[i] == AggrOp::AGGR_AVG) {
            result_cells[i].set_float(result_cells[i].get_float() / result_cells[result_cells.size() - 1].get_int());
        }
    }

    result_tuple_.set_cells(result_cells);

    return rc;
 }
 if(rc==RC::RECORD_EOF){
    rc=RC::SUCCESS;
 }

 result_tuple_.set_cells(result_cells);

 return rc;
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
