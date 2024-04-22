#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple.h"


class Trx;
class DeleteStmt;

/**
 * @brief 物理算子，删除
 * @ingroup PhysicalOperator
 */
class AggregatePhysicalOperator : public PhysicalOperator
{
public:
  AggregatePhysicalOperator() 
  {}

  virtual ~AggregatePhysicalOperator() = default;

  void add_aggregation(const AggrOp aggregation);

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::AGGRGATE;
  }

  RC open(Trx *trx) override;
  RC   next() override;
  void NewFunction(std::vector<Value> &result_cells, int cell_idx, Value &cell);
  RC   close() override;

  Tuple *current_tuple() override {return &result_tuple_; }

private:
  std::vector<AggrOp> aggregations_;
  ValueListTuple result_tuple_;
  Trx *trx_ = nullptr;
  
};
