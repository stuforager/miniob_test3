/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include <sstream>
#include "sql/expr/tuple_cell.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"

/*TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias)
{
  std::cout<<"b1";
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if (alias) {
    alias_ = alias;
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
    } else {
      alias_ = table_name_ + "." + field_name_;
    }
  }
}

TupleCellSpec::TupleCellSpec(const char *alias)
{
  std::cout<<"b2";
  if (alias) {
    alias_ = alias;
  }
}*/

void aggr_to_string(AggrOp aggr, std::string &aggr_repr) {
    switch (aggr) {
        case AggrOp::AGGR_COUNT:
            aggr_repr = "COUNT";
            break;
        case AggrOp::AGGR_SUM:
            aggr_repr = "SUM";
            break;
        case AggrOp::AGGR_AVG:
            aggr_repr = "AVG";
            break;
        case AggrOp::AGGR_MAX:
            aggr_repr = "MAX";
            break;
        case AggrOp::AGGR_MIN:
            aggr_repr = "MIN";
            break;
    }
}
TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias,const AggrOp aggr)
{
  
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if(aggr){
    aggr_=aggr;
  }
  if (alias) {
     for (char &c : alias_) {
     c = std::toupper(c);
      }
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
      for (char &c : alias_) {
     c = std::toupper(c);
      }
    } else {
      alias_ = table_name_ + "." + field_name_;
      for (char &c : alias_) {
     c = std::toupper(c);
      }
    }

    if(aggr_==AggrOp::AGGR_COUNT_ALL){
      alias_="COUNT(*)";
    }
    else if(aggr_!=AggrOp::AGGR_NODE){
      std::string aggr_repr;
      aggr_to_string(aggr_,aggr_repr);
      alias_=aggr_repr+"("+alias_+")";
      for (char &c : alias_) {
     c = std::toupper(c);
      }
    }
  }
}

TupleCellSpec::TupleCellSpec(const char *alias,const AggrOp aggr)
{

  if(aggr){
    aggr_=aggr;
  }
  if (alias) {
    alias_ = alias;
    for (char &c : alias_) {
     c = std::toupper(c);
      }
    if(aggr_==AggrOp::AGGR_COUNT_ALL){
      alias_="COUNT(*)";
    }
    else if(aggr_!=AggrOp::AGGR_NODE){
      std::string aggr_repr;
      aggr_to_string(aggr_,aggr_repr);
      alias_=aggr_repr+"("+alias_+")";
      for (char &c : alias_) {
     c = std::toupper(c);
      }
    }

  }
}
