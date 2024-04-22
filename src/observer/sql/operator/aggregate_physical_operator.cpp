
#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/delete_stmt.h"
#include <iostream>
#include <cctype> // 包含头文件
#include <ctime> // 需要包含头文件以使用 strptime
#include<cstring>


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
 int avg_count = 0; // 初始化 avg_count

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
                    }else if (attr_type == AttrType::CHARS) {
                        std::string current_str = cell.get_string();
                        std::string max_str = result_cells[cell_idx].get_string();
                        char * arr=new char[1000];
                        for(int i=0;i<current_str.size();i++){
                          if('a' <= current_str[i] && current_str[i] <= 'z')
                          arr[i]=current_str[i]-32;
                          else arr[i]=current_str[i];
                        }
                        if (current_str > max_str) {
                            result_cells[cell_idx].set_string(arr);
                            free(arr);
                        }
                    }
                    else if (attr_type == AttrType::DATES) {
        std::string current_date_str = cell.get_string(); // 假设返回的是日期字符串
        std::string max_date_str = result_cells[cell_idx].get_string(); // 假设返回的是日期字符串

        // 使用 strptime 解析日期字符串为 tm 结构体
        struct tm current_tm, max_tm;
        strptime(current_date_str.c_str(), "%Y-%m-%d", &current_tm);
        strptime(max_date_str.c_str(), "%Y-%m-%d", &max_tm);

        // 将 tm 结构体转换为 time_t 类型
        time_t current_time = mktime(&current_tm);
        time_t max_time = mktime(&max_tm);

        if (current_time > max_time) {
            char max_date_char[current_date_str.length() + 1];
            std::strcpy(max_date_char, current_date_str.c_str());
            result_cells[cell_idx].set_string(max_date_char);
        }
    }
                   
                    
                 break;
          case AggrOp::AGGR_MIN:
              rc = tuple->cell_at(cell_idx, cell);
                    attr_type = cell.attr_type();
                    if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {
                         if (result_cells[cell_idx].length()==0) {
                           result_cells[cell_idx] = cell;
                           } else {
            // 否则，如果当前单元格值小于之前保存的最小值，则更新最小值
                   if (cell.get_float() < result_cells[cell_idx].get_float()) {
                    result_cells[cell_idx].set_float(cell.get_float());
                  }
                 }
                    }
                    else if (attr_type == AttrType::CHARS) {
                      if (result_cells[cell_idx].length()==0) {
                           result_cells[cell_idx] = cell;
                           string sr=cell.get_string();
                           char* srr=new char[1000];
                           for(int j=0;j<sr.size();j++){
                            if('a'<=sr[j]&&sr[j]<='z'){
                              srr[j]=sr[j]-32;
                            }
                            else {srr[j]=sr[j];
                            
                            }
                           }
                           srr[sr.size()] = '\0'; // 添加终止符
                            result_cells[cell_idx].set_string(srr);
                            delete[]srr;
                           }
                          else{
                        std::string current_str = cell.get_string();
                        std::string min_str = result_cells[cell_idx].get_string();
                        char * brr=new char[1000];
                        for(int i=0;i<current_str.size();i++){
                           if('a' <= current_str[i] && current_str[i] <= 'z')
                          brr[i]=current_str[i]-32;
                          else brr[i]=current_str[i];
                        }
                        if (current_str < min_str) {
                            brr[current_str.size()] = '\0'; // 添加终止符
                            result_cells[cell_idx].set_string(brr);
                            delete[]brr;
                        }
                           }
                    }
                    else if (attr_type == AttrType::DATES) {
                        std::string current_date_str = cell.get_string();
                        std::string min_date_str = result_cells[cell_idx].get_string();

                        if (min_date_str.empty() || current_date_str < min_date_str) {
                            char min_date_char[current_date_str.length() + 1];
                            std::strcpy(min_date_char, current_date_str.c_str());
                            result_cells[cell_idx].set_string(min_date_char);
                        }
                    }

                    break;
          case AggrOp::AGGR_COUNT:
              result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
                    break;
          case AggrOp::AGGR_COUNT_ALL:
              result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
                    break;
          case AggrOp::AGGR_AVG:
                rc = tuple->cell_at(cell_idx, cell);
                    attr_type = cell.attr_type();
                    
                if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {
                  if (result_cells[cell_idx].length()==0) {
                    result_cells[cell_idx] = cell;
                    
             } else {
            // 否则，实现求和逻辑
             result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
             }
        
              avg_count++;
           }
            break;
              
        
        default:
            return RC::UNIMPLENMENT;
                }
    }
 }
     if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
    }

 if(rc==RC::RECORD_EOF){
    rc=RC::SUCCESS;
 }

for (size_t cell_idx = 0; cell_idx < aggregations_.size(); cell_idx++) {
        if (aggregations_[cell_idx] == AggrOp::AGGR_AVG) {
            if (avg_count > 0) {
                result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() / avg_count);
            }
        }
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
