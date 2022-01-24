#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  index_column_ = index_column;
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  // vector<char *> rows
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    int32_t tmp = 0;
    std::memcpy(&tmp, cur_row + (index_column_ * FIXED_FIELD_LEN), FIXED_FIELD_LEN);
    index_[tmp].emplace_back(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row, FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  int32_t res = 0;
  std::memcpy(&res, offset + rows_, FIXED_FIELD_LEN);
  return res;
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  std::memcpy(offset + rows_, &field, FIXED_FIELD_LEN);
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t sum = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t tmp = 0;
      std::memcpy(&tmp, row_id * num_cols_ * FIXED_FIELD_LEN + rows_, FIXED_FIELD_LEN);
      sum += tmp;
  }
  return sum;
}

// Implements the query
// SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
// Returns the sum of all elements in the first column of the table,
// subject to the passed-in predicates.
int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col1 = 0, col2 = 0;
      std::memcpy(&col1, (row_id * num_cols_ + 1) * FIXED_FIELD_LEN + rows_, FIXED_FIELD_LEN);
      std::memcpy(&col2, (row_id * num_cols_ + 2) * FIXED_FIELD_LEN + rows_, FIXED_FIELD_LEN);
      if( col1 > threshold1 && col2 < threshold2 ){
          int32_t tmp = 0;
          std::memcpy(&tmp, row_id * num_cols_ * FIXED_FIELD_LEN + rows_, FIXED_FIELD_LEN);
          sum += tmp;
      }
  }
  return sum;
}

// Implements the query
// SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
// threshold; Returns the sum of all elements in the rows which pass the
// predicate.
int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  std::unordered_map<int32_t, std::vector<int32_t>>::iterator it;
  for( it = index_.begin(); it != index_.end(); ++it){
      int32_t k = it->first;
      if(k > threshold){
        std::vector<int32_t> row_list = it->second;
        for(auto row_id :row_list){
          for(int32_t col_id = 0; col_id < num_cols_; ++col_id){
            int32_t tmp = 0;
            std::memcpy(&tmp, rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
            sum += tmp;
          }
        }
      }
  }
  return sum;
}

// Implements the query
// UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
// Returns the number of rows updated.
int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  std::unordered_map<int32_t, std::vector<int32_t>>::iterator it;
  for( it = index_.begin(); it != index_.end(); ++it){
      int32_t k = it->first;
      if(k < threshold){
          std::vector<int32_t> row_list = it->second;
          cnt += row_list.size();
          for(auto row_id :row_list){
              int32_t col2 = 0, col3 = 0;
              std::memcpy(&col2, rows_ + (row_id * num_cols_ + 2) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
              std::memcpy(&col3, rows_ + (row_id * num_cols_ + 3) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
              col3 += col2;
              std::memcpy(rows_ + (row_id * num_cols_ + 3) * FIXED_FIELD_LEN, &col3, FIXED_FIELD_LEN);
          }
      }
  }
  return cnt;
}
} // namespace bytedance_db_project