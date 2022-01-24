#include "custom_table.h"
#include <cstring>

namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  // 存储两份数据，按照列、行各存储一份
  // 读取的时候，根据查询的需求从行、列取数据
  // 来减小缓存命中失败的次数
  num_cols_ = loader->GetNumCols();
  std::vector<char *> rows = loader->GetRows();
  num_rows_ = rows.size();
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];

  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  int32_t res = 0;
  std::memcpy(&res, offset + rows_, FIXED_FIELD_LEN);
  return res;
}

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  std::memcpy(offset + rows_, &field, FIXED_FIELD_LEN);
}

// Implements the query
// SELECT SUM(col0) FROM table;
// Returns the sum of all elements in the first column of the table.
int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for(int offset = 0; offset < num_rows_; ++offset){
      int32_t tmp = 0;
      std::memcpy(&tmp, columns_ + FIXED_FIELD_LEN * offset, FIXED_FIELD_LEN);
      res += tmp;
  }
  return res;
}

// Implements the query
// SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
// Returns the sum of all elements in the first column of the table,
// subject to the passed-in predicates.
int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int64_t col1 = 0, col2 = 0;
      std::memcpy(&col1, columns_ + FIXED_FIELD_LEN * (num_rows_ * 1 + row_id), FIXED_FIELD_LEN);
      std::memcpy(&col2, columns_ + FIXED_FIELD_LEN * (num_rows_ * 2 + row_id), FIXED_FIELD_LEN);
      if(col1 > threshold1 && col2 < threshold2){
        int32_t tmp = 0;
        std::memcpy(&tmp, columns_ + FIXED_FIELD_LEN * row_id, FIXED_FIELD_LEN);
        res += tmp;
      }
  }
  return res;
}


// Implements the query
// SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
// threshold; Returns the sum of all elements in the rows which pass the
// predicate.
int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col0 = 0;
      std::memcpy(&col0, rows_ + row_id * num_cols_ * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
      if( col0 > threshold ){
          auto cur = rows_ + row_id * num_cols_ * FIXED_FIELD_LEN;
          for(int32_t col_id = 0; col_id < num_cols_; ++col_id){
              int32_t tmp = 0;
              std::memcpy(&tmp, cur + col_id * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
              sum += tmp;
          }
      }
  }
  return sum;
}

// Implements the query
// UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
// Returns the number of rows updated.
int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col0 = 0;
      std::memcpy(&col0, rows_ + row_id * num_cols_ * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
      if( col0 < threshold ){
          int32_t col2 = 0, col3 = 0;
          std::memcpy(&col2, rows_ + (row_id * num_cols_ + 2) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
          std::memcpy(&col3, rows_ + (row_id * num_cols_ + 3) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
          col3 += col2;
          std::memcpy(rows_ + (row_id * num_cols_ + 3) * FIXED_FIELD_LEN, &col3, FIXED_FIELD_LEN);
          ++cnt;
      }
  }
  return cnt;
}
} // namespace bytedance_db_project