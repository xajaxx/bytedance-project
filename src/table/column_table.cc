#include "column_table.h"
#include <cstring>
#include <iostream>

namespace bytedance_db_project {
ColumnTable::ColumnTable() {}

ColumnTable::~ColumnTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
}

//
// columnTable, which stores data in column-major format.
// That is, data is laid out like
//   col 1 | col 2 | ... | col m.
//
void ColumnTable::Load(BaseDataLoader *loader) {
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
}

int32_t ColumnTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
  int32_t res = 0;
  std::memcpy(&res, columns_ + offset, FIXED_FIELD_LEN);
  return res;
}

void ColumnTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
  std::memcpy(columns_ + offset, &field, FIXED_FIELD_LEN);
}

// Implements the query
// SELECT SUM(col0) FROM table;
// Returns the sum of all elements in the first column of the table.
int64_t ColumnTable::ColumnSum() {
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
int64_t ColumnTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col1 = 0, col2 = 0;
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
int64_t ColumnTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col0 = 0;
      std::memcpy(&col0, columns_ + FIXED_FIELD_LEN * row_id, FIXED_FIELD_LEN);
      if( col0 > threshold ) {
          for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
              int32_t tmp = 0;
              std::memcpy(&tmp, columns_ + (col_id * num_rows_ + row_id) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
              res += tmp;
          }
      }
  }
  return res;
}

// Implements the query
// UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
// Returns the number of rows updated.
int64_t ColumnTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!     
  int64_t cnt = 0;
  for(int32_t row_id = 0; row_id < num_rows_; ++row_id){
      int32_t col0 = 0;
      std::memcpy(&col0, columns_ +  row_id * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
      if( col0 < threshold ){
          int32_t num_2 = 0, num_3 = 0;
          std::memcpy(&num_2, columns_ + (num_rows_ * 2 + row_id) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
          std::memcpy(&num_3, columns_ + (num_rows_ * 3 + row_id) * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
          num_3 += num_2;
          std::memcpy(columns_ + (num_rows_ * 3 + row_id) * FIXED_FIELD_LEN, &num_3, FIXED_FIELD_LEN);
          ++cnt;
      }
  }
  return cnt;
}
} // namespace bytedance_db_project