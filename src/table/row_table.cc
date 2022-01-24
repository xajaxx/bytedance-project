#include "row_table.h"
#include <cstring>

namespace bytedance_db_project {
RowTable::RowTable() {}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  // vector<char *> rows
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

// Returns the int32_t field at row `row_id` and column `col_id`.
int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  int32_t res = 0;
  std::memcpy(&res, offset + rows_, FIXED_FIELD_LEN);
  return res;
}

// Inserts the passed-in int32_t field at row `row_id` and column `col_id`.
void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  int32_t offset = FIXED_FIELD_LEN * (row_id * num_cols_ + col_id);
  std::memcpy(offset + rows_, &field, FIXED_FIELD_LEN);
}

// Implements the query
// SELECT SUM(col0) FROM table;
// Returns the sum of all elements in the first column of the table.
int64_t RowTable::ColumnSum() {
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
int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
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
int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
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
int64_t RowTable::PredicatedUpdate(int32_t threshold) {
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