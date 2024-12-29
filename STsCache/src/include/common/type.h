#pragma once
#ifndef TYPE_H
#define TYPE_H
#include <cstdint>

namespace SemanticCache {

#define PROJECT_ROOT_PATH std::string(PROJECT_ROOT)

#define INVALID_ID -1

#define EXEC_RET_TYPE int
#define EXEC_SUCC 0
#define EXEC_FAIL -1

#define DEFAULT_TABLENAME "test"
#define AGG_DEFAULT_NAME "agg#"
#define AGG_DEFAULT_NAME_LEN 4

#define COLD_HOT_COL_NAME "ColdHot"

enum OP_TYPE {
  VAL = 0,
  FD = 1,
  GT = 2,
  GE = 3,
  LT = 4,
  LE = 5,
  EQ = 6,
  NE = 7,
  AND = 8,
  OR = 9,
  UnaryMinus = 10,
};

enum DATA_TYPE {
  INVALID_DATA_TYPE = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  VARCHAR,
  TIMESTAMP
};

#define SHUFFLE_ID int

enum DB_TYPE {
  INVALID_DB_TYPE = -1,
  REDIS = 0,
  SQLITE = 1,
  OPENGAUSS = 2,
};

enum SIDE_TYPE {
  CLOUD_SIDE,
  END_SIDE,
};

enum class AggregationType {
  CountStarAggregate,
  CountAggregate,
  SumAggregate,
  MinAggregate,
  MaxAggregate
};

enum class JoinType : uint8_t {
  INVALID = 0,
  LEFT = 1,
  RIGHT = 3,
  INNER = 4,
  OUTER = 5
};

enum class OrderByType : uint8_t {
  INVALID = 0,
  DEFAULT = 1,
  ASC = 2,
  DESC = 3,
};

enum class ExpressionType : uint8_t {
  INVALID = 0,
  CONSTANT = 1,
  COLUMN_REF = 3,
  TYPE_CAST = 4,
  FUNCTION = 5,
  AGG_CALL = 6,
  STAR = 7,
  UNARY_OP = 8,
  BINARY_OP = 9,
  ALIAS = 10,
};

}

#endif

#pragma once