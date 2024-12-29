//===----------------------------------------------------------------------===//
//
//                         SemanticCache
//
// statement_type.h
//
// Identification: src/include/enums/statement_type.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace SemanticCache {

enum class StatementType : uint8_t {
  INVALID_STATEMENT,
  SELECT_STATEMENT,
  INSERT_STATEMENT,
  UPDATE_STATEMENT,
  CREATE_STATEMENT,
  DELETE_STATEMENT,
  EXPLAIN_STATEMENT,
  DROP_STATEMENT,
  INDEX_STATEMENT,
  VARIABLE_SET_STATEMENT,
  VARIABLE_SHOW_STATEMENT,
};

}