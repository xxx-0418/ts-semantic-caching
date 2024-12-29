#include <cassert>
#include <cmath>

#include "type/integer_parent_type.h"

namespace SemanticCache
{
  IntegerParentType::IntegerParentType(DATA_TYPE type) : NumericType(type) {}

  auto IntegerParentType::Min(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    if (left.CompareLessThan(right) == CmpBool::CmpTrue)
    {
      return left.Copy();
    }
    return right.Copy();
  }

  auto IntegerParentType::Max(const Value &left, const Value &right) const -> Value
  {
    assert(left.CheckInteger());
    assert(left.CheckComparable(right));
    if (left.IsNull() || right.IsNull())
    {
      return left.OperateNull(right);
    }

    if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue)
    {
      return left.Copy();
    }
    return right.Copy();
  }
}
