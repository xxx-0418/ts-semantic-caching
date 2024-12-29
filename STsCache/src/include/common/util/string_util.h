#pragma once
#ifndef STRING_UTIL_H
#define STRING_UTIL_H
#include <sstream>
#include <string>
#include <vector>
#include "common/exception.h"
#include "common/type.h"
#include "type/compare_type.h"
#include "type/value.h"

namespace SemanticCache {

class StringUtil {
 public:
  static auto Contains(const std::string &haystack, const std::string &needle) -> bool;

  static auto StartsWith(const std::string &str, const std::string &prefix) -> bool {
    return std::equal(prefix.begin(), prefix.end(), str.begin());
  }

  static auto EndsWith(const std::string &str, const std::string &suffix) -> bool;

  static auto Repeat(const std::string &str, std::size_t n) -> std::string;

  static auto Split(const std::string &str, char delimiter) -> std::vector<std::string> {
    std::stringstream ss(str);
    std::vector<std::string> lines;
    std::string temp;
    while (std::getline(ss, temp, delimiter)) {
      lines.push_back(temp);
    }
    return (lines);
  }

  static auto Join(const std::vector<std::string> &input, const std::string &separator)
      -> std::string;

  template <typename C, typename S, typename Func>
  static auto Join(const C &input, S count, const std::string &separator, Func f) -> std::string {
    std::string result;

    if (count > 0) {
      result += f(input[0]);
    }

    for (size_t i = 1; i < count; i++) {
      result += separator + f(input[i]);
    }

    return result;
  }

  static auto Prefix(const std::string &str, const std::string &prefix) -> std::string;

  static auto FormatSize(uint64_t bytes) -> std::string;

  static auto Bold(const std::string &str) -> std::string;

  static auto Upper(const std::string &str) -> std::string;

  static auto Lower(const std::string &str) -> std::string;

  static auto Format(std::string fmt_str, ...) -> std::string;

  static auto Split(const std::string &input, const std::string &split)
      -> std::vector<std::string> {
    std::vector<std::string> splits;

    size_t last = 0;
    size_t input_len = input.size();
    size_t split_len = split.size();
    while (last <= input_len) {
      size_t next = input.find(split, last);
      if (next == std::string::npos) {
        next = input_len;
      }

      std::string substr = input.substr(last, next - last);
      if (!substr.empty()) {
        splits.push_back(substr);
      }
      last = next + split_len;
    }
    return splits;
  }

  static void RTrim(std::string *str);

  static auto Indent(int num_indent) -> std::string { return std::string(num_indent, ' '); }

  static auto Strip(const std::string &str, char c) -> std::string;

  static auto Replace(std::string source, const std::string &from, const std::string &to)
      -> std::string;

  static auto IndentAllLines(const std::string &lines, size_t num_indent,
                             bool except_first_line = false) -> std::string {
    std::vector<std::string> lines_str;
    auto lines_split = StringUtil::Split(lines, '\n');
    lines_str.reserve(lines_split.size());
    auto indent_str = StringUtil::Indent(num_indent);
    bool is_first_line = true;
    for (auto &line : lines_split) {
      if (is_first_line) {
        is_first_line = false;
        if (except_first_line) {
          lines_str.push_back(line);
          continue;
        }
      }
      lines_str.push_back("{" + indent_str + "}{" + line + "}");
    }
    std::string s = "{";
    for (auto &item : lines_str) {
      s.append(item + "\n");
    }
    return s + "}\n";
  }

  static auto ToString(const OP_TYPE &op_type) -> std::string {
    std::string str = "";
    switch (op_type) {
      case OP_TYPE::VAL:
        str.append("VAL");
        break;
      case OP_TYPE::FD:
        str.append("FD");
        break;
      case OP_TYPE::GT:
        str.append(">");
        break;
      case OP_TYPE::GE:
        str.append(">=");
        break;
      case OP_TYPE::LT:
        str.append("<");
        break;
      case OP_TYPE::LE:
        str.append("<=");
        break;
      case OP_TYPE::EQ:
        str.append("=");
        break;
      case OP_TYPE::NE:
        str.append("!");
        break;
      case OP_TYPE::AND:
        str.append("and");
        break;
      case OP_TYPE::OR:
        str.append("or");
        break;
      case OP_TYPE::UnaryMinus:
        str.append("-");
        break;
      default:
        throw Exception("StringUtil ToString(op type): unsupport op type\n");
    }
    return str;
  }

  static auto ToString(const DATA_TYPE &data_type) -> std::string {
    switch (data_type) {
      case DATA_TYPE::BOOLEAN:
        return "BOOLEAN";
      case DATA_TYPE::TINYINT:
        return "TINYINT";
      case DATA_TYPE::SMALLINT:
        return "SMALLINT";
      case DATA_TYPE::INTEGER:
        return "INTEGER";
      case DATA_TYPE::BIGINT:
        return "BIGINT";
      case DATA_TYPE::DECIMAL:
        return "DECIMAL";
      case DATA_TYPE::TIMESTAMP:
        return "TIMESTAMP";
      case DATA_TYPE::VARCHAR:
        return "VARCHAR";
      default:
        throw Exception("StringUtil ToString(data type): unsupport op type\n");
    }
  }

  static auto StringToOpType(const std::string &op_name) -> OP_TYPE {
    if (op_name.compare("VAL") == 0 || op_name.compare("val") == 0) {
      return OP_TYPE::VAL;
    }
    if (op_name.compare("FD") == 0 || op_name.compare("fd") == 0) {
      return OP_TYPE::FD;
    }
    if (op_name.compare(">") == 0) {
      return OP_TYPE::GT;
    }
    if (op_name.compare(">=") == 0) {
      return OP_TYPE::GE;
    }
    if (op_name.compare("<") == 0) {
      return OP_TYPE::LT;
    }
    if (op_name.compare("<=") == 0) {
      return OP_TYPE::LE;
    }
    if (op_name.compare("=") == 0 || op_name.compare("==") == 0) {
      return OP_TYPE::EQ;
    }
    if (op_name.compare("!") == 0) {
      return OP_TYPE::NE;
    }
    if (op_name.compare("AND") == 0 || op_name.compare("and") == 0) {
      return OP_TYPE::AND;
    }
    if (op_name.compare("OR") == 0 || op_name.compare("or") == 0) {
      return OP_TYPE::OR;
    }
    if (op_name == "-") {
      return OP_TYPE::UnaryMinus;
    }
    throw Exception("StringUtil StringToOpType(op type): unsupport op type\n");
  }

  static auto ToString(const AggregationType &type) -> std::string {
    switch (type) {
      case AggregationType::CountStarAggregate:
        return "count_star";
      case AggregationType::MinAggregate:
        return "min";
      case AggregationType::MaxAggregate:
        return "max";
      case AggregationType::SumAggregate:
        return "sum";
      case AggregationType::CountAggregate:
        return "count";
      default:
        throw Exception("StringUtil ToString(AggregationType): unsupport agg type\n");
    }
  }

  static auto StringToAggType(const std::string &agg_name) -> AggregationType {
    if (agg_name.compare("count_star") == 0 || agg_name.compare("COUNT_START") == 0) {
      return AggregationType::CountStarAggregate;
    }
    if (agg_name.compare("min") == 0 || agg_name.compare("MIN") == 0) {
      return AggregationType::MinAggregate;
    }
    if (agg_name.compare("max") == 0 || agg_name.compare("MAX") == 0) {
      return AggregationType::MaxAggregate;
    }
    if (agg_name.compare("sum") == 0 || agg_name.compare("SUM") == 0) {
      return AggregationType::SumAggregate;
    }
    if (agg_name.compare("count") == 0 || agg_name.compare("COUNT") == 0) {
      return AggregationType::CountAggregate;
    }
    throw Exception("StringUtil StringToAggType(agg type): unsupport agg type\n");
  }

  static auto ToString(const JoinType &join_type) -> std::string {
    std::string name;
    switch (join_type) {
      case JoinType::INVALID:
        name = "Invalid";
        break;
      case JoinType::LEFT:
        name = "left join";
        break;
      case JoinType::RIGHT:
        name = "right join";
        break;
      case JoinType::INNER:
        name = "inner join";
        break;
      case JoinType::OUTER:
        name = "outer join";
        break;
      default:
        name = "Unknown";
        break;
    }
    return name;
  }

  static auto ToString(const OrderByType &type) -> std::string {
    std::string name;
    switch (type) {
      case OrderByType::INVALID:
        name = "invalid";
        break;
      case OrderByType::ASC:
        name = "asc";
        break;
      case OrderByType::DEFAULT:
        name = "default";
        break;
      case OrderByType::DESC:
        name = "desc";
        break;
      default:
        name = "Unknown";
        break;
    }
    return name;
  }
};
}

#endif