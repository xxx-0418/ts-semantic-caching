#include <algorithm>
#include <cstdarg>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/util/string_util.h"

namespace SemanticCache {

auto StringUtil::Contains(const std::string &haystack, const std::string &needle) -> bool {
  return (haystack.find(needle) != std::string::npos);
}

void StringUtil::RTrim(std::string *str) {
  str->erase(
      std::find_if(str->rbegin(), str->rend(), [](int ch) { return std::isspace(ch) == 0; }).base(),
      str->end());
}
auto StringUtil::EndsWith(const std::string &str, const std::string &suffix) -> bool {
  if (suffix.size() > str.size()) {
    return false;
  }
  return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

auto StringUtil::Repeat(const std::string &str, const std::size_t n) -> std::string {
  std::ostringstream os;
  if (n == 0 || str.empty()) {
    return (os.str());
  }
  for (int i = 0; i < static_cast<int>(n); i++) {
    os << str;
  }
  return (os.str());
}

auto StringUtil::Join(const std::vector<std::string> &input, const std::string &separator)
    -> std::string {
  std::string result;

  if (!input.empty()) {
    result += input[0];
  }

  for (uint32_t i = 1; i < input.size(); i++) {
    result += separator + input[i];
  }

  return result;
}

auto StringUtil::Prefix(const std::string &str, const std::string &prefix) -> std::string {
  std::vector<std::string> lines = StringUtil::Split(str, '\n');

  if (lines.empty()) {
    return ("");
  }

  std::ostringstream os;
  for (uint64_t i = 0, cnt = lines.size(); i < cnt; i++) {
    if (i > 0) {
      os << std::endl;
    }
    os << prefix << lines[i];
  }
  return (os.str());
}

auto StringUtil::FormatSize(uint64_t bytes) -> std::string {
  double base = 1024;
  double kb = base;
  double mb = kb * base;
  double gb = mb * base;

  std::ostringstream os;

  if (bytes >= gb) {
    os << std::fixed << std::setprecision(2) << (bytes / gb) << " GB";
  } else if (bytes >= mb) {
    os << std::fixed << std::setprecision(2) << (bytes / mb) << " MB";
  } else if (bytes >= kb) {
    os << std::fixed << std::setprecision(2) << (bytes / kb) << " KB";
  } else {
    os << std::to_string(bytes) + " bytes";
  }
  return (os.str());
}

auto StringUtil::Bold(const std::string &str) -> std::string {
  std::string set_plain_text = "\033[0;0m";
  std::string set_bold_text = "\033[0;1m";

  std::ostringstream os;
  os << set_bold_text << str << set_plain_text;
  return (os.str());
}

auto StringUtil::Upper(const std::string &str) -> std::string {
  std::string copy(str);
  std::transform(copy.begin(), copy.end(), copy.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return (copy);
}

auto StringUtil::Lower(const std::string &str) -> std::string {
  std::string copy(str);
  std::transform(copy.begin(), copy.end(), copy.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return (copy);
}

std::string StringUtil::Format(std::string fmt_str, ...) {
  int final_n;
  int n = (static_cast<int>(fmt_str.size())) * 2;
  std::string str;
  std::unique_ptr<char[]> formatted;
  va_list ap;

  while (true) {
    formatted = std::make_unique<char[]>(n);
    strcpy(&formatted[0], fmt_str.c_str());
    va_start(ap, fmt_str);
    final_n = vsnprintf(&formatted[0], static_cast<size_t>(n), fmt_str.c_str(), ap);
    va_end(ap);
    if (final_n < 0 || final_n >= n) {
      n += abs(final_n - n + 1);
    } else {
      break;
    }
  }
  return {formatted.get()};
}

auto StringUtil::Strip(const std::string &str, char c) -> std::string {
  std::string tmp = str;
  tmp.erase(std::remove(tmp.begin(), tmp.end(), c), tmp.end());
  return tmp;
}

auto StringUtil::Replace(std::string source, const std::string &from, const std::string &to)
    -> std::string {
  uint64_t start_pos = 0;
  while ((start_pos = source.find(from, start_pos)) != std::string::npos) {
    source.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
  return source;
}

}