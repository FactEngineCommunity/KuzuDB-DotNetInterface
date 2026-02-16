#pragma once

// Helpers
#if defined _WIN32 || defined __CYGWIN__
#define KUZU_HELPER_DLL_IMPORT __declspec(dllimport)
#define KUZU_HELPER_DLL_EXPORT __declspec(dllexport)
#define KUZU_HELPER_DLL_LOCAL
#define KUZU_HELPER_DEPRECATED __declspec(deprecated)
#else
#define KUZU_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define KUZU_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#define KUZU_HELPER_DEPRECATED __attribute__((__deprecated__))
#endif

#ifdef KUZU_STATIC_DEFINE
#define KUZU_API
#else
#ifndef KUZU_API
#ifdef KUZU_EXPORTS
/* We are building this library */
#define KUZU_API KUZU_HELPER_DLL_EXPORT
#else
/* We are using this library */
#define KUZU_API KUZU_HELPER_DLL_IMPORT
#endif
#endif
#endif

#ifndef KUZU_DEPRECATED
#define KUZU_DEPRECATED KUZU_HELPER_DEPRECATED
#endif

#ifndef KUZU_DEPRECATED_EXPORT
#define KUZU_DEPRECATED_EXPORT KUZU_API KUZU_DEPRECATED
#endif
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>
// This file defines many macros for controlling copy constructors and move constructors on classes.

// NOLINTBEGIN(bugprone-macro-parentheses): Although this is a good check in general, here, we
// cannot add parantheses around the arguments, for it would be invalid syntax.
#define DELETE_COPY_CONSTRUCT(Object) Object(const Object& other) = delete
#define DELETE_COPY_ASSN(Object) Object& operator=(const Object& other) = delete

#define DELETE_MOVE_CONSTRUCT(Object) Object(Object&& other) = delete
#define DELETE_MOVE_ASSN(Object) Object& operator=(Object&& other) = delete

#define DELETE_BOTH_COPY(Object)                                                                   \
    DELETE_COPY_CONSTRUCT(Object);                                                                 \
    DELETE_COPY_ASSN(Object)

#define DELETE_BOTH_MOVE(Object)                                                                   \
    DELETE_MOVE_CONSTRUCT(Object);                                                                 \
    DELETE_MOVE_ASSN(Object)

#define DEFAULT_MOVE_CONSTRUCT(Object) Object(Object&& other) = default
#define DEFAULT_MOVE_ASSN(Object) Object& operator=(Object&& other) = default

#define DEFAULT_BOTH_MOVE(Object)                                                                  \
    DEFAULT_MOVE_CONSTRUCT(Object);                                                                \
    DEFAULT_MOVE_ASSN(Object)

#define EXPLICIT_COPY_METHOD(Object)                                                               \
    Object copy() const {                                                                          \
        return *this;                                                                              \
    }

// EXPLICIT_COPY_DEFAULT_MOVE should be the default choice. It expects a PRIVATE copy constructor to
// be defined, which will be used by an explicit `copy()` method. For instance:
//
//   private:
//     MyClass(const MyClass& other) : field(other.field.copy()) {}
//
//   public:
//     EXPLICIT_COPY_DEFAULT_MOVE(MyClass);
//
// Now:
//
// MyClass o1;
// MyClass o2 = o1; // Compile error, copy assignment deleted.
// MyClass o2 = o1.copy(); // OK.
// MyClass o2(o1); // Compile error, copy constructor is private.
#define EXPLICIT_COPY_DEFAULT_MOVE(Object)                                                         \
    DELETE_COPY_ASSN(Object);                                                                      \
    DEFAULT_BOTH_MOVE(Object);                                                                     \
    EXPLICIT_COPY_METHOD(Object)

// NO_COPY should be used for objects that for whatever reason, should never be copied, but can be
// moved.
#define DELETE_COPY_DEFAULT_MOVE(Object)                                                           \
    DELETE_BOTH_COPY(Object);                                                                      \
    DEFAULT_BOTH_MOVE(Object)

// NO_MOVE_OR_COPY exists solely for explicitness, when an object cannot be moved nor copied. Any
// object containing a lock cannot be moved or copied.
#define DELETE_COPY_AND_MOVE(Object)                                                               \
    DELETE_BOTH_COPY(Object);                                                                      \
    DELETE_BOTH_MOVE(Object)
// NOLINTEND(bugprone-macro-parentheses):

template<typename T>
static std::vector<T> copyVector(const std::vector<T>& objects) {
    std::vector<T> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        result.push_back(object.copy());
    }
    return result;
}

template<typename T>
static std::vector<std::shared_ptr<T>> copyVector(const std::vector<std::shared_ptr<T>>& objects) {
    std::vector<std::shared_ptr<T>> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        T& ob = *object;
        result.push_back(ob.copy());
    }
    return result;
}

template<typename T>
static std::vector<std::unique_ptr<T>> copyVector(const std::vector<std::unique_ptr<T>>& objects) {
    std::vector<std::unique_ptr<T>> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        T& ob = *object;
        result.push_back(ob.copy());
    }
    return result;
}

template<typename K, typename V>
static std::unordered_map<K, V> copyUnorderedMap(const std::unordered_map<K, V>& objects) {
    std::unordered_map<K, V> result;
    for (auto& [k, v] : objects) {
        result.insert({k, v.copy()});
    }
    return result;
}

template<typename K, typename V>
static std::map<K, V> copyMap(const std::map<K, V>& objects) {
    std::map<K, V> result;
    for (auto& [k, v] : objects) {
        result.insert({k, v.copy()});
    }
    return result;
}
#include <string>

namespace kuzu {
namespace parser {

struct YieldVariable {
    std::string name;
    std::string alias;

    YieldVariable(std::string name, std::string alias)
        : name{std::move(name)}, alias{std::move(alias)} {}
    bool hasAlias() const { return alias != ""; }
};

} // namespace parser
} // namespace kuzu

#include <memory>
#include <string>

namespace kuzu {

struct OPPrintInfo {
    OPPrintInfo() {}
    virtual ~OPPrintInfo() = default;

    virtual std::string toString() const { return std::string(); }

    virtual std::unique_ptr<OPPrintInfo> copy() const { return std::make_unique<OPPrintInfo>(); }

    static std::unique_ptr<OPPrintInfo> EmptyInfo() { return std::make_unique<OPPrintInfo>(); }
};

} // namespace kuzu

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class PathSemantic : uint8_t {
    WALK = 0,
    TRAIL = 1,
    ACYCLIC = 2,
};

struct PathSemanticUtils {
    static PathSemantic fromString(const std::string& str);
    static std::string toString(PathSemantic semantic);
};

} // namespace common
} // namespace kuzu

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_TABLE = 1,
    DROP = 2,
    ALTER = 3,
    COPY_TO = 19,
    COPY_FROM = 20,
    STANDALONE_CALL = 21,
    STANDALONE_CALL_FUNCTION = 22,
    EXPLAIN = 23,
    CREATE_MACRO = 24,
    TRANSACTION = 30,
    EXTENSION = 31,
    EXPORT_DATABASE = 32,
    IMPORT_DATABASE = 33,
    ATTACH_DATABASE = 34,
    DETACH_DATABASE = 35,
    USE_DATABASE = 36,
    CREATE_SEQUENCE = 37,
    CREATE_TYPE = 39,
};

} // namespace common
} // namespace kuzu

#include <cstdint>

namespace kuzu {

namespace testing {
class BaseGraphTest;
class PrivateGraphTest;
class TestHelper;
class TestRunner;
} // namespace testing

namespace benchmark {
class Benchmark;
} // namespace benchmark

namespace binder {
class Expression;
class BoundStatementResult;
class PropertyExpression;
} // namespace binder

namespace catalog {
class Catalog;
} // namespace catalog

namespace common {
enum class StatementType : uint8_t;
class Value;
struct FileInfo;
class VirtualFileSystem;
} // namespace common

namespace storage {
class MemoryManager;
class BufferManager;
class StorageManager;
class WAL;
enum class WALReplayMode : uint8_t;
} // namespace storage

namespace planner {
class LogicalOperator;
class LogicalPlan;
} // namespace planner

namespace processor {
class QueryProcessor;
class FactorizedTable;
class FlatTupleIterator;
class PhysicalOperator;
class PhysicalPlan;
} // namespace processor

namespace transaction {
class Transaction;
class TransactionManager;
class TransactionContext;
} // namespace transaction

} // namespace kuzu

#include <algorithm>
#include <array>
#include <cstddef>

namespace kuzu::common {
template<typename T, size_t N1, size_t N2>
constexpr std::array<T, N1 + N2> arrayConcat(const std::array<T, N1>& arr1,
    const std::array<T, N2>& arr2) {
    std::array<T, N1 + N2> ret{};
    std::copy_n(arr1.cbegin(), arr1.size(), ret.begin());
    std::copy_n(arr2.cbegin(), arr2.size(), ret.begin() + arr1.size());
    return ret;
}
} // namespace kuzu::common

// The Arrow C data interface.
// https://arrow.apache.org/docs/format/CDataInterface.html

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;
};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;
};

#endif // ARROW_C_DATA_INTERFACE

#ifdef __cplusplus
}
#endif

struct ArrowSchemaWrapper : public ArrowSchema {
    ArrowSchemaWrapper() : ArrowSchema{} { release = nullptr; }
    ~ArrowSchemaWrapper() {
        if (release) {
            release(this);
        }
    }
};

struct ArrowArrayWrapper : public ArrowArray {
    ArrowArrayWrapper() : ArrowArray{} { release = nullptr; }
    ~ArrowArrayWrapper() {
        if (release) {
            release(this);
        }
    }
};

namespace kuzu {
namespace common {
struct DatabaseLifeCycleManager {
    bool isDatabaseClosed = false;
    void checkDatabaseClosedOrThrow() const;
};
} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string>


namespace kuzu {

namespace regex {
class RE2;
}

namespace common {

struct timestamp_t;
struct date_t;

enum class KUZU_API DatePartSpecifier : uint8_t {
    YEAR,
    MONTH,
    DAY,
    DECADE,
    CENTURY,
    MILLENNIUM,
    QUARTER,
    MICROSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    WEEK,
};

struct KUZU_API interval_t {
    int32_t months = 0;
    int32_t days = 0;
    int64_t micros = 0;

    interval_t();
    interval_t(int32_t months_p, int32_t days_p, int64_t micros_p);

    // comparator operators
    bool operator==(const interval_t& rhs) const;
    bool operator!=(const interval_t& rhs) const;

    bool operator>(const interval_t& rhs) const;
    bool operator<=(const interval_t& rhs) const;
    bool operator<(const interval_t& rhs) const;
    bool operator>=(const interval_t& rhs) const;

    // arithmetic operators
    interval_t operator+(const interval_t& rhs) const;
    timestamp_t operator+(const timestamp_t& rhs) const;
    date_t operator+(const date_t& rhs) const;
    interval_t operator-(const interval_t& rhs) const;

    interval_t operator/(const uint64_t& rhs) const;
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/interval.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/interval.cpp.
// When more functionality is needed, we should first consult these DuckDB links.
// The Interval class is a static class that holds helper functions for the Interval type.
class Interval {
public:
    static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
    static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
    static constexpr const int32_t MONTHS_PER_DECADE = 120;
    static constexpr const int32_t MONTHS_PER_YEAR = 12;
    static constexpr const int32_t MONTHS_PER_QUARTER = 3;
    static constexpr const int32_t DAYS_PER_WEEK = 7;
    //! only used for interval comparison/ordering purposes, in which case a month counts as 30 days
    static constexpr const int64_t DAYS_PER_MONTH = 30;
    static constexpr const int64_t DAYS_PER_YEAR = 365;
    static constexpr const int64_t MSECS_PER_SEC = 1000;
    static constexpr const int32_t SECS_PER_MINUTE = 60;
    static constexpr const int32_t MINS_PER_HOUR = 60;
    static constexpr const int32_t HOURS_PER_DAY = 24;
    static constexpr const int32_t SECS_PER_HOUR = SECS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int32_t SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int32_t SECS_PER_WEEK = SECS_PER_DAY * DAYS_PER_WEEK;

    static constexpr const int64_t MICROS_PER_MSEC = 1000;
    static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int64_t MICROS_PER_WEEK = MICROS_PER_DAY * DAYS_PER_WEEK;
    static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

    static constexpr const int64_t NANOS_PER_MICRO = 1000;
    static constexpr const int64_t NANOS_PER_MSEC = NANOS_PER_MICRO * MICROS_PER_MSEC;
    static constexpr const int64_t NANOS_PER_SEC = NANOS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t NANOS_PER_MINUTE = NANOS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t NANOS_PER_HOUR = NANOS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t NANOS_PER_DAY = NANOS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int64_t NANOS_PER_WEEK = NANOS_PER_DAY * DAYS_PER_WEEK;

    KUZU_API static void addition(interval_t& result, uint64_t number, std::string specifierStr);
    KUZU_API static interval_t fromCString(const char* str, uint64_t len);
    KUZU_API static std::string toString(interval_t interval);
    KUZU_API static bool greaterThan(const interval_t& left, const interval_t& right);
    KUZU_API static void normalizeIntervalEntries(interval_t input, int64_t& months, int64_t& days,
        int64_t& micros);
    KUZU_API static void tryGetDatePartSpecifier(std::string specifier, DatePartSpecifier& result);
    KUZU_API static int32_t getIntervalPart(DatePartSpecifier specifier, interval_t timestamp);
    KUZU_API static int64_t getMicro(const interval_t& val);
    KUZU_API static int64_t getNanoseconds(const interval_t& val);
    KUZU_API static const regex::RE2& regexPattern1();
    KUZU_API static const regex::RE2& regexPattern2();
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string>


namespace kuzu {
namespace common {

// Type used to represent time (microseconds)
struct KUZU_API dtime_t {
    int64_t micros;

    dtime_t();
    explicit dtime_t(int64_t micros_p);
    dtime_t& operator=(int64_t micros_p);

    // explicit conversion
    explicit operator int64_t() const;
    explicit operator double() const;

    // comparison operators
    bool operator==(const dtime_t& rhs) const;
    bool operator!=(const dtime_t& rhs) const;
    bool operator<=(const dtime_t& rhs) const;
    bool operator<(const dtime_t& rhs) const;
    bool operator>(const dtime_t& rhs) const;
    bool operator>=(const dtime_t& rhs) const;
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/time.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/time.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.
class Time {
public:
    // Convert a string in the format "hh:mm:ss" to a time object
    KUZU_API static dtime_t fromCString(const char* buf, uint64_t len);
    KUZU_API static bool tryConvertInterval(const char* buf, uint64_t len, uint64_t& pos,
        dtime_t& result);
    KUZU_API static bool tryConvertTime(const char* buf, uint64_t len, uint64_t& pos,
        dtime_t& result);

    // Convert a time object to a string in the format "hh:mm:ss"
    KUZU_API static std::string toString(dtime_t time);

    KUZU_API static dtime_t fromTime(int32_t hour, int32_t minute, int32_t second,
        int32_t microseconds = 0);

    // Extract the time from a given timestamp object
    KUZU_API static void convert(dtime_t time, int32_t& out_hour, int32_t& out_min,
        int32_t& out_sec, int32_t& out_micros);

    KUZU_API static bool isValid(int32_t hour, int32_t minute, int32_t second,
        int32_t milliseconds);

private:
    static bool tryConvertInternal(const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);
    static dtime_t fromTimeInternal(int32_t hour, int32_t minute, int32_t second,
        int32_t microseconds = 0);
};

} // namespace common
} // namespace kuzu
// =========================================================================================
// This int128 implementtaion got

// =========================================================================================

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>


namespace kuzu {
namespace common {

struct KUZU_API int128_t;

// System representation for int128_t.
struct int128_t {
    uint64_t low;
    int64_t high;

    int128_t() = default;
    int128_t(int64_t value);  // NOLINT: Allow implicit conversion from numeric values
    int128_t(int32_t value);  // NOLINT: Allow implicit conversion from numeric values
    int128_t(int16_t value);  // NOLINT: Allow implicit conversion from numeric values
    int128_t(int8_t value);   // NOLINT: Allow implicit conversion from numeric values
    int128_t(uint64_t value); // NOLINT: Allow implicit conversion from numeric values
    int128_t(uint32_t value); // NOLINT: Allow implicit conversion from numeric values
    int128_t(uint16_t value); // NOLINT: Allow implicit conversion from numeric values
    int128_t(uint8_t value);  // NOLINT: Allow implicit conversion from numeric values
    int128_t(double value);   // NOLINT: Allow implicit conversion from numeric values
    int128_t(float value);    // NOLINT: Allow implicit conversion from numeric values

    constexpr int128_t(uint64_t low, int64_t high) : low(low), high(high) {}

    constexpr int128_t(const int128_t&) = default;
    constexpr int128_t(int128_t&&) = default;
    int128_t& operator=(const int128_t&) = default;
    int128_t& operator=(int128_t&&) = default;

    int128_t operator-() const;

    // inplace arithmetic operators
    int128_t& operator+=(const int128_t& rhs);
    int128_t& operator*=(const int128_t& rhs);
    int128_t& operator|=(const int128_t& rhs);
    int128_t& operator&=(const int128_t& rhs);

    // cast operators
    explicit operator int64_t() const;
    explicit operator int32_t() const;
    explicit operator int16_t() const;
    explicit operator int8_t() const;
    explicit operator uint64_t() const;
    explicit operator uint32_t() const;
    explicit operator uint16_t() const;
    explicit operator uint8_t() const;
    explicit operator double() const;
    explicit operator float() const;
};

// arithmetic operators
KUZU_API int128_t operator+(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator-(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator*(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator/(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator%(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator^(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator&(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator~(const int128_t& val);
KUZU_API int128_t operator|(const int128_t& lhs, const int128_t& rhs);
KUZU_API int128_t operator<<(const int128_t& lhs, int amount);
KUZU_API int128_t operator>>(const int128_t& lhs, int amount);

// comparison operators
KUZU_API bool operator==(const int128_t& lhs, const int128_t& rhs);
KUZU_API bool operator!=(const int128_t& lhs, const int128_t& rhs);
KUZU_API bool operator>(const int128_t& lhs, const int128_t& rhs);
KUZU_API bool operator>=(const int128_t& lhs, const int128_t& rhs);
KUZU_API bool operator<(const int128_t& lhs, const int128_t& rhs);
KUZU_API bool operator<=(const int128_t& lhs, const int128_t& rhs);

class Int128_t {
public:
    static std::string ToString(int128_t input);

    template<class T>
    static bool tryCast(int128_t input, T& result);

    template<class T>
    static T Cast(int128_t input) {
        T result;
        tryCast(input, result);
        return result;
    }

    template<class T>
    static bool tryCastTo(T value, int128_t& result);

    template<class T>
    static int128_t castTo(T value) {
        int128_t result{};
        if (!tryCastTo(value, result)) {
            throw std::overflow_error("INT128 is out of range");
        }
        return result;
    }

    // negate
    static void negateInPlace(int128_t& input) {
        if (input.high == INT64_MIN && input.low == 0) {
            throw std::overflow_error("INT128 is out of range: cannot negate INT128_MIN");
        }
        input.low = UINT64_MAX + 1 - input.low;
        input.high = -input.high - 1 + (input.low == 0);
    }

    static int128_t negate(int128_t input) {
        negateInPlace(input);
        return input;
    }

    static bool tryMultiply(int128_t lhs, int128_t rhs, int128_t& result);

    static int128_t Add(int128_t lhs, int128_t rhs);
    static int128_t Sub(int128_t lhs, int128_t rhs);
    static int128_t Mul(int128_t lhs, int128_t rhs);
    static int128_t Div(int128_t lhs, int128_t rhs);
    static int128_t Mod(int128_t lhs, int128_t rhs);
    static int128_t Xor(int128_t lhs, int128_t rhs);
    static int128_t LeftShift(int128_t lhs, int amount);
    static int128_t RightShift(int128_t lhs, int amount);
    static int128_t BinaryAnd(int128_t lhs, int128_t rhs);
    static int128_t BinaryOr(int128_t lhs, int128_t rhs);
    static int128_t BinaryNot(int128_t val);

    static int128_t divMod(int128_t lhs, int128_t rhs, int128_t& remainder);
    static int128_t divModPositive(int128_t lhs, uint64_t rhs, uint64_t& remainder);

    static bool addInPlace(int128_t& lhs, int128_t rhs);
    static bool subInPlace(int128_t& lhs, int128_t rhs);

    // comparison operators
    static bool Equals(int128_t lhs, int128_t rhs) {
        return lhs.low == rhs.low && lhs.high == rhs.high;
    }

    static bool notEquals(int128_t lhs, int128_t rhs) {
        return lhs.low != rhs.low || lhs.high != rhs.high;
    }

    static bool greaterThan(int128_t lhs, int128_t rhs) {
        return (lhs.high > rhs.high) || (lhs.high == rhs.high && lhs.low > rhs.low);
    }

    static bool greaterThanOrEquals(int128_t lhs, int128_t rhs) {
        return (lhs.high > rhs.high) || (lhs.high == rhs.high && lhs.low >= rhs.low);
    }

    static bool lessThan(int128_t lhs, int128_t rhs) {
        return (lhs.high < rhs.high) || (lhs.high == rhs.high && lhs.low < rhs.low);
    }

    static bool lessThanOrEquals(int128_t lhs, int128_t rhs) {
        return (lhs.high < rhs.high) || (lhs.high == rhs.high && lhs.low <= rhs.low);
    }
    static const int128_t powerOf10[40];
};

template<>
bool Int128_t::tryCast(int128_t input, int8_t& result);
template<>
bool Int128_t::tryCast(int128_t input, int16_t& result);
template<>
bool Int128_t::tryCast(int128_t input, int32_t& result);
template<>
bool Int128_t::tryCast(int128_t input, int64_t& result);
template<>
bool Int128_t::tryCast(int128_t input, uint8_t& result);
template<>
bool Int128_t::tryCast(int128_t input, uint16_t& result);
template<>
bool Int128_t::tryCast(int128_t input, uint32_t& result);
template<>
bool Int128_t::tryCast(int128_t input, uint64_t& result);
template<>
bool Int128_t::tryCast(int128_t input, float& result);
template<>
bool Int128_t::tryCast(int128_t input, double& result);
template<>
bool Int128_t::tryCast(int128_t input, long double& result);

template<>
bool Int128_t::tryCastTo(int8_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(int16_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(int32_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(int64_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(uint8_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(uint16_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(uint32_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(uint64_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(int128_t value, int128_t& result);
template<>
bool Int128_t::tryCastTo(float value, int128_t& result);
template<>
bool Int128_t::tryCastTo(double value, int128_t& result);
template<>
bool Int128_t::tryCastTo(long double value, int128_t& result);

// TODO: const char to int128

} // namespace common
} // namespace kuzu

template<>
struct std::hash<kuzu::common::int128_t> {
    std::size_t operator()(const kuzu::common::int128_t& v) const noexcept;
};

#include <exception>
#include <string>


namespace kuzu {
namespace common {

class KUZU_API Exception : public std::exception {
public:
    explicit Exception(std::string msg);

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

private:
    std::string exception_message_;
};

} // namespace common
} // namespace kuzu

#include <cstdint>


namespace kuzu {
namespace common {

class Value;

class NestedVal {
public:
    KUZU_API static uint32_t getChildrenSize(const Value* val);

    KUZU_API static Value* getChildVal(const Value* val, uint32_t idx);
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>


namespace kuzu {
namespace common {

class Value;

/**
 * @brief NodeVal represents a node in the graph and stores the nodeID, label and properties of that
 * node.
 */
class NodeVal {
public:
    /**
     * @return all properties of the NodeVal.
     * @note this function copies all the properties into a vector, which is not efficient. use
     * `getPropertyName` and `getPropertyVal` instead if possible.
     */
    KUZU_API static std::vector<std::pair<std::string, std::unique_ptr<Value>>> getProperties(
        const Value* val);
    /**
     * @return number of properties of the RelVal.
     */
    KUZU_API static uint64_t getNumProperties(const Value* val);

    /**
     * @return the name of the property at the given index.
     */
    KUZU_API static std::string getPropertyName(const Value* val, uint64_t index);

    /**
     * @return the value of the property at the given index.
     */
    KUZU_API static Value* getPropertyVal(const Value* val, uint64_t index);
    /**
     * @return the nodeID as a Value.
     */
    KUZU_API static Value* getNodeIDVal(const Value* val);
    /**
     * @return the name of the node as a Value.
     */
    KUZU_API static Value* getLabelVal(const Value* val);
    /**
     * @return the current node values in string format.
     */
    KUZU_API static std::string toString(const Value* val);

private:
    static void throwIfNotNode(const Value* val);
    // 2 offsets for id and label.
    static constexpr uint64_t OFFSET = 2;
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

class Value;

/**
 * @brief RecursiveRelVal represents a path in the graph and stores the corresponding rels and nodes
 * of that path.
 */
class RecursiveRelVal {
public:
    /**
     * @return the list of nodes in the recursive rel as a Value.
     */
    KUZU_API static Value* getNodes(const Value* val);

    /**
     * @return the list of rels in the recursive rel as a Value.
     */
    KUZU_API static Value* getRels(const Value* val);

private:
    static void throwIfNotRecursiveRel(const Value* val);
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>


namespace kuzu {
namespace common {

class Value;

/**
 * @brief RelVal represents a rel in the graph and stores the relID, src/dst nodes and properties of
 * that rel.
 */
class RelVal {
public:
    /**
     * @return all properties of the RelVal.
     * @note this function copies all the properties into a vector, which is not efficient. use
     * `getPropertyName` and `getPropertyVal` instead if possible.
     */
    KUZU_API static std::vector<std::pair<std::string, std::unique_ptr<Value>>> getProperties(
        const Value* val);
    /**
     * @return number of properties of the RelVal.
     */
    KUZU_API static uint64_t getNumProperties(const Value* val);
    /**
     * @return the name of the property at the given index.
     */
    KUZU_API static std::string getPropertyName(const Value* val, uint64_t index);
    /**
     * @return the value of the property at the given index.
     */
    KUZU_API static Value* getPropertyVal(const Value* val, uint64_t index);
    /**
     * @return the src nodeID value of the RelVal in Value.
     */
    KUZU_API static Value* getSrcNodeIDVal(const Value* val);
    /**
     * @return the dst nodeID value of the RelVal in Value.
     */
    KUZU_API static Value* getDstNodeIDVal(const Value* val);
    /**
     * @return the internal ID value of the RelVal in Value.
     */
    KUZU_API static Value* getIDVal(const Value* val);
    /**
     * @return the label value of the RelVal.
     */
    KUZU_API static Value* getLabelVal(const Value* val);
    /**
     * @return the value of the RelVal in string format.
     */
    KUZU_API static std::string toString(const Value* val);

private:
    static void throwIfNotRel(const Value* val);
    // 4 offset for id, label, src, dst.
    static constexpr uint64_t OFFSET = 4;
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string>


namespace kuzu {
namespace common {

enum class ExpressionType : uint8_t {
    // Boolean Connection Expressions
    OR = 0,
    XOR = 1,
    AND = 2,
    NOT = 3,

    // Comparison Expressions
    EQUALS = 10,
    NOT_EQUALS = 11,
    GREATER_THAN = 12,
    GREATER_THAN_EQUALS = 13,
    LESS_THAN = 14,
    LESS_THAN_EQUALS = 15,

    // Null Operator Expressions
    IS_NULL = 50,
    IS_NOT_NULL = 51,

    PROPERTY = 60,

    LITERAL = 70,

    STAR = 80,

    VARIABLE = 90,
    PATH = 91,
    PATTERN = 92, // Node & Rel pattern

    PARAMETER = 100,

    // At parsing stage, both aggregate and scalar functions have type FUNCTION.
    // After binding, only scalar function have type FUNCTION.
    FUNCTION = 110,

    AGGREGATE_FUNCTION = 130,

    SUBQUERY = 190,

    CASE_ELSE = 200,

    GRAPH = 210,

    LAMBDA = 220,

    // NOTE: this enum has type uint8_t so don't assign over 255.
    INVALID = 255,
};

struct ExpressionTypeUtil {
    static bool isUnary(ExpressionType type);
    static bool isBinary(ExpressionType type);
    static bool isBoolean(ExpressionType type);
    static bool isComparison(ExpressionType type);
    static bool isNullOperator(ExpressionType type);

    static ExpressionType reverseComparisonDirection(ExpressionType type);

    static KUZU_API std::string toString(ExpressionType type);
    static std::string toParsableString(ExpressionType type);
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>


namespace kuzu {
namespace common {

struct CaseInsensitiveStringHashFunction {
    KUZU_API uint64_t operator()(const std::string& str) const;
};

struct CaseInsensitiveStringEquality {
    KUZU_API bool operator()(const std::string& lhs, const std::string& rhs) const;
};

template<typename T>
using case_insensitive_map_t = std::unordered_map<std::string, T, CaseInsensitiveStringHashFunction,
    CaseInsensitiveStringEquality>;

using case_insensitve_set_t = std::unordered_set<std::string, CaseInsensitiveStringHashFunction,
    CaseInsensitiveStringEquality>;

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <cstring>
#include <string>


namespace kuzu {
namespace common {

struct KUZU_API ku_string_t {

    static constexpr uint64_t PREFIX_LENGTH = 4;
    static constexpr uint64_t INLINED_SUFFIX_LENGTH = 8;
    static constexpr uint64_t SHORT_STR_LENGTH = PREFIX_LENGTH + INLINED_SUFFIX_LENGTH;

    uint32_t len;
    uint8_t prefix[PREFIX_LENGTH];
    union {
        uint8_t data[INLINED_SUFFIX_LENGTH];
        uint64_t overflowPtr;
    };

    ku_string_t() : len{0}, prefix{}, overflowPtr{0} {}
    ku_string_t(const char* value, uint64_t length);

    static bool isShortString(uint32_t len) { return len <= SHORT_STR_LENGTH; }

    const uint8_t* getData() const {
        return isShortString(len) ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    uint8_t* getDataUnsafe() {
        return isShortString(len) ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    // These functions do *NOT* allocate/resize the overflow buffer, it only copies the content and
    // set the length.
    void set(const std::string& value);
    void set(const char* value, uint64_t length);
    void set(const ku_string_t& value);
    void setShortString(const char* value, uint64_t length) {
        this->len = length;
        memcpy(prefix, value, length);
    }
    void setLongString(const char* value, uint64_t length) {
        this->len = length;
        memcpy(prefix, value, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), value, length);
    }
    void setShortString(const ku_string_t& value) {
        this->len = value.len;
        memcpy(prefix, value.prefix, value.len);
    }
    void setLongString(const ku_string_t& value) {
        this->len = value.len;
        memcpy(prefix, value.prefix, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), reinterpret_cast<char*>(value.overflowPtr),
            value.len);
    }

    void setFromRawStr(const char* value, uint64_t length) {
        this->len = length;
        if (isShortString(length)) {
            setShortString(value, length);
        } else {
            memcpy(prefix, value, PREFIX_LENGTH);
            overflowPtr = reinterpret_cast<uint64_t>(value);
        }
    }

    std::string getAsShortString() const;
    std::string getAsString() const;
    std::string_view getAsStringView() const;

    bool operator==(const ku_string_t& rhs) const;

    inline bool operator!=(const ku_string_t& rhs) const { return !(*this == rhs); }

    bool operator>(const ku_string_t& rhs) const;

    inline bool operator>=(const ku_string_t& rhs) const { return (*this > rhs) || (*this == rhs); }

    inline bool operator<(const ku_string_t& rhs) const { return !(*this >= rhs); }

    inline bool operator<=(const ku_string_t& rhs) const { return !(*this > rhs); }
};

} // namespace common
} // namespace kuzu
#include <cstdint>

namespace kuzu {
namespace main {

struct Version {
public:
    /**
     * @brief Get the version of the Kuzu library.
     * @return const char* The version of the Kuzu library.
     */
    KUZU_API static const char* getVersion();

    /**
     * @brief Get the storage version of the Kuzu library.
     * @return uint64_t The storage version of the Kuzu library.
     */
    KUZU_API static uint64_t getStorageVersion();
};
} // namespace main
} // namespace kuzu

#include <cstdint>
#include <string>
#include <unordered_map>


namespace kuzu {
namespace storage {

using storage_version_t = uint64_t;

struct StorageVersionInfo {
    static std::unordered_map<std::string, storage_version_t> getStorageVersionInfo() {
        return {{"0.11.1", 39}, {"0.11.0", 39}, {"0.10.0", 38}, {"0.9.0", 37}, {"0.8.0", 36},
            {"0.7.1.1", 35}, {"0.7.0", 34}, {"0.6.0.6", 33}, {"0.6.0.5", 32}, {"0.6.0.2", 31},
            {"0.6.0.1", 31}, {"0.6.0", 28}, {"0.5.0", 28}, {"0.4.2", 27}, {"0.4.1", 27},
            {"0.4.0", 27}, {"0.3.2", 26}, {"0.3.1", 26}, {"0.3.0", 26}, {"0.2.1", 25},
            {"0.2.0", 25}, {"0.1.0", 24}, {"0.0.12.3", 24}, {"0.0.12.2", 24}, {"0.0.12.1", 24},
            {"0.0.12", 23}, {"0.0.11", 23}, {"0.0.10", 23}, {"0.0.9", 23}, {"0.0.8", 17},
            {"0.0.7", 15}, {"0.0.6", 9}, {"0.0.5", 8}, {"0.0.4", 7}, {"0.0.3", 1}};
    }

    static KUZU_API storage_version_t getStorageVersion();

    static constexpr const char* MAGIC_BYTES = "KUZU";
};

} // namespace storage
} // namespace kuzu

#include <iterator>
#include <memory>
#include <vector>


namespace kuzu {
namespace storage {
class MemoryBuffer;
class MemoryManager;
} // namespace storage

namespace common {

struct KUZU_API BufferBlock {
public:
    explicit BufferBlock(std::unique_ptr<storage::MemoryBuffer> block);
    ~BufferBlock();

    uint64_t size() const;
    uint8_t* data() const;

public:
    uint64_t currentOffset;
    std::unique_ptr<storage::MemoryBuffer> block;

    void resetCurrentOffset() { currentOffset = 0; }
};

class KUZU_API InMemOverflowBuffer {

public:
    explicit InMemOverflowBuffer(storage::MemoryManager* memoryManager)
        : memoryManager{memoryManager} {};

    DEFAULT_BOTH_MOVE(InMemOverflowBuffer);

    uint8_t* allocateSpace(uint64_t size);

    void merge(InMemOverflowBuffer& other) {
        move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
        // We clear the other InMemOverflowBuffer's block because when it is deconstructed,
        // InMemOverflowBuffer's deconstructed tries to free these pages by calling
        // memoryManager->freeBlock, but it should not because this InMemOverflowBuffer still
        // needs them.
        other.blocks.clear();
    }

    // Releases all memory accumulated for string overflows so far and re-initializes its state to
    // an empty buffer. If there is a large string that used point to any of these overflow buffers
    // they will error.
    void resetBuffer();

    // Manually set the underlying memory buffer to evicted to avoid double free
    void preventDestruction();

    storage::MemoryManager* getMemoryManager() { return memoryManager; }

private:
    bool requireNewBlock(uint64_t sizeToAllocate) {
        return blocks.empty() ||
               (currentBlock()->currentOffset + sizeToAllocate) > currentBlock()->size();
    }

    void allocateNewBlock(uint64_t size);

    BufferBlock* currentBlock() { return blocks.back().get(); }

private:
    std::vector<std::unique_ptr<BufferBlock>> blocks;
    storage::MemoryManager* memoryManager;
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string>


namespace kuzu {
namespace main {

struct ClientConfigDefault {
    // 0 means timeout is disabled by default.
    static constexpr uint64_t TIMEOUT_IN_MS = 0;
    static constexpr uint32_t VAR_LENGTH_MAX_DEPTH = 30;
    static constexpr uint64_t SPARSE_FRONTIER_THRESHOLD = 1000;
    static constexpr bool ENABLE_SEMI_MASK = true;
    static constexpr bool ENABLE_ZONE_MAP = true;
    static constexpr bool ENABLE_PROGRESS_BAR = false;
    static constexpr uint64_t SHOW_PROGRESS_AFTER = 1000;
    static constexpr common::PathSemantic RECURSIVE_PATTERN_SEMANTIC = common::PathSemantic::WALK;
    static constexpr uint32_t RECURSIVE_PATTERN_FACTOR = 100;
    static constexpr bool DISABLE_MAP_KEY_CHECK = true;
    static constexpr uint64_t WARNING_LIMIT = 8 * 1024;
    static constexpr bool ENABLE_PLAN_OPTIMIZER = true;
    static constexpr bool ENABLE_INTERNAL_CATALOG = false;
};

struct ClientConfig {
    // System home directory.
    std::string homeDirectory;
    // File search path.
    std::string fileSearchPath;
    // If using semi mask in join.
    bool enableSemiMask = ClientConfigDefault::ENABLE_SEMI_MASK;
    // If using zone map in scan.
    bool enableZoneMap = ClientConfigDefault::ENABLE_ZONE_MAP;
    // Number of threads for execution.
    uint64_t numThreads = 1;
    // Timeout (milliseconds).
    uint64_t timeoutInMS = ClientConfigDefault::TIMEOUT_IN_MS;
    // Variable length maximum depth.
    uint32_t varLengthMaxDepth = ClientConfigDefault::VAR_LENGTH_MAX_DEPTH;
    // Threshold determines when to switch from sparse frontier to dense frontier
    uint64_t sparseFrontierThreshold = ClientConfigDefault::SPARSE_FRONTIER_THRESHOLD;
    // If using progress bar.
    bool enableProgressBar = ClientConfigDefault::ENABLE_PROGRESS_BAR;
    // time before displaying progress bar
    uint64_t showProgressAfter = ClientConfigDefault::SHOW_PROGRESS_AFTER;
    // Semantic for recursive pattern, can be either WALK, TRAIL, ACYCLIC
    common::PathSemantic recursivePatternSemantic = ClientConfigDefault::RECURSIVE_PATTERN_SEMANTIC;
    // Scale factor for recursive pattern cardinality estimation.
    uint32_t recursivePatternCardinalityScaleFactor = ClientConfigDefault::RECURSIVE_PATTERN_FACTOR;
    // Maximum number of cached warnings
    uint64_t warningLimit = ClientConfigDefault::WARNING_LIMIT;
    bool disableMapKeyCheck = ClientConfigDefault::DISABLE_MAP_KEY_CHECK;
    // If enable plan optimizer
    bool enablePlanOptimizer = ClientConfigDefault::ENABLE_PLAN_OPTIMIZER;
    // If use internal catalog during binding
    bool enableInternalCatalog = ClientConfigDefault::ENABLE_INTERNAL_CATALOG;
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a query.
 */
struct PreparedSummary { // NOLINT(*-pro-type-member-init)
    double compilingTime = 0;
    common::StatementType statementType;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
class QuerySummary {
    friend class ClientContext;
    friend class benchmark::Benchmark;

public:
    /**
     * @return query compiling time in milliseconds.
     */
    KUZU_API double getCompilingTime() const;
    /**
     * @return query execution time in milliseconds.
     */
    KUZU_API double getExecutionTime() const;

    void incrementCompilingTime(double increment);
    void incrementExecutionTime(double increment);

    void setPreparedSummary(PreparedSummary preparedSummary_);

    /**
     * @return true if the query is executed with EXPLAIN.
     */
    bool isExplain() const;

    /**
     * @return the statement type of the query.
     */
    common::StatementType getStatementType() const;

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
};

} // namespace main
} // namespace kuzu


namespace kuzu {

namespace regex {
class RE2;
}

namespace common {

struct timestamp_t;

// System representation of dates as the number of days since 1970-01-01.
struct KUZU_API date_t {
    int32_t days;

    date_t();
    explicit date_t(int32_t days_p);

    // Comparison operators with date_t.
    bool operator==(const date_t& rhs) const;
    bool operator!=(const date_t& rhs) const;
    bool operator<=(const date_t& rhs) const;
    bool operator<(const date_t& rhs) const;
    bool operator>(const date_t& rhs) const;
    bool operator>=(const date_t& rhs) const;

    // Comparison operators with timestamp_t.
    bool operator==(const timestamp_t& rhs) const;
    bool operator!=(const timestamp_t& rhs) const;
    bool operator<(const timestamp_t& rhs) const;
    bool operator<=(const timestamp_t& rhs) const;
    bool operator>(const timestamp_t& rhs) const;
    bool operator>=(const timestamp_t& rhs) const;

    // arithmetic operators
    date_t operator+(const int32_t& day) const;
    date_t operator-(const int32_t& day) const;

    date_t operator+(const interval_t& interval) const;
    date_t operator-(const interval_t& interval) const;

    int64_t operator-(const date_t& rhs) const;
};

inline date_t operator+(int64_t i, const date_t date) {
    return date + i;
}

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/date.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/date.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.
class Date {
public:
    KUZU_API static const int32_t NORMAL_DAYS[13];
    KUZU_API static const int32_t CUMULATIVE_DAYS[13];
    KUZU_API static const int32_t LEAP_DAYS[13];
    KUZU_API static const int32_t CUMULATIVE_LEAP_DAYS[13];
    KUZU_API static const int32_t CUMULATIVE_YEAR_DAYS[401];
    KUZU_API static const int8_t MONTH_PER_DAY_OF_YEAR[365];
    KUZU_API static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

    KUZU_API constexpr static const int32_t MIN_YEAR = -290307;
    KUZU_API constexpr static const int32_t MAX_YEAR = 294247;
    KUZU_API constexpr static const int32_t EPOCH_YEAR = 1970;

    KUZU_API constexpr static const int32_t YEAR_INTERVAL = 400;
    KUZU_API constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;
    constexpr static const char* BC_SUFFIX = " (BC)";

    // Convert a string in the format "YYYY-MM-DD" to a date object
    KUZU_API static date_t fromCString(const char* str, uint64_t len);
    // Convert a date object to a string in the format "YYYY-MM-DD"
    KUZU_API static std::string toString(date_t date);
    // Try to convert text in a buffer to a date; returns true if parsing was successful
    KUZU_API static bool tryConvertDate(const char* buf, uint64_t len, uint64_t& pos,
        date_t& result, bool allowTrailing = false);

    // private:
    // Returns true if (year) is a leap year, and false otherwise
    KUZU_API static bool isLeapYear(int32_t year);
    // Returns true if the specified (year, month, day) combination is a valid
    // date
    KUZU_API static bool isValid(int32_t year, int32_t month, int32_t day);
    // Extract the year, month and day from a given date object
    KUZU_API static void convert(date_t date, int32_t& out_year, int32_t& out_month,
        int32_t& out_day);
    // Create a Date object from a specified (year, month, day) combination
    KUZU_API static date_t fromDate(int32_t year, int32_t month, int32_t day);

    // Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
    KUZU_API static bool parseDoubleDigit(const char* buf, uint64_t len, uint64_t& pos,
        int32_t& result);

    KUZU_API static int32_t monthDays(int32_t year, int32_t month);

    KUZU_API static std::string getDayName(date_t date);

    KUZU_API static std::string getMonthName(date_t date);

    KUZU_API static date_t getLastDay(date_t date);

    KUZU_API static int32_t getDatePart(DatePartSpecifier specifier, date_t date);

    KUZU_API static date_t trunc(DatePartSpecifier specifier, date_t date);

    KUZU_API static int64_t getEpochNanoSeconds(const date_t& date);

    KUZU_API static const regex::RE2& regexPattern();

private:
    static void extractYearOffset(int32_t& n, int32_t& year, int32_t& year_offset);
};

} // namespace common
} // namespace kuzu


namespace kuzu {

namespace regex {
class RE2;
}

namespace common {

class RandomEngine;

// Note: uuid_t is a reserved keyword in MSVC, we have to use ku_uuid_t instead.
struct ku_uuid_t {
    int128_t value;
};

struct UUID {
    static constexpr const uint8_t UUID_STRING_LENGTH = 36;
    static constexpr const char HEX_DIGITS[] = "0123456789abcdef";
    static void byteToHex(char byteVal, char* buf, uint64_t& pos);
    static unsigned char hex2Char(char ch);
    static bool isHex(char ch);
    static bool fromString(std::string str, int128_t& result);

    static int128_t fromString(std::string str);
    static int128_t fromCString(const char* str, uint64_t len);
    static void toString(int128_t input, char* buf);
    static std::string toString(int128_t input);
    static std::string toString(ku_uuid_t val);

    static ku_uuid_t generateRandomUUID(RandomEngine* engine);

    static const regex::RE2& regexPattern();
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

class KUZU_API InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg){};
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

class KUZU_API BinderException : public Exception {
public:
    explicit BinderException(const std::string& msg) : Exception("Binder exception: " + msg){};
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

class KUZU_API CatalogException : public Exception {
public:
    explicit CatalogException(const std::string& msg) : Exception("Catalog exception: " + msg){};
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

struct blob_t {
    ku_string_t value;
};

struct HexFormatConstants {
    // map of integer -> hex value.
    static constexpr const char* HEX_TABLE = "0123456789ABCDEF";
    // reverse map of byte -> integer value, or -1 for invalid hex values.
    static const int HEX_MAP[256];
    static constexpr const uint64_t NUM_BYTES_TO_SHIFT_FOR_FIRST_BYTE = 4;
    static constexpr const uint64_t SECOND_BYTE_MASK = 0x0F;
    static constexpr const char PREFIX[] = "\\x";
    static constexpr const uint64_t PREFIX_LENGTH = 2;
    static constexpr const uint64_t FIRST_BYTE_POS = PREFIX_LENGTH;
    static constexpr const uint64_t SECOND_BYTES_POS = PREFIX_LENGTH + 1;
    static constexpr const uint64_t LENGTH = 4;
};

struct Blob {
    static std::string toString(const uint8_t* value, uint64_t len);

    static inline std::string toString(const blob_t& blob) {
        return toString(blob.value.getData(), blob.value.len);
    }

    static uint64_t getBlobSize(const ku_string_t& blob);

    static uint64_t fromString(const char* str, uint64_t length, uint8_t* resultBuffer);

    template<typename T>
    static inline T getValue(const blob_t& data) {
        return *reinterpret_cast<const T*>(data.value.getData());
    }
    template<typename T>
    // NOLINTNEXTLINE(readability-non-const-parameter): Would cast away qualifiers.
    static inline T getValue(char* data) {
        return *reinterpret_cast<T*>(data);
    }

private:
    static void validateHexCode(const uint8_t* blobStr, uint64_t length, uint64_t curPos);
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
struct KUZU_API timestamp_t {
    int64_t value = 0;

    timestamp_t();
    explicit timestamp_t(int64_t value_p);
    timestamp_t& operator=(int64_t value_p);

    // explicit conversion
    explicit operator int64_t() const;

    // Comparison operators with timestamp_t.
    bool operator==(const timestamp_t& rhs) const;
    bool operator!=(const timestamp_t& rhs) const;
    bool operator<=(const timestamp_t& rhs) const;
    bool operator<(const timestamp_t& rhs) const;
    bool operator>(const timestamp_t& rhs) const;
    bool operator>=(const timestamp_t& rhs) const;

    // Comparison operators with date_t.
    bool operator==(const date_t& rhs) const;
    bool operator!=(const date_t& rhs) const;
    bool operator<(const date_t& rhs) const;
    bool operator<=(const date_t& rhs) const;
    bool operator>(const date_t& rhs) const;
    bool operator>=(const date_t& rhs) const;

    // arithmetic operator
    timestamp_t operator+(const interval_t& interval) const;
    timestamp_t operator-(const interval_t& interval) const;

    interval_t operator-(const timestamp_t& rhs) const;
};

struct timestamp_tz_t : public timestamp_t { // NO LINT
    using timestamp_t::timestamp_t;
};
struct timestamp_ns_t : public timestamp_t { // NO LINT
    using timestamp_t::timestamp_t;
};
struct timestamp_ms_t : public timestamp_t { // NO LINT
    using timestamp_t::timestamp_t;
};
struct timestamp_sec_t : public timestamp_t { // NO LINT
    using timestamp_t::timestamp_t;
};

// Note: Aside from some minor changes, this implementation is copied from DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/timestamp.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/timestamp.cpp.
// For example, instead of using their idx_t type to refer to indices, we directly use uint64_t,
// which is the actual type of idx_t (so we say uint64_t len instead of idx_t len). When more
// functionality is needed, we should first consult these DuckDB links.

// The Timestamp class is a static class that holds helper functions for the Timestamp type.
// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
class Timestamp {
public:
    KUZU_API static timestamp_t fromCString(const char* str, uint64_t len);

    // Convert a timestamp object to a std::string in the format "YYYY-MM-DD hh:mm:ss".
    KUZU_API static std::string toString(timestamp_t timestamp);

    // Date header is in the format: %Y%m%d.
    KUZU_API static std::string getDateHeader(const timestamp_t& timestamp);

    // Timestamp header is in the format: %Y%m%dT%H%M%SZ.
    KUZU_API static std::string getDateTimeHeader(const timestamp_t& timestamp);

    KUZU_API static date_t getDate(timestamp_t timestamp);

    KUZU_API static dtime_t getTime(timestamp_t timestamp);

    // Create a Timestamp object from a specified (date, time) combination.
    KUZU_API static timestamp_t fromDateTime(date_t date, dtime_t time);

    KUZU_API static bool tryConvertTimestamp(const char* str, uint64_t len, timestamp_t& result);

    // Extract the date and time from a given timestamp object.
    KUZU_API static void convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time);

    // Create a Timestamp object from the specified epochMs.
    KUZU_API static timestamp_t fromEpochMicroSeconds(int64_t epochMs);

    // Create a Timestamp object from the specified epochMs.
    KUZU_API static timestamp_t fromEpochMilliSeconds(int64_t ms);

    // Create a Timestamp object from the specified epochSec.
    KUZU_API static timestamp_t fromEpochSeconds(int64_t sec);

    // Create a Timestamp object from the specified epochNs.
    KUZU_API static timestamp_t fromEpochNanoSeconds(int64_t ns);

    KUZU_API static int32_t getTimestampPart(DatePartSpecifier specifier, timestamp_t timestamp);

    KUZU_API static timestamp_t trunc(DatePartSpecifier specifier, timestamp_t date);

    KUZU_API static int64_t getEpochNanoSeconds(const timestamp_t& timestamp);

    KUZU_API static int64_t getEpochMilliSeconds(const timestamp_t& timestamp);

    KUZU_API static int64_t getEpochSeconds(const timestamp_t& timestamp);

    KUZU_API static bool tryParseUTCOffset(const char* str, uint64_t& pos, uint64_t len,
        int& hour_offset, int& minute_offset);

    static std::string getTimestampConversionExceptionMsg(const char* str, uint64_t len,
        const std::string& typeID = "TIMESTAMP") {
        return "Error occurred during parsing " + typeID + ". Given: \"" + std::string(str, len) +
               "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
    }

    KUZU_API static timestamp_t getCurrentTimestamp();
};

} // namespace common
} // namespace kuzu

#include <string>
#include <string_view>
#if USE_STD_FORMAT
#include <format>
#else
#endif

namespace kuzu {
namespace common {

#if USE_STD_FORMAT

template<typename... Args>
inline std::string stringFormat(std::format_string<Args...> format, Args&&... args) {
    return std::format(format, std::forward<Args>(args)...);
}

#else

namespace string_format_detail {
#define MAP_STD_TO_STRING(typ)                                                                     \
    inline std::string map(typ v) {                                                                \
        return std::to_string(v);                                                                  \
    }

MAP_STD_TO_STRING(short)
MAP_STD_TO_STRING(unsigned short)
MAP_STD_TO_STRING(int)
MAP_STD_TO_STRING(unsigned int)
MAP_STD_TO_STRING(long)
MAP_STD_TO_STRING(unsigned long)
MAP_STD_TO_STRING(long long)
MAP_STD_TO_STRING(unsigned long long)
MAP_STD_TO_STRING(float)
MAP_STD_TO_STRING(double)
#undef MAP_STD_TO_STRING

#define MAP_SELF(typ)                                                                              \
    inline typ map(typ v) {                                                                        \
        return v;                                                                                  \
    }
MAP_SELF(const char*);
// Also covers std::string
MAP_SELF(std::string_view)

// Chars are mapped to themselves, but signed char and unsigned char (which are used for int8_t and
// uint8_t respectively), need to be cast to be properly output as integers. This is consistent with
// fmt's behavior.
MAP_SELF(char)
inline std::string map(signed char v) {
    return std::to_string(int(v));
}
inline std::string map(unsigned char v) {
    return std::to_string(unsigned(v));
}
#undef MAP_SELF

template<typename... Args>
inline void stringFormatHelper(std::string& ret, std::string_view format, Args&&... args) {
    size_t bracket = format.find('{');
    if (bracket == std::string_view::npos) {
        ret += format;
        return;
    }
    ret += format.substr(0, bracket);
    if (format.substr(bracket, 4) == "{{}}") {
        // Escaped {}.
        ret += "{}";
        return stringFormatHelper(ret, format.substr(bracket + 4), std::forward<Args>(args)...);
    } else if (format.substr(bracket, 2) == "{}") {
        // Formatted {}.
        throw InternalException("Not enough values for string_format.");
    }
    // Something else.
    ret.push_back('{');
    return stringFormatHelper(ret, format.substr(bracket + 1), std::forward<Args>(args)...);
}

template<typename Arg, typename... Args>
inline void stringFormatHelper(std::string& ret, std::string_view format, Arg&& arg,
    Args&&... args) {
    size_t bracket = format.find('{');
    if (bracket == std::string_view::npos) {
        throw InternalException("Too many values for string_format.");
    }
    ret += format.substr(0, bracket);
    if (format.substr(bracket, 4) == "{{}}") {
        // Escaped {}.
        ret += "{}";
        return stringFormatHelper(ret, format.substr(bracket + 4), std::forward<Arg>(arg),
            std::forward<Args>(args)...);
    } else if (format.substr(bracket, 2) == "{}") {
        // Formatted {}.
        ret += map(arg);
        return stringFormatHelper(ret, format.substr(bracket + 2), std::forward<Args>(args)...);
    }
    // Something else.
    ret.push_back('{');
    return stringFormatHelper(ret, format.substr(bracket + 1), std::forward<Arg>(arg),
        std::forward<Args>(args)...);
}
} // namespace string_format_detail

// Formats `args` according to `format`. Accepts {} for formatting the argument and {{}} for
// a literal {}. Formatting is done with std::ostream::operator<<.
template<typename... Args>
inline std::string stringFormat(std::string_view format, Args&&... args) {
    std::string ret;
    ret.reserve(32); // Optimistic pre-allocation.
    string_format_detail::stringFormatHelper(ret, format, std::forward<Args>(args)...);
    return ret;
}

#endif

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(const char* condition_name, const char* file,
    int linenr) {
    // LCOV_EXCL_START
    throw InternalException(stringFormat("Assertion failed in file \"{}\" on line {}: {}", file,
        linenr, condition_name));
    // LCOV_EXCL_STOP
}

#define KU_ASSERT_UNCONDITIONAL(condition)                                                         \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)

#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define RUNTIME_CHECK(code) code
#define KU_ASSERT(condition) KU_ASSERT_UNCONDITIONAL(condition)
#else
#define KU_ASSERT(condition) void(0)
#define RUNTIME_CHECK(code) void(0)
#endif

#define KU_UNREACHABLE                                                                             \
    /* LCOV_EXCL_START */ [[unlikely]] kuzu::common::kuAssertFailureInternal("KU_UNREACHABLE",     \
        __FILE__, __LINE__) /* LCOV_EXCL_STOP */
#define KU_UNUSED(expr) (void)(expr)

} // namespace common
} // namespace kuzu

#include <typeinfo>


namespace kuzu {
namespace common {

template<typename TO, typename FROM>
TO ku_dynamic_cast(FROM* old) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    static_assert(std::is_pointer<TO>());
    TO newVal = dynamic_cast<TO>(old);
    KU_ASSERT(newVal != nullptr);
    return newVal;
#else
    return reinterpret_cast<TO>(old);
#endif
}

template<typename TO, typename FROM>
TO ku_dynamic_cast(FROM& old) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    static_assert(std::is_reference<TO>());
    try {
        TO newVal = dynamic_cast<TO>(old);
        return newVal;
    } catch (std::bad_cast& e) {
        KU_ASSERT(false);
    }
#else
    return reinterpret_cast<TO>(old);
#endif
}

} // namespace common
} // namespace kuzu

#include <chrono>
#include <string>


namespace kuzu {
namespace common {

class Timer {

public:
    void start() {
        finished = false;
        startTime = std::chrono::high_resolution_clock::now();
    }

    void stop() {
        stopTime = std::chrono::high_resolution_clock::now();
        finished = true;
    }

    double getDuration() const {
        if (finished) {
            auto duration = stopTime - startTime;
            return (double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        }
        throw Exception("Timer is still running.");
    }

    uint64_t getElapsedTimeInMS() const {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = now - startTime;
        auto count = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        KU_ASSERT(count >= 0);
        return count;
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> stopTime;
    bool finished = false;
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <memory>

#include <span>

namespace kuzu {
namespace common {

class ArrowNullMaskTree;
class Serializer;
class Deserializer;

constexpr uint64_t NULL_BITMASKS_WITH_SINGLE_ONE[64] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80,
    0x100, 0x200, 0x400, 0x800, 0x1000, 0x2000, 0x4000, 0x8000, 0x10000, 0x20000, 0x40000, 0x80000,
    0x100000, 0x200000, 0x400000, 0x800000, 0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000,
    0x20000000, 0x40000000, 0x80000000, 0x100000000, 0x200000000, 0x400000000, 0x800000000,
    0x1000000000, 0x2000000000, 0x4000000000, 0x8000000000, 0x10000000000, 0x20000000000,
    0x40000000000, 0x80000000000, 0x100000000000, 0x200000000000, 0x400000000000, 0x800000000000,
    0x1000000000000, 0x2000000000000, 0x4000000000000, 0x8000000000000, 0x10000000000000,
    0x20000000000000, 0x40000000000000, 0x80000000000000, 0x100000000000000, 0x200000000000000,
    0x400000000000000, 0x800000000000000, 0x1000000000000000, 0x2000000000000000,
    0x4000000000000000, 0x8000000000000000};
constexpr uint64_t NULL_BITMASKS_WITH_SINGLE_ZERO[64] = {0xfffffffffffffffe, 0xfffffffffffffffd,
    0xfffffffffffffffb, 0xfffffffffffffff7, 0xffffffffffffffef, 0xffffffffffffffdf,
    0xffffffffffffffbf, 0xffffffffffffff7f, 0xfffffffffffffeff, 0xfffffffffffffdff,
    0xfffffffffffffbff, 0xfffffffffffff7ff, 0xffffffffffffefff, 0xffffffffffffdfff,
    0xffffffffffffbfff, 0xffffffffffff7fff, 0xfffffffffffeffff, 0xfffffffffffdffff,
    0xfffffffffffbffff, 0xfffffffffff7ffff, 0xffffffffffefffff, 0xffffffffffdfffff,
    0xffffffffffbfffff, 0xffffffffff7fffff, 0xfffffffffeffffff, 0xfffffffffdffffff,
    0xfffffffffbffffff, 0xfffffffff7ffffff, 0xffffffffefffffff, 0xffffffffdfffffff,
    0xffffffffbfffffff, 0xffffffff7fffffff, 0xfffffffeffffffff, 0xfffffffdffffffff,
    0xfffffffbffffffff, 0xfffffff7ffffffff, 0xffffffefffffffff, 0xffffffdfffffffff,
    0xffffffbfffffffff, 0xffffff7fffffffff, 0xfffffeffffffffff, 0xfffffdffffffffff,
    0xfffffbffffffffff, 0xfffff7ffffffffff, 0xffffefffffffffff, 0xffffdfffffffffff,
    0xffffbfffffffffff, 0xffff7fffffffffff, 0xfffeffffffffffff, 0xfffdffffffffffff,
    0xfffbffffffffffff, 0xfff7ffffffffffff, 0xffefffffffffffff, 0xffdfffffffffffff,
    0xffbfffffffffffff, 0xff7fffffffffffff, 0xfeffffffffffffff, 0xfdffffffffffffff,
    0xfbffffffffffffff, 0xf7ffffffffffffff, 0xefffffffffffffff, 0xdfffffffffffffff,
    0xbfffffffffffffff, 0x7fffffffffffffff};

const uint64_t NULL_LOWER_MASKS[65] = {0x0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f, 0xff, 0x1ff,
    0x3ff, 0x7ff, 0xfff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x1ffff, 0x3ffff, 0x7ffff, 0xfffff,
    0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x1ffffff, 0x3ffffff, 0x7ffffff, 0xfffffff, 0x1fffffff,
    0x3fffffff, 0x7fffffff, 0xffffffff, 0x1ffffffff, 0x3ffffffff, 0x7ffffffff, 0xfffffffff,
    0x1fffffffff, 0x3fffffffff, 0x7fffffffff, 0xffffffffff, 0x1ffffffffff, 0x3ffffffffff,
    0x7ffffffffff, 0xfffffffffff, 0x1fffffffffff, 0x3fffffffffff, 0x7fffffffffff, 0xffffffffffff,
    0x1ffffffffffff, 0x3ffffffffffff, 0x7ffffffffffff, 0xfffffffffffff, 0x1fffffffffffff,
    0x3fffffffffffff, 0x7fffffffffffff, 0xffffffffffffff, 0x1ffffffffffffff, 0x3ffffffffffffff,
    0x7ffffffffffffff, 0xfffffffffffffff, 0x1fffffffffffffff, 0x3fffffffffffffff,
    0x7fffffffffffffff, 0xffffffffffffffff};
const uint64_t NULL_HIGH_MASKS[65] = {0x0, 0x8000000000000000, 0xc000000000000000,
    0xe000000000000000, 0xf000000000000000, 0xf800000000000000, 0xfc00000000000000,
    0xfe00000000000000, 0xff00000000000000, 0xff80000000000000, 0xffc0000000000000,
    0xffe0000000000000, 0xfff0000000000000, 0xfff8000000000000, 0xfffc000000000000,
    0xfffe000000000000, 0xffff000000000000, 0xffff800000000000, 0xffffc00000000000,
    0xffffe00000000000, 0xfffff00000000000, 0xfffff80000000000, 0xfffffc0000000000,
    0xfffffe0000000000, 0xffffff0000000000, 0xffffff8000000000, 0xffffffc000000000,
    0xffffffe000000000, 0xfffffff000000000, 0xfffffff800000000, 0xfffffffc00000000,
    0xfffffffe00000000, 0xffffffff00000000, 0xffffffff80000000, 0xffffffffc0000000,
    0xffffffffe0000000, 0xfffffffff0000000, 0xfffffffff8000000, 0xfffffffffc000000,
    0xfffffffffe000000, 0xffffffffff000000, 0xffffffffff800000, 0xffffffffffc00000,
    0xffffffffffe00000, 0xfffffffffff00000, 0xfffffffffff80000, 0xfffffffffffc0000,
    0xfffffffffffe0000, 0xffffffffffff0000, 0xffffffffffff8000, 0xffffffffffffc000,
    0xffffffffffffe000, 0xfffffffffffff000, 0xfffffffffffff800, 0xfffffffffffffc00,
    0xfffffffffffffe00, 0xffffffffffffff00, 0xffffffffffffff80, 0xffffffffffffffc0,
    0xffffffffffffffe0, 0xfffffffffffffff0, 0xfffffffffffffff8, 0xfffffffffffffffc,
    0xfffffffffffffffe, 0xffffffffffffffff};

class KUZU_API NullMask {
public:
    static constexpr uint64_t NO_NULL_ENTRY = 0;
    static constexpr uint64_t ALL_NULL_ENTRY = ~uint64_t(NO_NULL_ENTRY);
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY_LOG2 = 6;
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY = (uint64_t)1 << NUM_BITS_PER_NULL_ENTRY_LOG2;
    static constexpr uint64_t NUM_BYTES_PER_NULL_ENTRY = NUM_BITS_PER_NULL_ENTRY >> 3;

    // For creating a managed null mask
    explicit NullMask(uint64_t capacity) : mayContainNulls{false} {
        auto numNullEntries = (capacity + NUM_BITS_PER_NULL_ENTRY - 1) / NUM_BITS_PER_NULL_ENTRY;
        buffer = std::make_unique<uint64_t[]>(numNullEntries);
        data = std::span(buffer.get(), numNullEntries);
        std::fill(data.begin(), data.end(), NO_NULL_ENTRY);
    }

    // For creating a null mask using existing data
    explicit NullMask(std::span<uint64_t> nullData, bool mayContainNulls)
        : data{nullData}, buffer{}, mayContainNulls{mayContainNulls} {}

    inline void setAllNonNull() {
        if (!mayContainNulls) {
            return;
        }
        std::fill(data.begin(), data.end(), NO_NULL_ENTRY);
        mayContainNulls = false;
    }
    inline void setAllNull() {
        std::fill(data.begin(), data.end(), ALL_NULL_ENTRY);
        mayContainNulls = true;
    }

    inline bool hasNoNullsGuarantee() const { return !mayContainNulls; }
    uint64_t countNulls() const;

    static void setNull(uint64_t* nullEntries, uint32_t pos, bool isNull);
    inline void setNull(uint32_t pos, bool isNull) {
        KU_ASSERT(pos < getNumNullBits(data));
        setNull(data.data(), pos, isNull);
        if (isNull) {
            mayContainNulls = true;
        }
    }

    static inline bool isNull(const uint64_t* nullEntries, uint32_t pos) {
        auto [entryPos, bitPosInEntry] = getNullEntryAndBitPos(pos);
        return nullEntries[entryPos] & NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
    }

    static uint64_t getNumNullBits(std::span<uint64_t> data) {
        return data.size() * NullMask::NUM_BITS_PER_NULL_ENTRY;
    }

    inline bool isNull(uint32_t pos) const {
        KU_ASSERT(pos < getNumNullBits(data));
        return isNull(data.data(), pos);
    }

    // const because updates to the data must set mayContainNulls if any value
    // becomes non-null
    // Modifying the underlying data should be done with setNull or copyFromNullData
    inline const uint64_t* getData() const { return data.data(); }

    static inline uint64_t getNumNullEntries(uint64_t numNullBits) {
        return (numNullBits >> NUM_BITS_PER_NULL_ENTRY_LOG2) +
               ((numNullBits - (numNullBits << NUM_BITS_PER_NULL_ENTRY_LOG2)) == 0 ? 0 : 1);
    }

    // Copies bitpacked null flags from one buffer to another, starting at an arbitrary bit
    // offset and preserving adjacent bits.
    //
    // returns true if we have copied a nullBit with value 1 (indicates a null value) to
    // dstNullEntries.
    static bool copyNullMask(const uint64_t* srcNullEntries, uint64_t srcOffset,
        uint64_t* dstNullEntries, uint64_t dstOffset, uint64_t numBitsToCopy, bool invert = false);

    inline bool copyFrom(const NullMask& nullMask, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy, bool invert = false) {
        if (nullMask.hasNoNullsGuarantee()) {
            setNullFromRange(dstOffset, numBitsToCopy, invert);
            return invert;
        } else {
            return copyFromNullBits(nullMask.getData(), srcOffset, dstOffset, numBitsToCopy,
                invert);
        }
    }
    bool copyFromNullBits(const uint64_t* srcNullEntries, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy, bool invert = false);

    // Sets the given number of bits to null (if isNull is true) or non-null (if isNull is false),
    // starting at the offset
    static void setNullRange(uint64_t* nullEntries, uint64_t offset, uint64_t numBitsToSet,
        bool isNull);

    void setNullFromRange(uint64_t offset, uint64_t numBitsToSet, bool isNull);

    void resize(uint64_t capacity);

    void operator|=(const NullMask& other);

    // Fast calculation of the minimum and maximum null values
    // (essentially just three states, all null, all non-null and some null)
    static std::pair<bool, bool> getMinMax(const uint64_t* nullEntries, uint64_t offset,
        uint64_t numValues);

private:
    static inline std::pair<uint64_t, uint64_t> getNullEntryAndBitPos(uint64_t pos) {
        auto nullEntryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
        return std::make_pair(nullEntryPos,
            pos - (nullEntryPos << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2));
    }

    static bool copyUnaligned(const uint64_t* srcNullEntries, uint64_t srcOffset,
        uint64_t* dstNullEntries, uint64_t dstOffset, uint64_t numBitsToCopy, bool invert = false);

private:
    std::span<uint64_t> data;
    std::unique_ptr<uint64_t[]> buffer;
    bool mayContainNulls;
};

} // namespace common
} // namespace kuzu

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace kuzu {
namespace main {
class ClientContext;
}
namespace processor {
class ParquetReader;
}
namespace catalog {
class NodeTableCatalogEntry;
}
namespace common {

class Serializer;
class Deserializer;
struct FileInfo;

using sel_t = uint64_t;
constexpr sel_t INVALID_SEL = UINT64_MAX;
using hash_t = uint64_t;
using page_idx_t = uint32_t;
using frame_idx_t = page_idx_t;
using page_offset_t = uint32_t;
constexpr page_idx_t INVALID_PAGE_IDX = UINT32_MAX;
using file_idx_t = uint32_t;
constexpr file_idx_t INVALID_FILE_IDX = UINT32_MAX;
using page_group_idx_t = uint32_t;
using frame_group_idx_t = page_group_idx_t;
using column_id_t = uint32_t;
using property_id_t = uint32_t;
constexpr column_id_t INVALID_COLUMN_ID = UINT32_MAX;
constexpr column_id_t ROW_IDX_COLUMN_ID = INVALID_COLUMN_ID - 1;
using idx_t = uint32_t;
constexpr idx_t INVALID_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
constexpr block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;
using struct_field_idx_t = uint16_t;
using union_field_idx_t = struct_field_idx_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT16_MAX;
using row_idx_t = uint64_t;
constexpr row_idx_t INVALID_ROW_IDX = UINT64_MAX;
constexpr uint32_t UNDEFINED_CAST_COST = UINT32_MAX;
using node_group_idx_t = uint64_t;
constexpr node_group_idx_t INVALID_NODE_GROUP_IDX = UINT64_MAX;
using partition_idx_t = uint64_t;
constexpr partition_idx_t INVALID_PARTITION_IDX = UINT64_MAX;
using length_t = uint64_t;
constexpr length_t INVALID_LENGTH = UINT64_MAX;
using list_size_t = uint32_t;
using sequence_id_t = uint64_t;
using oid_t = uint64_t;
constexpr oid_t INVALID_OID = UINT64_MAX;

using transaction_t = uint64_t;
constexpr transaction_t INVALID_TRANSACTION = UINT64_MAX;
using executor_id_t = uint64_t;
using executor_info = std::unordered_map<executor_id_t, uint64_t>;

// table id type alias
using table_id_t = oid_t;
using table_id_vector_t = std::vector<table_id_t>;
using table_id_set_t = std::unordered_set<table_id_t>;
template<typename T>
using table_id_map_t = std::unordered_map<table_id_t, T>;
constexpr table_id_t INVALID_TABLE_ID = INVALID_OID;
// offset type alias
using offset_t = uint64_t;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;
// internal id type alias
struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

using cardinality_t = uint64_t;
constexpr offset_t INVALID_LIMIT = UINT64_MAX;
using offset_vec_t = std::vector<offset_t>;
// System representation for internalID.
struct KUZU_API internalID_t {
    offset_t offset;
    table_id_t tableID;

    internalID_t();
    internalID_t(offset_t offset, table_id_t tableID);

    // comparison operators
    bool operator==(const internalID_t& rhs) const;
    bool operator!=(const internalID_t& rhs) const;
    bool operator>(const internalID_t& rhs) const;
    bool operator>=(const internalID_t& rhs) const;
    bool operator<(const internalID_t& rhs) const;
    bool operator<=(const internalID_t& rhs) const;
};

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
};

struct list_entry_t {
    offset_t offset;
    list_size_t size;

    constexpr list_entry_t() : offset{INVALID_OFFSET}, size{UINT32_MAX} {}
    constexpr list_entry_t(offset_t offset, list_size_t size) : offset{offset}, size{size} {}
};

struct struct_entry_t {
    int64_t pos;
};

struct map_entry_t {
    list_entry_t entry;
};

struct union_entry_t {
    struct_entry_t entry;
};

struct int128_t;
struct ku_string_t;

template<typename T>
concept SignedIntegerTypes =
    std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
    std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>;

template<typename T>
concept IntegerTypes =
    SignedIntegerTypes<T> || std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
    std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>;

template<typename T>
concept FloatingPointTypes = std::is_same_v<T, float> || std::is_same_v<T, double>;

template<typename T>
concept NumericTypes = IntegerTypes<T> || std::floating_point<T>;

template<typename T>
concept ComparableTypes = NumericTypes<T> || std::is_same_v<T, ku_string_t> ||
                          std::is_same_v<T, interval_t> || std::is_same_v<T, bool>;

template<typename T>
concept HashablePrimitive = ((std::integral<T> && !std::is_same_v<T, bool>) ||
                             std::floating_point<T> || std::is_same_v<T, int128_t>);
template<typename T>
concept IndexHashable = ((std::integral<T> && !std::is_same_v<T, bool>) || std::floating_point<T> ||
                         std::is_same_v<T, int128_t> || std::is_same_v<T, ku_string_t> ||
                         std::is_same_v<T, std::string_view> || std::same_as<T, std::string>);

template<typename T>
concept HashableNonNestedTypes = (std::integral<T> || std::floating_point<T> ||
                                  std::is_same_v<T, int128_t> || std::is_same_v<T, internalID_t> ||
                                  std::is_same_v<T, interval_t> || std::is_same_v<T, ku_string_t>);

template<typename T>
concept HashableNestedTypes =
    (std::is_same_v<T, list_entry_t> || std::is_same_v<T, struct_entry_t>);

template<typename T>
concept HashableTypes = (HashableNestedTypes<T> || HashableNonNestedTypes<T>);

enum class LogicalTypeID : uint8_t {
    ANY = 0,
    NODE = 10,
    REL = 11,
    RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 13,

    BOOL = 22,
    INT64 = 23,
    INT32 = 24,
    INT16 = 25,
    INT8 = 26,
    UINT64 = 27,
    UINT32 = 28,
    UINT16 = 29,
    UINT8 = 30,
    INT128 = 31,
    DOUBLE = 32,
    FLOAT = 33,
    DATE = 34,
    TIMESTAMP = 35,
    TIMESTAMP_SEC = 36,
    TIMESTAMP_MS = 37,
    TIMESTAMP_NS = 38,
    TIMESTAMP_TZ = 39,
    INTERVAL = 40,
    DECIMAL = 41,
    INTERNAL_ID = 42,

    STRING = 50,
    BLOB = 51,

    LIST = 52,
    ARRAY = 53,
    STRUCT = 54,
    MAP = 55,
    UNION = 56,
    POINTER = 58,

    UUID = 59,

};

enum class PhysicalTypeID : uint8_t {
    // Fixed size types.
    ANY = 0,
    BOOL = 1,
    INT64 = 2,
    INT32 = 3,
    INT16 = 4,
    INT8 = 5,
    UINT64 = 6,
    UINT32 = 7,
    UINT16 = 8,
    UINT8 = 9,
    INT128 = 10,
    DOUBLE = 11,
    FLOAT = 12,
    INTERVAL = 13,
    INTERNAL_ID = 14,
    ALP_EXCEPTION_FLOAT = 15,
    ALP_EXCEPTION_DOUBLE = 16,

    // Variable size types.
    STRING = 20,
    LIST = 22,
    ARRAY = 23,
    STRUCT = 24,
    POINTER = 25,
};

class ExtraTypeInfo;
class StructField;
class StructTypeInfo;

enum class TypeCategory : uint8_t { INTERNAL = 0, UDT = 1 };

class LogicalType {
    friend struct LogicalTypeUtils;
    friend struct DecimalType;
    friend struct StructType;
    friend struct ListType;
    friend struct ArrayType;

    KUZU_API LogicalType(const LogicalType& other);

public:
    KUZU_API LogicalType() : typeID{LogicalTypeID::ANY}, extraTypeInfo{nullptr} {
        physicalType = getPhysicalType(this->typeID);
    };
    explicit KUZU_API LogicalType(LogicalTypeID typeID, TypeCategory info = TypeCategory::INTERNAL);
    EXPLICIT_COPY_DEFAULT_MOVE(LogicalType);

    KUZU_API bool operator==(const LogicalType& other) const;
    KUZU_API bool operator!=(const LogicalType& other) const;

    KUZU_API std::string toString() const;
    static bool isBuiltInType(const std::string& str);
    static LogicalType convertFromString(const std::string& str, main::ClientContext* context);

    KUZU_API LogicalTypeID getLogicalTypeID() const { return typeID; }
    bool containsAny() const;
    bool isInternalType() const { return category == TypeCategory::INTERNAL; }

    KUZU_API PhysicalTypeID getPhysicalType() const { return physicalType; }
    KUZU_API static PhysicalTypeID getPhysicalType(LogicalTypeID logicalType,
        const std::unique_ptr<ExtraTypeInfo>& extraTypeInfo = nullptr);

    void setExtraTypeInfo(std::unique_ptr<ExtraTypeInfo> typeInfo) {
        extraTypeInfo = std::move(typeInfo);
    }

    const ExtraTypeInfo* getExtraTypeInfo() const { return extraTypeInfo.get(); }

    void serialize(Serializer& serializer) const;

    static LogicalType deserialize(Deserializer& deserializer);

    KUZU_API static std::vector<LogicalType> copy(const std::vector<LogicalType>& types);
    KUZU_API static std::vector<LogicalType> copy(const std::vector<LogicalType*>& types);

    static LogicalType ANY() { return LogicalType(LogicalTypeID::ANY); }

    // NOTE: avoid using this if possible, this is a temporary hack for passing internal types
    // TODO(Royi) remove this when float compression no longer relies on this or ColumnChunkData
    // takes physical types instead of logical types
    static LogicalType ANY(PhysicalTypeID physicalType) {
        auto ret = LogicalType(LogicalTypeID::ANY);
        ret.physicalType = physicalType;
        return ret;
    }

    static LogicalType BOOL() { return LogicalType(LogicalTypeID::BOOL); }
    static LogicalType HASH() { return LogicalType(LogicalTypeID::UINT64); }
    static LogicalType INT64() { return LogicalType(LogicalTypeID::INT64); }
    static LogicalType INT32() { return LogicalType(LogicalTypeID::INT32); }
    static LogicalType INT16() { return LogicalType(LogicalTypeID::INT16); }
    static LogicalType INT8() { return LogicalType(LogicalTypeID::INT8); }
    static LogicalType UINT64() { return LogicalType(LogicalTypeID::UINT64); }
    static LogicalType UINT32() { return LogicalType(LogicalTypeID::UINT32); }
    static LogicalType UINT16() { return LogicalType(LogicalTypeID::UINT16); }
    static LogicalType UINT8() { return LogicalType(LogicalTypeID::UINT8); }
    static LogicalType INT128() { return LogicalType(LogicalTypeID::INT128); }
    static LogicalType DOUBLE() { return LogicalType(LogicalTypeID::DOUBLE); }
    static LogicalType FLOAT() { return LogicalType(LogicalTypeID::FLOAT); }
    static LogicalType DATE() { return LogicalType(LogicalTypeID::DATE); }
    static LogicalType TIMESTAMP_NS() { return LogicalType(LogicalTypeID::TIMESTAMP_NS); }
    static LogicalType TIMESTAMP_MS() { return LogicalType(LogicalTypeID::TIMESTAMP_MS); }
    static LogicalType TIMESTAMP_SEC() { return LogicalType(LogicalTypeID::TIMESTAMP_SEC); }
    static LogicalType TIMESTAMP_TZ() { return LogicalType(LogicalTypeID::TIMESTAMP_TZ); }
    static LogicalType TIMESTAMP() { return LogicalType(LogicalTypeID::TIMESTAMP); }
    static LogicalType INTERVAL() { return LogicalType(LogicalTypeID::INTERVAL); }
    static KUZU_API LogicalType DECIMAL(uint32_t precision, uint32_t scale);
    static LogicalType INTERNAL_ID() { return LogicalType(LogicalTypeID::INTERNAL_ID); }
    static LogicalType SERIAL() { return LogicalType(LogicalTypeID::SERIAL); }
    static LogicalType STRING() { return LogicalType(LogicalTypeID::STRING); }
    static LogicalType BLOB() { return LogicalType(LogicalTypeID::BLOB); }
    static LogicalType UUID() { return LogicalType(LogicalTypeID::UUID); }
    static LogicalType POINTER() { return LogicalType(LogicalTypeID::POINTER); }
    static KUZU_API LogicalType STRUCT(std::vector<StructField>&& fields);

    static KUZU_API LogicalType RECURSIVE_REL(std::vector<StructField>&& fields);

    static KUZU_API LogicalType NODE(std::vector<StructField>&& fields);

    static KUZU_API LogicalType REL(std::vector<StructField>&& fields);

    static KUZU_API LogicalType UNION(std::vector<StructField>&& fields);

    static KUZU_API LogicalType LIST(LogicalType childType);
    template<class T>
    static inline LogicalType LIST(T&& childType) {
        return LogicalType::LIST(LogicalType(std::forward<T>(childType)));
    }

    static KUZU_API LogicalType MAP(LogicalType keyType, LogicalType valueType);
    template<class T>
    static LogicalType MAP(T&& keyType, T&& valueType) {
        return LogicalType::MAP(LogicalType(std::forward<T>(keyType)),
            LogicalType(std::forward<T>(valueType)));
    }

    static KUZU_API LogicalType ARRAY(LogicalType childType, uint64_t numElements);
    template<class T>
    static LogicalType ARRAY(T&& childType, uint64_t numElements) {
        return LogicalType::ARRAY(LogicalType(std::forward<T>(childType)), numElements);
    }

private:
    friend struct CAPIHelper;
    friend struct JavaAPIHelper;
    friend class kuzu::processor::ParquetReader;
    explicit LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo);

private:
    LogicalTypeID typeID;
    PhysicalTypeID physicalType;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
    TypeCategory category = TypeCategory::INTERNAL;
};

class KUZU_API ExtraTypeInfo {
public:
    virtual ~ExtraTypeInfo() = default;

    void serialize(Serializer& serializer) const { serializeInternal(serializer); }

    virtual bool containsAny() const = 0;

    virtual bool operator==(const ExtraTypeInfo& other) const = 0;

    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

protected:
    virtual void serializeInternal(Serializer& serializer) const = 0;
};

class KUZU_API UDTTypeInfo : public ExtraTypeInfo {
public:
    explicit UDTTypeInfo(std::string typeName) : typeName{std::move(typeName)} {}

    std::string getTypeName() const { return typeName; }

    bool containsAny() const override { return false; }

    bool operator==(const ExtraTypeInfo& other) const override;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    std::string typeName;
};

class DecimalTypeInfo final : public ExtraTypeInfo {
public:
    explicit DecimalTypeInfo(uint32_t precision = 18, uint32_t scale = 3)
        : precision(precision), scale(scale) {}

    uint32_t getPrecision() const { return precision; }
    uint32_t getScale() const { return scale; }

    bool containsAny() const override { return false; }

    bool operator==(const ExtraTypeInfo& other) const override;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

protected:
    void serializeInternal(Serializer& serializer) const override;

    uint32_t precision, scale;
};

class KUZU_API ListTypeInfo : public ExtraTypeInfo {
public:
    ListTypeInfo() = default;
    explicit ListTypeInfo(LogicalType childType) : childType{std::move(childType)} {}

    const LogicalType& getChildType() const { return childType; }

    bool containsAny() const override;

    bool operator==(const ExtraTypeInfo& other) const override;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

protected:
    void serializeInternal(Serializer& serializer) const override;

protected:
    LogicalType childType;
};

class KUZU_API ArrayTypeInfo final : public ListTypeInfo {
public:
    ArrayTypeInfo() : numElements{0} {};
    explicit ArrayTypeInfo(LogicalType childType, uint64_t numElements)
        : ListTypeInfo{std::move(childType)}, numElements{numElements} {}

    uint64_t getNumElements() const { return numElements; }

    bool operator==(const ExtraTypeInfo& other) const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    uint64_t numElements;
};

class StructField {
public:
    StructField() : type{LogicalType()} {}
    StructField(std::string name, LogicalType type)
        : name{std::move(name)}, type{std::move(type)} {};

    DELETE_COPY_DEFAULT_MOVE(StructField);

    std::string getName() const { return name; }

    const LogicalType& getType() const { return type; }

    bool containsAny() const;

    bool operator==(const StructField& other) const;
    bool operator!=(const StructField& other) const { return !(*this == other); }

    void serialize(Serializer& serializer) const;

    static StructField deserialize(Deserializer& deserializer);

    StructField copy() const;

private:
    std::string name;
    LogicalType type;
};

class StructTypeInfo final : public ExtraTypeInfo {
public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<StructField>&& fields);
    StructTypeInfo(const std::vector<std::string>& fieldNames,
        const std::vector<LogicalType>& fieldTypes);

    bool hasField(const std::string& fieldName) const;
    struct_field_idx_t getStructFieldIdx(std::string fieldName) const;
    const StructField& getStructField(struct_field_idx_t idx) const;
    const StructField& getStructField(const std::string& fieldName) const;
    const std::vector<StructField>& getStructFields() const;

    const LogicalType& getChildType(struct_field_idx_t idx) const;
    std::vector<const LogicalType*> getChildrenTypes() const;
    // can't be a vector of refs since that can't be for-each looped through
    std::vector<std::string> getChildrenNames() const;

    bool containsAny() const override;

    bool operator==(const ExtraTypeInfo& other) const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    std::vector<StructField> fields;
    std::unordered_map<std::string, struct_field_idx_t> fieldNameToIdxMap;
};

using logical_type_vec_t = std::vector<LogicalType>;

struct KUZU_API DecimalType {
    static uint32_t getPrecision(const LogicalType& type);
    static uint32_t getScale(const LogicalType& type);
    static std::string insertDecimalPoint(const std::string& value, uint32_t posFromEnd);
};

struct KUZU_API ListType {
    static const LogicalType& getChildType(const LogicalType& type);
};

struct KUZU_API ArrayType {
    static const LogicalType& getChildType(const LogicalType& type);
    static uint64_t getNumElements(const LogicalType& type);
};

struct KUZU_API StructType {
    static std::vector<const LogicalType*> getFieldTypes(const LogicalType& type);
    // since the field types isn't stored as a vector of LogicalTypes, we can't return vector<>&

    static const LogicalType& getFieldType(const LogicalType& type, struct_field_idx_t idx);

    static const LogicalType& getFieldType(const LogicalType& type, const std::string& key);

    static std::vector<std::string> getFieldNames(const LogicalType& type);

    static uint64_t getNumFields(const LogicalType& type);

    static const std::vector<StructField>& getFields(const LogicalType& type);

    static bool hasField(const LogicalType& type, const std::string& key);

    static const StructField& getField(const LogicalType& type, struct_field_idx_t idx);

    static const StructField& getField(const LogicalType& type, const std::string& key);

    static struct_field_idx_t getFieldIdx(const LogicalType& type, const std::string& key);
};

struct KUZU_API MapType {
    static const LogicalType& getKeyType(const LogicalType& type);

    static const LogicalType& getValueType(const LogicalType& type);
};

struct KUZU_API UnionType {
    static constexpr union_field_idx_t TAG_FIELD_IDX = 0;

    static constexpr auto TAG_FIELD_TYPE = LogicalTypeID::UINT16;

    static constexpr char TAG_FIELD_NAME[] = "tag";

    static union_field_idx_t getInternalFieldIdx(union_field_idx_t idx);

    static std::string getFieldName(const LogicalType& type, union_field_idx_t idx);

    static const LogicalType& getFieldType(const LogicalType& type, union_field_idx_t idx);

    static uint64_t getNumFields(const LogicalType& type);
};

struct PhysicalTypeUtils {
    static std::string toString(PhysicalTypeID physicalType);
    static uint32_t getFixedTypeSize(PhysicalTypeID physicalType);
};

struct KUZU_API LogicalTypeUtils {
    static std::string toString(LogicalTypeID dataTypeID);
    static std::string toString(const std::vector<LogicalType>& dataTypes);
    static std::string toString(const std::vector<LogicalTypeID>& dataTypeIDs);
    static uint32_t getRowLayoutSize(const LogicalType& logicalType);
    static bool isDate(const LogicalType& dataType);
    static bool isDate(const LogicalTypeID& dataType);
    static bool isTimestamp(const LogicalType& dataType);
    static bool isTimestamp(const LogicalTypeID& dataType);
    static bool isUnsigned(const LogicalType& dataType);
    static bool isUnsigned(const LogicalTypeID& dataType);
    static bool isIntegral(const LogicalType& dataType);
    static bool isIntegral(const LogicalTypeID& dataType);
    static bool isNumerical(const LogicalType& dataType);
    static bool isNumerical(const LogicalTypeID& dataType);
    static bool isFloatingPoint(const LogicalTypeID& dataType);
    static bool isNested(const LogicalType& dataType);
    static bool isNested(LogicalTypeID logicalTypeID);
    static std::vector<LogicalTypeID> getAllValidComparableLogicalTypes();
    static std::vector<LogicalTypeID> getNumericalLogicalTypeIDs();
    static std::vector<LogicalTypeID> getIntegerTypeIDs();
    static std::vector<LogicalTypeID> getFloatingPointTypeIDs();
    static std::vector<LogicalTypeID> getAllValidLogicTypeIDs();
    static std::vector<LogicalType> getAllValidLogicTypes();
    static bool tryGetMaxLogicalType(const LogicalType& left, const LogicalType& right,
        LogicalType& result);
    static bool tryGetMaxLogicalType(const std::vector<LogicalType>& types, LogicalType& result);

    // Differs from tryGetMaxLogicalType because it treats string as a maximal type, instead of a
    // minimal type. as such, it will always succeed.
    // Also combines structs by the union of their fields. As such, currently, it is not guaranteed
    // for casting to work from input types to resulting types. Ideally this changes
    static LogicalType combineTypes(const LogicalType& left, const LogicalType& right);
    static LogicalType combineTypes(const std::vector<LogicalType>& types);

    // makes a copy of the type with any occurences of ANY replaced with replacement
    static LogicalType purgeAny(const LogicalType& type, const LogicalType& replacement);

private:
    static bool tryGetMaxLogicalTypeID(const LogicalTypeID& left, const LogicalTypeID& right,
        LogicalTypeID& result);
};

enum class FileVersionType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType)
        : parsingTime{0}, statementType{statementType}, internal{false} {}

    virtual ~Statement() = default;

    common::StatementType getStatementType() const { return statementType; }
    void setToInternal() { internal = true; }
    bool isInternal() const { return internal; }
    void setParsingTime(double time) { parsingTime = time; }
    double getParsingTime() const { return parsingTime; }

    bool requireTransaction() const {
        switch (statementType) {
        case common::StatementType::TRANSACTION:
            return false;
        default:
            return true;
        }
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

private:
    double parsingTime;
    common::StatementType statementType;
    // By setting the statement to internal, we still execute the statement, but will not return the
    // executio result as part of the query result returned to users.
    // The use case for this is when a query internally generates other queries to finish first,
    // e.g., `TableFunction::rewriteFunc`.
    bool internal;
};

} // namespace parser
} // namespace kuzu


namespace kuzu {
namespace common {

struct ku_list_t {

public:
    ku_list_t() : size{0}, overflowPtr{0} {}
    ku_list_t(uint64_t size, uint64_t overflowPtr) : size{size}, overflowPtr{overflowPtr} {}

    void set(const uint8_t* values, const LogicalType& dataType) const;

private:
    void set(const std::vector<uint8_t*>& parameters, LogicalTypeID childTypeId);

public:
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace kuzu

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace kuzu {
namespace binder {

class Expression;
using expression_vector = std::vector<std::shared_ptr<Expression>>;
using expression_pair = std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>;

struct ExpressionHasher;
struct ExpressionEquality;
using expression_set =
    std::unordered_set<std::shared_ptr<Expression>, ExpressionHasher, ExpressionEquality>;
template<typename T>
using expression_map =
    std::unordered_map<std::shared_ptr<Expression>, T, ExpressionHasher, ExpressionEquality>;

class KUZU_API Expression : public std::enable_shared_from_this<Expression> {
    friend class ExpressionChildrenCollector;

public:
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        expression_vector children, std::string uniqueName)
        : expressionType{expressionType}, dataType{std::move(dataType)},
          uniqueName{std::move(uniqueName)}, children{std::move(children)} {}
    // Create binary expression.
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right,
        std::string uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{left, right},
              std::move(uniqueName)} {}
    // Create unary expression.
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        const std::shared_ptr<Expression>& child, std::string uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{child},
              std::move(uniqueName)} {}
    // Create leaf expression
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        std::string uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{},
              std::move(uniqueName)} {}
    DELETE_COPY_DEFAULT_MOVE(Expression);
    virtual ~Expression();

    void setUniqueName(const std::string& name) { uniqueName = name; }
    std::string getUniqueName() const {
        KU_ASSERT(!uniqueName.empty());
        return uniqueName;
    }

    virtual void cast(const common::LogicalType& type);
    const common::LogicalType& getDataType() const { return dataType; }

    void setAlias(const std::string& newAlias) { alias = newAlias; }
    bool hasAlias() const { return !alias.empty(); }
    std::string getAlias() const { return alias; }

    common::idx_t getNumChildren() const { return children.size(); }
    std::shared_ptr<Expression> getChild(common::idx_t idx) const {
        KU_ASSERT(idx < children.size());
        return children[idx];
    }
    expression_vector getChildren() const { return children; }
    void setChild(common::idx_t idx, std::shared_ptr<Expression> child) {
        KU_ASSERT(idx < children.size());
        children[idx] = std::move(child);
    }

    expression_vector splitOnAND();

    bool operator==(const Expression& rhs) const { return uniqueName == rhs.uniqueName; }

    std::string toString() const { return hasAlias() ? alias : toStringInternal(); }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }

protected:
    virtual std::string toStringInternal() const = 0;

public:
    common::ExpressionType expressionType;
    common::LogicalType dataType;

protected:
    // Name that serves as the unique identifier.
    std::string uniqueName;
    std::string alias;
    expression_vector children;
};

struct ExpressionHasher {
    std::size_t operator()(const std::shared_ptr<Expression>& expression) const {
        return std::hash<std::string>{}(expression->getUniqueName());
    }
};

struct ExpressionEquality {
    bool operator()(const std::shared_ptr<Expression>& left,
        const std::shared_ptr<Expression>& right) const {
        return left->getUniqueName() == right->getUniqueName();
    }
};

} // namespace binder
} // namespace kuzu

#include <string.h>

#include <memory>

#include <span>

namespace kuzu {
namespace common {

class ValueVector;

// A lightweight, immutable view over a SelectionVector, or a subsequence of a selection vector
// SelectionVectors are also SelectionViews so that you can pass a SelectionVector to functions
// which take a SelectionView&
class SelectionView {
protected:
    // In DYNAMIC mode, selectedPositions points to a mutable buffer that can be modified through
    // getMutableBuffer In STATIC mode, selectedPositions points to somewhere in
    // INCREMENTAL_SELECTED_POS
    // Note that the vector is considered unfiltered only if it is both STATIC and the first
    // selected position is 0
    enum class State {
        DYNAMIC,
        STATIC,
    };

public:
    // STATIC selectionView over 0..selectedSize
    explicit SelectionView(sel_t selectedSize);

    template<class Func>
    void forEach(Func&& func) const {
        if (state == State::DYNAMIC) {
            for (size_t i = 0; i < selectedSize; i++) {
                func(selectedPositions[i]);
            }
        } else {
            const auto start = selectedPositions[0];
            for (size_t i = start; i < start + selectedSize; i++) {
                func(i);
            }
        }
    }

    template<class Func>
    void forEachBreakWhenFalse(Func&& func) const {
        if (state == State::DYNAMIC) {
            for (size_t i = 0; i < selectedSize; i++) {
                if (!func(selectedPositions[i])) {
                    break;
                }
            }
        } else {
            const auto start = selectedPositions[0];
            for (size_t i = start; i < start + selectedSize; i++) {
                if (!func(i)) {
                    break;
                }
            }
        }
    }

    sel_t getSelSize() const { return selectedSize; }

    sel_t operator[](sel_t index) const {
        KU_ASSERT(index < selectedSize);
        return selectedPositions[index];
    }

    bool isUnfiltered() const { return state == State::STATIC && selectedPositions[0] == 0; }
    bool isStatic() const { return state == State::STATIC; }

    std::span<const sel_t> getSelectedPositions() const {
        return std::span<const sel_t>(selectedPositions, selectedSize);
    }

protected:
    static SelectionView slice(std::span<const sel_t> selectedPositions, State state) {
        return SelectionView(selectedPositions, state);
    }

    // Intended to be used only as a subsequence of a SelectionVector in SelectionVector::slice
    explicit SelectionView(std::span<const sel_t> selectedPositions, State state)
        : selectedPositions{selectedPositions.data()}, selectedSize{selectedPositions.size()},
          state{state} {}

protected:
    const sel_t* selectedPositions;
    sel_t selectedSize;
    State state;
};

class SelectionVector : public SelectionView {
public:
    explicit SelectionVector(sel_t capacity)
        : SelectionView{std::span<const sel_t>(), State::STATIC},
          selectedPositionsBuffer{std::make_unique<sel_t[]>(capacity)}, capacity{capacity} {
        setToUnfiltered();
    }

    // This View should be considered invalid if the SelectionVector it was created from has been
    // modified
    SelectionView slice(sel_t startIndex, sel_t selectedSize) const {
        return SelectionView::slice(getSelectedPositions().subspan(startIndex, selectedSize),
            state);
    }

    SelectionVector();

    KUZU_API void setToUnfiltered();
    KUZU_API void setToUnfiltered(sel_t size);
    void setRange(sel_t startPos, sel_t size) {
        KU_ASSERT(startPos + size <= capacity);
        selectedPositions = selectedPositionsBuffer.get();
        for (auto i = 0u; i < size; ++i) {
            selectedPositionsBuffer[i] = startPos + i;
        }
        selectedSize = size;
        state = State::DYNAMIC;
    }

    // Set to filtered is not very accurate. It sets selectedPositions to a mutable array.
    void setToFiltered() {
        selectedPositions = selectedPositionsBuffer.get();
        state = State::DYNAMIC;
    }
    void setToFiltered(sel_t size) {
        KU_ASSERT(size <= capacity && selectedPositionsBuffer);
        setToFiltered();
        selectedSize = size;
    }

    // Copies the data in selectedPositions into selectedPositionsBuffer
    void makeDynamic() {
        memcpy(selectedPositionsBuffer.get(), selectedPositions, selectedSize * sizeof(sel_t));
        state = State::DYNAMIC;
        selectedPositions = selectedPositionsBuffer.get();
    }

    std::span<sel_t> getMutableBuffer() const {
        return std::span<sel_t>(selectedPositionsBuffer.get(), capacity);
    }

    void setSelSize(sel_t size) {
        KU_ASSERT(size <= capacity);
        selectedSize = size;
    }
    void incrementSelSize(sel_t increment = 1) {
        KU_ASSERT(selectedSize < capacity);
        selectedSize += increment;
    }

    sel_t operator[](sel_t index) const {
        KU_ASSERT(index < capacity);
        return const_cast<sel_t&>(selectedPositions[index]);
    }
    sel_t& operator[](sel_t index) {
        KU_ASSERT(index < capacity);
        return const_cast<sel_t&>(selectedPositions[index]);
    }

    static std::vector<SelectionVector*> fromValueVectors(
        const std::vector<std::shared_ptr<common::ValueVector>>& vec);

private:
    std::unique_ptr<sel_t[]> selectedPositionsBuffer;
    sel_t capacity;
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

class ValueVector;

// AuxiliaryBuffer holds data which is only used by the targeting dataType.
class KUZU_API AuxiliaryBuffer {
public:
    virtual ~AuxiliaryBuffer() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

class StringAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    explicit StringAuxiliaryBuffer(storage::MemoryManager* memoryManager) {
        inMemOverflowBuffer = std::make_unique<InMemOverflowBuffer>(memoryManager);
    }

    InMemOverflowBuffer* getOverflowBuffer() const { return inMemOverflowBuffer.get(); }
    uint8_t* allocateOverflow(uint64_t size) { return inMemOverflowBuffer->allocateSpace(size); }
    void resetOverflowBuffer() const { inMemOverflowBuffer->resetBuffer(); }

private:
    std::unique_ptr<InMemOverflowBuffer> inMemOverflowBuffer;
};

class KUZU_API StructAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    StructAuxiliaryBuffer(const LogicalType& type, storage::MemoryManager* memoryManager);

    void referenceChildVector(idx_t idx, std::shared_ptr<ValueVector> vectorToReference) {
        childrenVectors[idx] = std::move(vectorToReference);
    }
    const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors() const {
        return childrenVectors;
    }
    std::shared_ptr<ValueVector> getFieldVectorShared(idx_t idx) const {
        return childrenVectors[idx];
    }
    ValueVector* getFieldVectorPtr(idx_t idx) const { return childrenVectors[idx].get(); }

private:
    std::vector<std::shared_ptr<ValueVector>> childrenVectors;
};

// ListVector layout:
// To store a list value in the valueVector, we could use two separate vectors.
// 1. A vector(called offset vector) for the list offsets and length(called list_entry_t): This
// vector contains the starting indices and length for each list within the data vector.
// 2. A data vector(called dataVector) to store the actual list elements: This vector holds the
// actual elements of the lists in a flat, continuous storage. Each list would be represented as a
// contiguous subsequence of elements in this vector.
class KUZU_API ListAuxiliaryBuffer : public AuxiliaryBuffer {
    friend class ListVector;

public:
    ListAuxiliaryBuffer(const LogicalType& dataVectorType, storage::MemoryManager* memoryManager);

    void setDataVector(std::shared_ptr<ValueVector> vector) { dataVector = std::move(vector); }
    ValueVector* getDataVector() const { return dataVector.get(); }
    std::shared_ptr<ValueVector> getSharedDataVector() const { return dataVector; }

    list_entry_t addList(list_size_t listSize);

    uint64_t getSize() const { return size; }

    void resetSize() { size = 0; }

    void resize(uint64_t numValues);

private:
    void resizeDataVector(ValueVector* dataVector);

    void resizeStructDataVector(ValueVector* dataVector);

private:
    uint64_t capacity;
    uint64_t size;

    std::shared_ptr<ValueVector> dataVector;
};

class AuxiliaryBufferFactory {
public:
    static std::unique_ptr<AuxiliaryBuffer> getAuxiliaryBuffer(LogicalType& type,
        storage::MemoryManager* memoryManager);
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

// Note that this class is NOT thread-safe.
class SemiMask {
public:
    explicit SemiMask(offset_t maxOffset) : maxOffset{maxOffset}, enabled{false} {}

    virtual ~SemiMask() = default;

    virtual void mask(offset_t nodeOffset) = 0;
    virtual void maskRange(offset_t startNodeOffset, offset_t endNodeOffset) = 0;

    virtual bool isMasked(offset_t startNodeOffset) = 0;

    // include&exclude
    virtual offset_vec_t range(uint32_t start, uint32_t end) = 0;

    virtual uint64_t getNumMaskedNodes() const = 0;

    virtual offset_vec_t collectMaskedNodes(uint64_t size) const = 0;

    offset_t getMaxOffset() const { return maxOffset; }

    bool isEnabled() const { return enabled; }
    void enable() { enabled = true; }

private:
    offset_t maxOffset;
    bool enabled;
};

struct SemiMaskUtil {
    KUZU_API static std::unique_ptr<SemiMask> createMask(offset_t maxOffset);
};

class NodeOffsetMaskMap {
public:
    NodeOffsetMaskMap() = default;

    offset_t getNumMaskedNode() const;

    void addMask(table_id_t tableID, std::unique_ptr<SemiMask> mask) {
        KU_ASSERT(!maskMap.contains(tableID));
        maskMap.insert({tableID, std::move(mask)});
    }

    table_id_map_t<SemiMask*> getMasks() const {
        table_id_map_t<SemiMask*> result;
        for (auto& [tableID, mask] : maskMap) {
            result.emplace(tableID, mask.get());
        }
        return result;
    }

    bool containsTableID(table_id_t tableID) const { return maskMap.contains(tableID); }
    SemiMask* getOffsetMask(table_id_t tableID) const {
        KU_ASSERT(containsTableID(tableID));
        return maskMap.at(tableID).get();
    }

    void pin(table_id_t tableID) {
        if (maskMap.contains(tableID)) {
            pinnedMask = maskMap.at(tableID).get();
        } else {
            pinnedMask = nullptr;
        }
    }
    bool hasPinnedMask() const { return pinnedMask != nullptr; }
    SemiMask* getPinnedMask() const { return pinnedMask; }

    bool valid(offset_t offset) const {
        KU_ASSERT(pinnedMask != nullptr);
        return pinnedMask->isMasked(offset);
    }
    bool valid(nodeID_t nodeID) const {
        KU_ASSERT(maskMap.contains(nodeID.tableID));
        return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
    }

private:
    table_id_map_t<std::unique_ptr<SemiMask>> maskMap;
    SemiMask* pinnedMask = nullptr;
};

} // namespace common
} // namespace kuzu

#include <utility>


namespace kuzu {
namespace processor {

using data_chunk_pos_t = common::idx_t;
constexpr data_chunk_pos_t INVALID_DATA_CHUNK_POS = common::INVALID_IDX;
using value_vector_pos_t = common::idx_t;
constexpr value_vector_pos_t INVALID_VALUE_VECTOR_POS = common::INVALID_IDX;

struct DataPos {
    data_chunk_pos_t dataChunkPos;
    value_vector_pos_t valueVectorPos;

    DataPos() : dataChunkPos{INVALID_DATA_CHUNK_POS}, valueVectorPos{INVALID_VALUE_VECTOR_POS} {}
    explicit DataPos(data_chunk_pos_t dataChunkPos, value_vector_pos_t valueVectorPos)
        : dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos} {}
    explicit DataPos(std::pair<data_chunk_pos_t, value_vector_pos_t> pos)
        : dataChunkPos{pos.first}, valueVectorPos{pos.second} {}

    static DataPos getInvalidPos() { return DataPos(); }
    bool isValid() const {
        return dataChunkPos != INVALID_DATA_CHUNK_POS && valueVectorPos != INVALID_VALUE_VECTOR_POS;
    }

    inline bool operator==(const DataPos& rhs) const {
        return (dataChunkPos == rhs.dataChunkPos) && (valueVectorPos == rhs.valueVectorPos);
    }
};

} // namespace processor
} // namespace kuzu


namespace kuzu {
namespace planner {
class Schema;
} // namespace planner

namespace processor {

struct DataChunkDescriptor {
    bool isSingleState;
    std::vector<common::LogicalType> logicalTypes;

    explicit DataChunkDescriptor(bool isSingleState) : isSingleState{isSingleState} {}
    DataChunkDescriptor(const DataChunkDescriptor& other)
        : isSingleState{other.isSingleState},
          logicalTypes(common::LogicalType::copy(other.logicalTypes)) {}

    inline std::unique_ptr<DataChunkDescriptor> copy() const {
        return std::make_unique<DataChunkDescriptor>(*this);
    }
};

struct KUZU_API ResultSetDescriptor {
    std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;

    ResultSetDescriptor() = default;
    explicit ResultSetDescriptor(
        std::vector<std::unique_ptr<DataChunkDescriptor>> dataChunkDescriptors)
        : dataChunkDescriptors{std::move(dataChunkDescriptors)} {}
    explicit ResultSetDescriptor(planner::Schema* schema);
    DELETE_BOTH_COPY(ResultSetDescriptor);

    std::unique_ptr<ResultSetDescriptor> copy() const;

    static std::unique_ptr<ResultSetDescriptor> EmptyDescriptor() {
        return std::make_unique<ResultSetDescriptor>();
    }
};

} // namespace processor
} // namespace kuzu

#include <array>
#include <cstdint>
#include <string_view>


namespace kuzu {
namespace common {

extern const char* KUZU_VERSION;

constexpr double DEFAULT_HT_LOAD_FACTOR = 1.5;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 500;

constexpr uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS = 5000000;

// Note that some places use std::bit_ceil to calculate resizes,
// which won't work for values other than 2. If this is changed, those will need to be updated
constexpr uint64_t CHUNK_RESIZE_RATIO = 2;

struct InternalKeyword {
    static constexpr char ANONYMOUS[] = "";
    static constexpr char ID[] = "_ID";
    static constexpr char LABEL[] = "_LABEL";
    static constexpr char SRC[] = "_SRC";
    static constexpr char DST[] = "_DST";
    static constexpr char DIRECTION[] = "_DIRECTION";
    static constexpr char LENGTH[] = "_LENGTH";
    static constexpr char NODES[] = "_NODES";
    static constexpr char RELS[] = "_RELS";
    static constexpr char STAR[] = "*";
    static constexpr char PLACE_HOLDER[] = "_PLACE_HOLDER";
    static constexpr char MAP_KEY[] = "KEY";
    static constexpr char MAP_VALUE[] = "VALUE";

    static constexpr std::string_view ROW_OFFSET = "_row_offset";
    static constexpr std::string_view SRC_OFFSET = "_src_offset";
    static constexpr std::string_view DST_OFFSET = "_dst_offset";
};

enum PageSizeClass : uint8_t {
    REGULAR_PAGE = 0,
    TEMP_PAGE = 1,
};

struct BufferPoolConstants {
    // If a user does not specify a max size for BM, we by default set the max size of BM to
    // maxPhyMemSize * DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM.
    static constexpr double DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM = 0.8;
// The default max size for a VMRegion.
#ifdef __32BIT__
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 30; // (1GB)
#elif defined(__ANDROID__)
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 38; // (256GB)
#else
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = static_cast<uint64_t>(1) << 43; // (8TB)
#endif
};

struct StorageConstants {
    static constexpr page_idx_t DB_HEADER_PAGE_IDX = 0;
    static constexpr char WAL_FILE_SUFFIX[] = "wal";
    static constexpr char SHADOWING_SUFFIX[] = "shadow";
    static constexpr char TEMP_FILE_SUFFIX[] = "tmp";

    // The number of pages that we add at one time when we need to grow a file.
    static constexpr uint64_t PAGE_GROUP_SIZE_LOG2 = 10;
    static constexpr uint64_t PAGE_GROUP_SIZE = static_cast<uint64_t>(1) << PAGE_GROUP_SIZE_LOG2;
    static constexpr uint64_t PAGE_IDX_IN_GROUP_MASK =
        (static_cast<uint64_t>(1) << PAGE_GROUP_SIZE_LOG2) - 1;

    static constexpr double PACKED_CSR_DENSITY = 0.8;
    static constexpr double LEAF_HIGH_CSR_DENSITY = 1.0;

    static constexpr uint64_t MAX_NUM_ROWS_IN_TABLE = static_cast<uint64_t>(1) << 62;
};

struct TableOptionConstants {
    static constexpr char REL_STORAGE_DIRECTION_OPTION[] = "STORAGE_DIRECTION";
};

// Hash Index Configurations
struct HashIndexConstants {
    static constexpr uint16_t SLOT_CAPACITY_BYTES = 256;
    static constexpr uint64_t NUM_HASH_INDEXES_LOG2 = 8;
    static constexpr uint64_t NUM_HASH_INDEXES = 1 << NUM_HASH_INDEXES_LOG2;
};

struct CopyConstants {
    // Initial size of buffer for CSV Reader.
    static constexpr uint64_t INITIAL_BUFFER_SIZE = 16384;
    // This means that we will usually read the entirety of the contents of the file we need for a
    // block in one read request. It is also very small, which means we can parallelize small files
    // efficiently.
    static constexpr uint64_t PARALLEL_BLOCK_SIZE = INITIAL_BUFFER_SIZE / 2;

    static constexpr const char* IGNORE_ERRORS_OPTION_NAME = "IGNORE_ERRORS";

    static constexpr const char* FROM_OPTION_NAME = "FROM";
    static constexpr const char* TO_OPTION_NAME = "TO";

    static constexpr const char* BOOL_CSV_PARSING_OPTIONS[] = {"HEADER", "PARALLEL",
        "LIST_UNBRACED", "AUTODETECT", "AUTO_DETECT", CopyConstants::IGNORE_ERRORS_OPTION_NAME};
    static constexpr bool DEFAULT_CSV_HAS_HEADER = false;
    static constexpr bool DEFAULT_CSV_PARALLEL = true;

    // Default configuration for csv file parsing
    static constexpr const char* STRING_CSV_PARSING_OPTIONS[] = {"ESCAPE", "DELIM", "DELIMITER",
        "QUOTE"};
    static constexpr char DEFAULT_CSV_ESCAPE_CHAR = '"';
    static constexpr char DEFAULT_CSV_DELIMITER = ',';
    static constexpr bool DEFAULT_CSV_ALLOW_UNBRACED_LIST = false;
    static constexpr char DEFAULT_CSV_QUOTE_CHAR = '"';
    static constexpr char DEFAULT_CSV_LIST_BEGIN_CHAR = '[';
    static constexpr char DEFAULT_CSV_LIST_END_CHAR = ']';
    static constexpr bool DEFAULT_IGNORE_ERRORS = false;
    static constexpr bool DEFAULT_CSV_AUTO_DETECT = true;
    static constexpr bool DEFAULT_CSV_SET_DIALECT = false;
    static constexpr std::array DEFAULT_CSV_DELIMITER_SEARCH_SPACE = {',', ';', '\t', '|'};
    static constexpr std::array DEFAULT_CSV_QUOTE_SEARCH_SPACE = {'"', '\''};
    static constexpr std::array DEFAULT_CSV_ESCAPE_SEARCH_SPACE = {'"', '\\', '\''};
    static constexpr std::array DEFAULT_CSV_NULL_STRINGS = {""};

    static constexpr const char* INT_CSV_PARSING_OPTIONS[] = {"SKIP", "SAMPLE_SIZE"};
    static constexpr uint64_t DEFAULT_CSV_SKIP_NUM = 0;
    static constexpr uint64_t DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE = 256;

    static constexpr const char* LIST_CSV_PARSING_OPTIONS[] = {"NULL_STRINGS"};

    // metadata columns used to populate CSV warnings
    static constexpr std::array SHARED_WARNING_DATA_COLUMN_NAMES = {"blockIdx", "offsetInBlock",
        "startByteOffset", "endByteOffset"};
    static constexpr std::array SHARED_WARNING_DATA_COLUMN_TYPES = {LogicalTypeID::UINT64,
        LogicalTypeID::UINT32, LogicalTypeID::UINT64, LogicalTypeID::UINT64};
    static constexpr column_id_t SHARED_WARNING_DATA_NUM_COLUMNS =
        SHARED_WARNING_DATA_COLUMN_NAMES.size();

    static constexpr std::array CSV_SPECIFIC_WARNING_DATA_COLUMN_NAMES = {"fileIdx"};
    static constexpr std::array CSV_SPECIFIC_WARNING_DATA_COLUMN_TYPES = {LogicalTypeID::UINT32};

    static constexpr std::array CSV_WARNING_DATA_COLUMN_NAMES =
        arrayConcat(SHARED_WARNING_DATA_COLUMN_NAMES, CSV_SPECIFIC_WARNING_DATA_COLUMN_NAMES);
    static constexpr std::array CSV_WARNING_DATA_COLUMN_TYPES =
        arrayConcat(SHARED_WARNING_DATA_COLUMN_TYPES, CSV_SPECIFIC_WARNING_DATA_COLUMN_TYPES);
    static constexpr column_id_t CSV_WARNING_DATA_NUM_COLUMNS =
        CSV_WARNING_DATA_COLUMN_NAMES.size();
    static_assert(CSV_WARNING_DATA_NUM_COLUMNS == CSV_WARNING_DATA_COLUMN_TYPES.size());

    static constexpr column_id_t MAX_NUM_WARNING_DATA_COLUMNS = CSV_WARNING_DATA_NUM_COLUMNS;
};

struct PlannerKnobs {
    static constexpr double NON_EQUALITY_PREDICATE_SELECTIVITY = 0.1;
    static constexpr double EQUALITY_PREDICATE_SELECTIVITY = 0.01;
    static constexpr uint64_t BUILD_PENALTY = 2;
    // Avoid doing probe to build SIP if we have to accumulate a probe side that is much bigger than
    // build side. Also avoid doing build to probe SIP if probe side is not much bigger than build.
    static constexpr uint64_t SIP_RATIO = 5;
};

struct OrderByConstants {
    static constexpr uint64_t NUM_BYTES_FOR_PAYLOAD_IDX = 8;
    static constexpr uint64_t MIN_LIMIT_RATIO_TO_REDUCE = 2;
};

struct ParquetConstants {
    static constexpr uint64_t PARQUET_DEFINE_VALID = 65535;
    static constexpr const char* PARQUET_MAGIC_WORDS = "PAR1";
    // We limit the uncompressed page size to 100MB.
    // The max size in Parquet is 2GB, but we choose a more conservative limit.
    static constexpr uint64_t MAX_UNCOMPRESSED_PAGE_SIZE = 100000000;
    // Dictionary pages must be below 2GB. Unlike data pages, there's only one dictionary page.
    // For this reason we go with a much higher, but still a conservative upper bound of 1GB.
    static constexpr uint64_t MAX_UNCOMPRESSED_DICT_PAGE_SIZE = 1e9;
    // The maximum size a key entry in an RLE page takes.
    static constexpr uint64_t MAX_DICTIONARY_KEY_SIZE = sizeof(uint32_t);
    // The size of encoding the string length.
    static constexpr uint64_t STRING_LENGTH_SIZE = sizeof(uint32_t);
    static constexpr uint64_t MAX_STRING_STATISTICS_SIZE = 10000;
    static constexpr uint64_t PARQUET_INTERVAL_SIZE = 12;
    static constexpr uint64_t PARQUET_UUID_SIZE = 16;
};

struct ExportCSVConstants {
    static constexpr const char* DEFAULT_CSV_NEWLINE = "\n\r";
    static constexpr const char* DEFAULT_NULL_STR = "";
    static constexpr bool DEFAULT_FORCE_QUOTE = false;
    static constexpr uint64_t DEFAULT_CSV_FLUSH_SIZE = 4096 * 8;
};

struct PortDBConstants {
    static constexpr char INDEX_FILE_NAME[] = "index.cypher";
    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_FILE_NAME[] = "copy.cypher";
    static constexpr const char* SCHEMA_ONLY_OPTION = "SCHEMA_ONLY";
    static constexpr const char* EXPORT_FORMAT_OPTION = "FORMAT";
    static constexpr const char* DEFAULT_EXPORT_FORMAT_OPTION = "PARQUET";
};

struct WarningConstants {
    static constexpr std::array WARNING_TABLE_COLUMN_NAMES{"query_id", "message", "file_path",
        "line_number", "skipped_line_or_record"};
    static constexpr std::array WARNING_TABLE_COLUMN_DATA_TYPES{LogicalTypeID::UINT64,
        LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::UINT64, LogicalTypeID::STRING};
    static constexpr uint64_t WARNING_TABLE_NUM_COLUMNS = WARNING_TABLE_COLUMN_NAMES.size();

    static_assert(WARNING_TABLE_COLUMN_DATA_TYPES.size() == WARNING_TABLE_NUM_COLUMNS);
};

static constexpr char ATTACHED_KUZU_DB_TYPE[] = "KUZU";

static constexpr char LOCAL_DB_NAME[] = "local(kuzu)";

constexpr auto DECIMAL_PRECISION_LIMIT = 38;

} // namespace common
} // namespace kuzu

#include <type_traits>


namespace kuzu {
namespace common {

class ValueVector;

template<class... Funcs>
struct overload : Funcs... {
    explicit overload(Funcs... funcs) : Funcs(funcs)... {}
    using Funcs::operator()...;
};

class TypeUtils {
public:
    template<typename Func, typename... Types, size_t... indices>
    static void paramPackForEachHelper(const Func& func, std::index_sequence<indices...>,
        Types&&... values) {
        ((func(indices, values)), ...);
    }

    template<typename Func, typename... Types>
    static void paramPackForEach(const Func& func, Types&&... values) {
        paramPackForEachHelper(func, std::index_sequence_for<Types...>(),
            std::forward<Types>(values)...);
    }

    static std::string entryToString(const LogicalType& dataType, const uint8_t* value,
        ValueVector* vector);

    template<typename T>
    static inline std::string toString(const T& val, void* /*valueVector*/ = nullptr) {
        if constexpr (std::is_same_v<T, std::string>) {
            return val;
        } else if constexpr (std::is_same_v<T, ku_string_t>) {
            return val.getAsString();
        } else {
            static_assert(std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
                          std::is_same<T, int16_t>::value || std::is_same<T, int8_t>::value ||
                          std::is_same<T, uint64_t>::value || std::is_same<T, uint32_t>::value ||
                          std::is_same<T, uint16_t>::value || std::is_same<T, uint8_t>::value ||
                          std::is_same<T, double>::value || std::is_same<T, float>::value);
            return std::to_string(val);
        }
    }
    static std::string nodeToString(const struct_entry_t& val, ValueVector* vector);
    static std::string relToString(const struct_entry_t& val, ValueVector* vector);

    static inline void encodeOverflowPtr(uint64_t& overflowPtr, page_idx_t pageIdx,
        uint32_t pageOffset) {
        memcpy(&overflowPtr, &pageIdx, 4);
        memcpy(((uint8_t*)&overflowPtr) + 4, &pageOffset, 4);
    }
    static inline void decodeOverflowPtr(uint64_t overflowPtr, page_idx_t& pageIdx,
        uint32_t& pageOffset) {
        pageIdx = 0;
        memcpy(&pageIdx, &overflowPtr, 4);
        memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 4, 4);
    }

    template<typename T>
    static inline constexpr common::PhysicalTypeID getPhysicalTypeIDForType() {
        if constexpr (std::is_same_v<T, int64_t>) {
            return common::PhysicalTypeID::INT64;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            return common::PhysicalTypeID::INT32;
        } else if constexpr (std::is_same_v<T, int16_t>) {
            return common::PhysicalTypeID::INT16;
        } else if constexpr (std::is_same_v<T, int8_t>) {
            return common::PhysicalTypeID::INT8;
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            return common::PhysicalTypeID::UINT64;
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            return common::PhysicalTypeID::UINT32;
        } else if constexpr (std::is_same_v<T, uint16_t>) {
            return common::PhysicalTypeID::UINT16;
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            return common::PhysicalTypeID::UINT8;
        } else if constexpr (std::is_same_v<T, float>) {
            return common::PhysicalTypeID::FLOAT;
        } else if constexpr (std::is_same_v<T, double>) {
            return common::PhysicalTypeID::DOUBLE;
        } else if constexpr (std::is_same_v<T, int128_t>) {
            return common::PhysicalTypeID::INT128;
        } else if constexpr (std::is_same_v<T, interval_t>) {
            return common::PhysicalTypeID::INTERVAL;
        } else if constexpr (std::same_as<T, ku_string_t> || std::same_as<T, std::string> ||
                             std::same_as<T, std::string_view>) {
            return common::PhysicalTypeID::STRING;
        } else {
            KU_UNREACHABLE;
        }
    }

    /*
     * TypeUtils::visit can be used to call generic code on all or some Logical and Physical type
     * variants with access to type information.
     *
     * E.g.
     *
     *  std::string result;
     *  visit(dataType, [&]<typename T>(T) {
     *      if constexpr(std::is_same_v<T, ku_string_t>()) {
     *          result = vector->getValue<ku_string_t>(0).getAsString();
     *      } else if (std::integral<T>) {
     *          result = std::to_string(vector->getValue<T>(0));
     *      } else {
     *          KU_UNREACHABLE;
     *      }
     *  });
     *
     * or
     *  std::string result;
     *  visit(dataType,
     *      [&](ku_string_t) {
     *          result = vector->getValue<ku_string_t>(0);
     *      },
     *      [&]<std::integral T>(T) {
     *          result = std::to_string(vector->getValue<T>(0));
     *      },
     *      [](auto) { KU_UNREACHABLE; }
     *  );
     *
     * Note that when multiple functions are provided, at least one function must match all data
     * types.
     *
     * Also note that implicit conversions may occur with the multi-function variant
     * if you don't include a generic auto function to cover types which aren't explicitly included.
     * See https://en.cppreference.com/w/cpp/utility/variant/visit
     */
    template<typename... Fs>
    static inline auto visit(const LogicalType& dataType, Fs... funcs) {
        // Note: arguments are used only for type deduction and have no meaningful value.
        // They should be optimized out by the compiler
        auto func = overload(funcs...);
        switch (dataType.getLogicalTypeID()) {
        /* NOLINTBEGIN(bugprone-branch-clone)*/
        case LogicalTypeID::INT8:
            return func(int8_t());
        case LogicalTypeID::UINT8:
            return func(uint8_t());
        case LogicalTypeID::INT16:
            return func(int16_t());
        case LogicalTypeID::UINT16:
            return func(uint16_t());
        case LogicalTypeID::INT32:
            return func(int32_t());
        case LogicalTypeID::UINT32:
            return func(uint32_t());
        case LogicalTypeID::SERIAL:
        case LogicalTypeID::INT64:
            return func(int64_t());
        case LogicalTypeID::UINT64:
            return func(uint64_t());
        case LogicalTypeID::BOOL:
            return func(bool());
        case LogicalTypeID::INT128:
            return func(int128_t());
        case LogicalTypeID::DOUBLE:
            return func(double());
        case LogicalTypeID::FLOAT:
            return func(float());
        case LogicalTypeID::DECIMAL:
            switch (dataType.getPhysicalType()) {
            case PhysicalTypeID::INT16:
                return func(int16_t());
            case PhysicalTypeID::INT32:
                return func(int32_t());
            case PhysicalTypeID::INT64:
                return func(int64_t());
            case PhysicalTypeID::INT128:
                return func(int128_t());
            default:
                KU_UNREACHABLE;
            }
        case LogicalTypeID::INTERVAL:
            return func(interval_t());
        case LogicalTypeID::INTERNAL_ID:
            return func(internalID_t());
        case LogicalTypeID::STRING:
            return func(ku_string_t());
        case LogicalTypeID::DATE:
            return func(date_t());
        case LogicalTypeID::TIMESTAMP_NS:
            return func(timestamp_ns_t());
        case LogicalTypeID::TIMESTAMP_MS:
            return func(timestamp_ms_t());
        case LogicalTypeID::TIMESTAMP_SEC:
            return func(timestamp_sec_t());
        case LogicalTypeID::TIMESTAMP_TZ:
            return func(timestamp_tz_t());
        case LogicalTypeID::TIMESTAMP:
            return func(timestamp_t());
        case LogicalTypeID::BLOB:
            return func(blob_t());
        case LogicalTypeID::UUID:
            return func(ku_uuid_t());
        case LogicalTypeID::ARRAY:
        case LogicalTypeID::LIST:
            return func(list_entry_t());
        case LogicalTypeID::MAP:
            return func(map_entry_t());
        case LogicalTypeID::NODE:
        case LogicalTypeID::REL:
        case LogicalTypeID::RECURSIVE_REL:
        case LogicalTypeID::STRUCT:
            return func(struct_entry_t());
        case LogicalTypeID::UNION:
            return func(union_entry_t());
        /* NOLINTEND(bugprone-branch-clone)*/
        default:
            // Unsupported type
            KU_UNREACHABLE;
        }
    }

    template<typename... Fs>
    static inline auto visit(PhysicalTypeID dataType, Fs&&... funcs) {
        // Note: arguments are used only for type deduction and have no meaningful value.
        // They should be optimized out by the compiler
        auto func = overload(funcs...);
        switch (dataType) {
        /* NOLINTBEGIN(bugprone-branch-clone)*/
        case PhysicalTypeID::INT8:
            return func(int8_t());
        case PhysicalTypeID::UINT8:
            return func(uint8_t());
        case PhysicalTypeID::INT16:
            return func(int16_t());
        case PhysicalTypeID::UINT16:
            return func(uint16_t());
        case PhysicalTypeID::INT32:
            return func(int32_t());
        case PhysicalTypeID::UINT32:
            return func(uint32_t());
        case PhysicalTypeID::INT64:
            return func(int64_t());
        case PhysicalTypeID::UINT64:
            return func(uint64_t());
        case PhysicalTypeID::BOOL:
            return func(bool());
        case PhysicalTypeID::INT128:
            return func(int128_t());
        case PhysicalTypeID::DOUBLE:
            return func(double());
        case PhysicalTypeID::FLOAT:
            return func(float());
        case PhysicalTypeID::INTERVAL:
            return func(interval_t());
        case PhysicalTypeID::INTERNAL_ID:
            return func(internalID_t());
        case PhysicalTypeID::STRING:
            return func(ku_string_t());
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST:
            return func(list_entry_t());
        case PhysicalTypeID::STRUCT:
            return func(struct_entry_t());
        /* NOLINTEND(bugprone-branch-clone)*/
        case PhysicalTypeID::ANY:
        case PhysicalTypeID::POINTER:
        case PhysicalTypeID::ALP_EXCEPTION_DOUBLE:
        case PhysicalTypeID::ALP_EXCEPTION_FLOAT:
            // Unsupported type
            KU_UNREACHABLE;
            // Needed for return type deduction to work
            return func(uint8_t());
        default:
            KU_UNREACHABLE;
        }
    }
};

// Forward declaration of template specializations.
template<>
std::string TypeUtils::toString(const int128_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const bool& val, void* valueVector);
template<>
std::string TypeUtils::toString(const internalID_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const date_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_ns_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_ms_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_sec_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_tz_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const interval_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const ku_string_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const blob_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const ku_uuid_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const list_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const map_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const struct_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const union_entry_t& val, void* valueVector);

} // namespace common
} // namespace kuzu

#include <atomic>
#include <mutex>


namespace kuzu {
namespace binder {
struct BoundAlterInfo;
}
namespace catalog {
class CatalogEntry;
class CatalogSet;
class SequenceCatalogEntry;
struct SequenceRollbackData;
} // namespace catalog
namespace main {
class ClientContext;
} // namespace main
namespace storage {
class LocalWAL;
class LocalStorage;
class UndoBuffer;
class WAL;
class VersionInfo;
class UpdateInfo;
struct VectorUpdateInfo;
class ChunkedNodeGroup;
class VersionRecordHandler;
} // namespace storage
namespace transaction {
class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE, CHECKPOINT, DUMMY, RECOVERY };

class LocalCacheManager;
class KUZU_API LocalCacheObject {
public:
    explicit LocalCacheObject(std::string key) : key{std::move(key)} {}

    virtual ~LocalCacheObject() = default;

    std::string getKey() const { return key; }

    template<typename T>
    T* cast() {
        return common::ku_dynamic_cast<T*>(this);
    }

private:
    std::string key;
};

class LocalCacheManager {
public:
    bool contains(const std::string& key) {
        std::unique_lock lck{mtx};
        return cachedObjects.contains(key);
    }
    LocalCacheObject& at(const std::string& key) {
        std::unique_lock lck{mtx};
        return *cachedObjects.at(key);
    }
    bool put(std::unique_ptr<LocalCacheObject> object);

    void remove(const std::string& key) {
        std::unique_lock lck{mtx};
        cachedObjects.erase(key);
    }

private:
    std::unordered_map<std::string, std::unique_ptr<LocalCacheObject>> cachedObjects;
    std::mutex mtx;
};

class KUZU_API Transaction {
    friend class TransactionManager;

public:
    static constexpr common::transaction_t DUMMY_TRANSACTION_ID = 0;
    static constexpr common::transaction_t DUMMY_START_TIMESTAMP = 0;
    static constexpr common::transaction_t START_TRANSACTION_ID =
        static_cast<common::transaction_t>(1) << 63;

    Transaction(main::ClientContext& clientContext, TransactionType transactionType,
        common::transaction_t transactionID, common::transaction_t startTS);

    explicit Transaction(TransactionType transactionType) noexcept;
    Transaction(TransactionType transactionType, common::transaction_t ID,
        common::transaction_t startTS) noexcept;

    ~Transaction();

    TransactionType getType() const { return type; }
    bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    bool isDummy() const { return TransactionType::DUMMY == type; }
    bool isRecovery() const { return TransactionType::RECOVERY == type; }
    common::transaction_t getID() const { return ID; }
    common::transaction_t getStartTS() const { return startTS; }
    common::transaction_t getCommitTS() const { return commitTS; }
    int64_t getCurrentTS() const { return currentTS; }

    void setForceCheckpoint() { forceCheckpoint = true; }
    bool shouldAppendToUndoBuffer() const {
        // Only write transactions and recovery transactions should append to the undo buffer.
        return isWriteTransaction() || isRecovery();
    }
    bool shouldLogToWAL() const;
    storage::LocalWAL& getLocalWAL() const {
        KU_ASSERT(localWAL);
        return *localWAL;
    }

    bool shouldForceCheckpoint() const;

    void commit(storage::WAL* wal);
    void rollback(storage::WAL* wal);

    uint64_t getEstimatedMemUsage() const;
    storage::LocalStorage* getLocalStorage() const { return localStorage.get(); }
    LocalCacheManager& getLocalCacheManager() { return localCacheManager; }
    bool isUnCommitted(common::table_id_t tableID, common::offset_t nodeOffset) const;
    common::row_idx_t getLocalRowIdx(common::table_id_t tableID,
        common::offset_t nodeOffset) const {
        return nodeOffset - getMinUncommittedNodeOffset(tableID);
    }
    common::offset_t getUncommittedOffset(common::table_id_t tableID,
        common::row_idx_t localRowIdx) const {
        return getMinUncommittedNodeOffset(tableID) + localRowIdx;
    }

    void pushCreateDropCatalogEntry(catalog::CatalogSet& catalogSet,
        catalog::CatalogEntry& catalogEntry, bool isInternal, bool skipLoggingToWAL = false);
    void pushAlterCatalogEntry(catalog::CatalogSet& catalogSet, catalog::CatalogEntry& catalogEntry,
        const binder::BoundAlterInfo& alterInfo);
    void pushSequenceChange(catalog::SequenceCatalogEntry* sequenceEntry, int64_t kCount,
        const catalog::SequenceRollbackData& data);
    void pushInsertInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const;
    void pushDeleteInfo(common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
        common::row_idx_t numRows, const storage::VersionRecordHandler* versionRecordHandler) const;
    void pushVectorUpdateInfo(storage::UpdateInfo& updateInfo, common::idx_t vectorIdx,
        storage::VectorUpdateInfo& vectorUpdateInfo) const;

private:
    common::offset_t getMinUncommittedNodeOffset(common::table_id_t tableID) const;

private:
    TransactionType type;
    common::transaction_t ID;
    common::transaction_t startTS;
    common::transaction_t commitTS;
    int64_t currentTS;
    main::ClientContext* clientContext;
    std::unique_ptr<storage::LocalStorage> localStorage;
    std::unique_ptr<storage::UndoBuffer> undoBuffer;
    std::unique_ptr<storage::LocalWAL> localWAL;
    LocalCacheManager localCacheManager;
    bool forceCheckpoint;
    std::atomic<bool> hasCatalogChanges;
};

// TODO(bmwinger): These shouldn't need to be exported
extern KUZU_API Transaction DUMMY_TRANSACTION;
extern KUZU_API Transaction DUMMY_CHECKPOINT_TRANSACTION;

} // namespace transaction
} // namespace kuzu

#include <memory>
#include <string>
#include <unordered_map>


namespace kuzu {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning the same query for
 * repeated execution.
 */
class PreparedStatement {
    friend class Connection;
    friend class ClientContext;
    friend class testing::TestHelper;
    friend class testing::TestRunner;

public:
    bool isTransactionStatement() const;
    /**
     * @return the query is prepared successfully or not.
     */
    KUZU_API bool isSuccess() const;
    /**
     * @return the error message if the query is not prepared successfully.
     */
    KUZU_API std::string getErrorMessage() const;
    /**
     * @return the prepared statement is read-only or not.
     */
    KUZU_API bool isReadOnly() const;

    std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return parameterMap;
    }

    common::StatementType getStatementType() const;

    void validateExecuteParam(const std::string& paramName, common::Value* param) const;

    KUZU_API ~PreparedStatement();

private:
    bool isProfile() const;

private:
    bool success = true;
    bool readOnly = false;
    bool useInternalCatalogEntry = false;
    std::string errMsg;
    PreparedSummary preparedSummary;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
    std::unique_ptr<binder::BoundStatementResult> statementResult;
    std::unique_ptr<planner::LogicalPlan> logicalPlan;
    std::shared_ptr<parser::Statement> parsedStatement;
};

} // namespace main
} // namespace kuzu

#include <utility>


namespace kuzu {
namespace common {

class NodeVal;
class RelVal;
struct FileInfo;
class NestedVal;
class RecursiveRelVal;
class ArrowRowBatch;
class ValueVector;
class Serializer;
class Deserializer;

class Value {
    friend class NodeVal;
    friend class RelVal;
    friend class NestedVal;
    friend class RecursiveRelVal;
    friend class ArrowRowBatch;
    friend class ValueVector;

public:
    /**
     * @return a NULL value of ANY type.
     */
    KUZU_API static Value createNullValue();
    /**
     * @param dataType the type of the NULL value.
     * @return a NULL value of the given type.
     */
    KUZU_API static Value createNullValue(const LogicalType& dataType);
    /**
     * @param dataType the type of the non-NULL value.
     * @return a default non-NULL value of the given type.
     */
    KUZU_API static Value createDefaultValue(const LogicalType& dataType);
    /**
     * @param val_ the boolean value to set.
     */
    KUZU_API explicit Value(bool val_);
    /**
     * @param val_ the int8_t value to set.
     */
    KUZU_API explicit Value(int8_t val_);
    /**
     * @param val_ the int16_t value to set.
     */
    KUZU_API explicit Value(int16_t val_);
    /**
     * @param val_ the int32_t value to set.
     */
    KUZU_API explicit Value(int32_t val_);
    /**
     * @param val_ the int64_t value to set.
     */
    KUZU_API explicit Value(int64_t val_);
    /**
     * @param val_ the uint8_t value to set.
     */
    KUZU_API explicit Value(uint8_t val_);
    /**
     * @param val_ the uint16_t value to set.
     */
    KUZU_API explicit Value(uint16_t val_);
    /**
     * @param val_ the uint32_t value to set.
     */
    KUZU_API explicit Value(uint32_t val_);
    /**
     * @param val_ the uint64_t value to set.
     */
    KUZU_API explicit Value(uint64_t val_);
    /**
     * @param val_ the int128_t value to set.
     */
    KUZU_API explicit Value(int128_t val_);
    /**
     * @param val_ the UUID value to set.
     */
    KUZU_API explicit Value(ku_uuid_t val_);
    /**
     * @param val_ the double value to set.
     */
    KUZU_API explicit Value(double val_);
    /**
     * @param val_ the float value to set.
     */
    KUZU_API explicit Value(float val_);
    /**
     * @param val_ the date value to set.
     */
    KUZU_API explicit Value(date_t val_);
    /**
     * @param val_ the timestamp_ns value to set.
     */
    KUZU_API explicit Value(timestamp_ns_t val_);
    /**
     * @param val_ the timestamp_ms value to set.
     */
    KUZU_API explicit Value(timestamp_ms_t val_);
    /**
     * @param val_ the timestamp_sec value to set.
     */
    KUZU_API explicit Value(timestamp_sec_t val_);
    /**
     * @param val_ the timestamp_tz value to set.
     */
    KUZU_API explicit Value(timestamp_tz_t val_);
    /**
     * @param val_ the timestamp value to set.
     */
    KUZU_API explicit Value(timestamp_t val_);
    /**
     * @param val_ the interval value to set.
     */
    KUZU_API explicit Value(interval_t val_);
    /**
     * @param val_ the internalID value to set.
     */
    KUZU_API explicit Value(internalID_t val_);
    /**
     * @param val_ the string value to set.
     */
    KUZU_API explicit Value(const char* val_);
    /**
     * @param val_ the string value to set.
     */
    KUZU_API explicit Value(const std::string& val_);
    /**
     * @param val_ the uint8_t* value to set.
     */
    KUZU_API explicit Value(uint8_t* val_);
    /**
     * @param type the logical type of the value.
     * @param val_ the string value to set.
     */
    KUZU_API explicit Value(LogicalType type, std::string val_);
    /**
     * @param dataType the logical type of the value.
     * @param children a vector of children values.
     */
    KUZU_API explicit Value(LogicalType dataType, std::vector<std::unique_ptr<Value>> children);
    /**
     * @param other the value to copy from.
     */
    KUZU_API Value(const Value& other);

    /**
     * @param other the value to move from.
     */
    KUZU_API Value(Value&& other) = default;
    KUZU_API Value& operator=(Value&& other) = default;
    KUZU_API bool operator==(const Value& rhs) const;

    /**
     * @brief Sets the data type of the Value.
     * @param dataType_ the data type to set to.
     */
    KUZU_API void setDataType(const LogicalType& dataType_);
    /**
     * @return the dataType of the value.
     */
    KUZU_API const LogicalType& getDataType() const;
    /**
     * @brief Sets the null flag of the Value.
     * @param flag null value flag to set.
     */
    KUZU_API void setNull(bool flag);
    /**
     * @brief Sets the null flag of the Value to true.
     */
    KUZU_API void setNull();
    /**
     * @return whether the Value is null or not.
     */
    KUZU_API bool isNull() const;
    /**
     * @brief Copies from the row layout value.
     * @param value value to copy from.
     */
    KUZU_API void copyFromRowLayout(const uint8_t* value);
    /**
     * @brief Copies from the col layout value.
     * @param value value to copy from.
     */
    KUZU_API void copyFromColLayout(const uint8_t* value, ValueVector* vec = nullptr);
    /**
     * @brief Copies from the other.
     * @param other value to copy from.
     */
    KUZU_API void copyValueFrom(const Value& other);
    /**
     * @return the value of the given type.
     */
    template<class T>
    T getValue() const {
        throw std::runtime_error("Unimplemented template for Value::getValue()");
    }
    /**
     * @return a reference to the value of the given type.
     */
    template<class T>
    T& getValueReference() {
        throw std::runtime_error("Unimplemented template for Value::getValueReference()");
    }
    /**
     * @return a Value object based on value.
     */
    template<class T>
    static Value createValue(T /*value*/) {
        throw std::runtime_error("Unimplemented template for Value::createValue()");
    }

    /**
     * @return a copy of the current value.
     */
    KUZU_API std::unique_ptr<Value> copy() const;
    /**
     * @return the current value in string format.
     */
    KUZU_API std::string toString() const;

    KUZU_API void serialize(Serializer& serializer) const;

    KUZU_API static std::unique_ptr<Value> deserialize(Deserializer& deserializer);

    KUZU_API void validateType(common::LogicalTypeID targetTypeID) const;

    bool hasNoneNullChildren() const;
    bool allowTypeChange() const;

    uint64_t computeHash() const;

    KUZU_API uint32_t getChildrenSize() const { return childrenSize; }

private:
    Value();
    explicit Value(const LogicalType& dataType);

    void resizeChildrenVector(uint64_t size, const LogicalType& childType);
    void copyFromRowLayoutList(const ku_list_t& list, const LogicalType& childType);
    void copyFromColLayoutList(const list_entry_t& list, ValueVector* vec);
    void copyFromRowLayoutStruct(const uint8_t* kuStruct);
    void copyFromColLayoutStruct(const struct_entry_t& structEntry, ValueVector* vec);
    void copyFromUnion(const uint8_t* kuUnion);

    std::string mapToString() const;
    std::string listToString() const;
    std::string structToString() const;
    std::string nodeToString() const;
    std::string relToString() const;
    std::string decimalToString() const;

public:
    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int128_t int128Val;
        int64_t int64Val;
        int32_t int32Val;
        int16_t int16Val;
        int8_t int8Val;
        uint64_t uint64Val;
        uint32_t uint32Val;
        uint16_t uint16Val;
        uint8_t uint8Val;
        double doubleVal;
        float floatVal;
        // TODO(Ziyi): Should we remove the val suffix from all values in Val? Looks redundant.
        uint8_t* pointer;
        interval_t intervalVal;
        internalID_t internalIDVal;
    } val;
    std::string strVal;

private:
    LogicalType dataType;
    bool isNull_;

    // Note: ALWAYS use childrenSize over children.size(). We do NOT resize children when
    // iterating with nested value. So children.size() reflects the capacity() rather the actual
    // size.
    std::vector<std::unique_ptr<Value>> children;
    uint32_t childrenSize;
};

/**
 * @return boolean value.
 */
template<>
KUZU_API inline bool Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return int8 value.
 */
template<>
KUZU_API inline int8_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT8);
    return val.int8Val;
}

/**
 * @return int16 value.
 */
template<>
KUZU_API inline int16_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return int32 value.
 */
template<>
KUZU_API inline int32_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return int64 value.
 */
template<>
KUZU_API inline int64_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return uint64 value.
 */
template<>
KUZU_API inline uint64_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT64);
    return val.uint64Val;
}

/**
 * @return uint32 value.
 */
template<>
KUZU_API inline uint32_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT32);
    return val.uint32Val;
}

/**
 * @return uint16 value.
 */
template<>
KUZU_API inline uint16_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT16);
    return val.uint16Val;
}

/**
 * @return uint8 value.
 */
template<>
KUZU_API inline uint8_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT8);
    return val.uint8Val;
}

/**
 * @return int128 value.
 */
template<>
KUZU_API inline int128_t Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT128);
    return val.int128Val;
}

/**
 * @return float value.
 */
template<>
KUZU_API inline float Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return double value.
 */
template<>
KUZU_API inline double Value::getValue() const {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return date_t value.
 */
template<>
KUZU_API inline date_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return date_t{val.int32Val};
}

/**
 * @return timestamp_t value.
 */
template<>
KUZU_API inline timestamp_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return timestamp_t{val.int64Val};
}

/**
 * @return timestamp_ns_t value.
 */
template<>
KUZU_API inline timestamp_ns_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_NS);
    return timestamp_ns_t{val.int64Val};
}

/**
 * @return timestamp_ms_t value.
 */
template<>
KUZU_API inline timestamp_ms_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_MS);
    return timestamp_ms_t{val.int64Val};
}

/**
 * @return timestamp_sec_t value.
 */
template<>
KUZU_API inline timestamp_sec_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_SEC);
    return timestamp_sec_t{val.int64Val};
}

/**
 * @return timestamp_tz_t value.
 */
template<>
KUZU_API inline timestamp_tz_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_TZ);
    return timestamp_tz_t{val.int64Val};
}

/**
 * @return interval_t value.
 */
template<>
KUZU_API inline interval_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return internal_t value.
 */
template<>
KUZU_API inline internalID_t Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return string value.
 */
template<>
KUZU_API inline std::string Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::STRING ||
              dataType.getLogicalTypeID() == LogicalTypeID::BLOB ||
              dataType.getLogicalTypeID() == LogicalTypeID::UUID);
    return strVal;
}

/**
 * @return uint8_t* value.
 */
template<>
KUZU_API inline uint8_t* Value::getValue() const {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::POINTER);
    return val.pointer;
}

/**
 * @return the reference to the boolean value.
 */
template<>
KUZU_API inline bool& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return the reference to the int8 value.
 */
template<>
KUZU_API inline int8_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT8);
    return val.int8Val;
}

/**
 * @return the reference to the int16 value.
 */
template<>
KUZU_API inline int16_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return the reference to the int32 value.
 */
template<>
KUZU_API inline int32_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return the reference to the int64 value.
 */
template<>
KUZU_API inline int64_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return the reference to the uint8 value.
 */
template<>
KUZU_API inline uint8_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT8);
    return val.uint8Val;
}

/**
 * @return the reference to the uint16 value.
 */
template<>
KUZU_API inline uint16_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT16);
    return val.uint16Val;
}

/**
 * @return the reference to the uint32 value.
 */
template<>
KUZU_API inline uint32_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT32);
    return val.uint32Val;
}

/**
 * @return the reference to the uint64 value.
 */
template<>
KUZU_API inline uint64_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::UINT64);
    return val.uint64Val;
}

/**
 * @return the reference to the int128 value.
 */
template<>
KUZU_API inline int128_t& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::INT128);
    return val.int128Val;
}

/**
 * @return the reference to the float value.
 */
template<>
KUZU_API inline float& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return the reference to the double value.
 */
template<>
KUZU_API inline double& Value::getValueReference() {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return the reference to the date value.
 */
template<>
KUZU_API inline date_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return *reinterpret_cast<date_t*>(&val.int32Val);
}

/**
 * @return the reference to the timestamp value.
 */
template<>
KUZU_API inline timestamp_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return *reinterpret_cast<timestamp_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_ms value.
 */
template<>
KUZU_API inline timestamp_ms_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_MS);
    return *reinterpret_cast<timestamp_ms_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_ns value.
 */
template<>
KUZU_API inline timestamp_ns_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_NS);
    return *reinterpret_cast<timestamp_ns_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_sec value.
 */
template<>
KUZU_API inline timestamp_sec_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_SEC);
    return *reinterpret_cast<timestamp_sec_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_tz value.
 */
template<>
KUZU_API inline timestamp_tz_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP_TZ);
    return *reinterpret_cast<timestamp_tz_t*>(&val.int64Val);
}

/**
 * @return the reference to the interval value.
 */
template<>
KUZU_API inline interval_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return the reference to the internal_id value.
 */
template<>
KUZU_API inline nodeID_t& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return the reference to the string value.
 */
template<>
KUZU_API inline std::string& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return the reference to the uint8_t* value.
 */
template<>
KUZU_API inline uint8_t*& Value::getValueReference() {
    KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::POINTER);
    return val.pointer;
}

/**
 * @param val the boolean value
 * @return a Value with BOOL type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(bool val) {
    return Value(val);
}

template<>
KUZU_API inline Value Value::createValue(int8_t val) {
    return Value(val);
}

/**
 * @param val the int16 value
 * @return a Value with INT16 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(int16_t val) {
    return Value(val);
}

/**
 * @param val the int32 value
 * @return a Value with INT32 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(int32_t val) {
    return Value(val);
}

/**
 * @param val the int64 value
 * @return a Value with INT64 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(int64_t val) {
    return Value(val);
}

/**
 * @param val the uint8 value
 * @return a Value with UINT8 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(uint8_t val) {
    return Value(val);
}

/**
 * @param val the uint16 value
 * @return a Value with UINT16 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(uint16_t val) {
    return Value(val);
}

/**
 * @param val the uint32 value
 * @return a Value with UINT32 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(uint32_t val) {
    return Value(val);
}

/**
 * @param val the uint64 value
 * @return a Value with UINT64 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(uint64_t val) {
    return Value(val);
}

/**
 * @param val the int128_t value
 * @return a Value with INT128 type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(int128_t val) {
    return Value(val);
}

/**
 * @param val the double value
 * @return a Value with DOUBLE type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(double val) {
    return Value(val);
}

/**
 * @param val the date_t value
 * @return a Value with DATE type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(date_t val) {
    return Value(val);
}

/**
 * @param val the timestamp_t value
 * @return a Value with TIMESTAMP type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(timestamp_t val) {
    return Value(val);
}

/**
 * @param val the interval_t value
 * @return a Value with INTERVAL type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(interval_t val) {
    return Value(val);
}

/**
 * @param val the nodeID_t value
 * @return a Value with NODE_ID type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(nodeID_t val) {
    return Value(val);
}

/**
 * @param val the string value
 * @return a Value with type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(std::string val) {
    return Value(LogicalType::STRING(), std::move(val));
}

/**
 * @param value the string value
 * @return a Value with STRING type and val value.
 */
template<>
KUZU_API inline Value Value::createValue(const char* value) {
    return Value(LogicalType::STRING(), std::string(value));
}

/**
 * @param val the uint8_t* val
 * @return a Value with POINTER type and val val.
 */
template<>
KUZU_API inline Value Value::createValue(uint8_t* val) {
    return Value(val);
}

} // namespace common
} // namespace kuzu


namespace kuzu {

namespace main {
class ClientContext;
}

namespace function {

struct KUZU_API FunctionBindData {
    std::vector<common::LogicalType> paramTypes;
    common::LogicalType resultType;
    // TODO: the following two fields should be moved to FunctionLocalState.
    main::ClientContext* clientContext;
    int64_t count;

    explicit FunctionBindData(common::LogicalType dataType)
        : resultType{std::move(dataType)}, clientContext{nullptr}, count{1} {}
    FunctionBindData(std::vector<common::LogicalType> paramTypes, common::LogicalType resultType)
        : paramTypes{std::move(paramTypes)}, resultType{std::move(resultType)},
          clientContext{nullptr}, count{1} {}
    DELETE_COPY_AND_MOVE(FunctionBindData);
    virtual ~FunctionBindData() = default;

    static std::unique_ptr<FunctionBindData> getSimpleBindData(
        const binder::expression_vector& params, const common::LogicalType& resultType);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

    virtual std::unique_ptr<FunctionBindData> copy() const {
        return std::make_unique<FunctionBindData>(common::LogicalType::copy(paramTypes),
            resultType.copy());
    }
};

struct Function;
using function_set = std::vector<std::unique_ptr<Function>>;

struct ScalarBindFuncInput {
    const binder::expression_vector& arguments;
    Function* definition;
    main::ClientContext* context;
    std::vector<std::string> optionalArguments;

    ScalarBindFuncInput(const binder::expression_vector& arguments, Function* definition,
        main::ClientContext* context, std::vector<std::string> optionalArguments)
        : arguments{arguments}, definition{definition}, context{context},
          optionalArguments{std::move(optionalArguments)} {}
};

using scalar_bind_func =
    std::function<std::unique_ptr<FunctionBindData>(const ScalarBindFuncInput& bindInput)>;

struct KUZU_API Function {
    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    // For variable length function. A
    bool isVarLength = false;
    bool isListLambda = false;
    bool isReadOnly = true;

    Function() : isVarLength{false}, isListLambda{false}, isReadOnly{true} {};
    Function(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs)
        : name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)}, isVarLength{false},
          isListLambda{false} {}
    Function(const Function&) = default;

    virtual ~Function() = default;

    virtual std::string signatureToString() const {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct ScalarOrAggregateFunction : Function {
    common::LogicalTypeID returnTypeID = common::LogicalTypeID::ANY;
    scalar_bind_func bindFunc = nullptr;

    ScalarOrAggregateFunction() : Function{} {}
    ScalarOrAggregateFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID)
        : Function{std::move(name), std::move(parameterTypeIDs)}, returnTypeID{returnTypeID} {}
    ScalarOrAggregateFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_bind_func bindFunc)
        : Function{std::move(name), std::move(parameterTypeIDs)}, returnTypeID{returnTypeID},
          bindFunc{std::move(bindFunc)} {}

    std::string signatureToString() const override {
        auto result = Function::signatureToString();
        result += " -> " + common::LogicalTypeUtils::toString(returnTypeID);
        return result;
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace common {

// F stands for Factorization
enum class FStateType : uint8_t {
    FLAT = 0,
    UNFLAT = 1,
};

class KUZU_API DataChunkState {
public:
    DataChunkState();
    explicit DataChunkState(sel_t capacity) : fStateType{FStateType::UNFLAT} {
        selVector = std::make_shared<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

    void initOriginalAndSelectedSize(uint64_t size) { selVector->setSelSize(size); }
    bool isFlat() const { return fStateType == FStateType::FLAT; }
    void setToFlat() { fStateType = FStateType::FLAT; }
    void setToUnflat() { fStateType = FStateType::UNFLAT; }

    const SelectionVector& getSelVector() const { return *selVector; }
    sel_t getSelSize() const { return selVector->getSelSize(); }
    SelectionVector& getSelVectorUnsafe() { return *selVector; }
    std::shared_ptr<SelectionVector> getSelVectorShared() { return selVector; }
    void setSelVector(std::shared_ptr<SelectionVector> selVector_) {
        this->selVector = std::move(selVector_);
    }

private:
    std::shared_ptr<SelectionVector> selVector;
    // TODO: We should get rid of `fStateType` and merge DataChunkState with SelectionVector.
    FStateType fStateType;
};

} // namespace common
} // namespace kuzu

#include <mutex>


namespace kuzu {

namespace main {
class ClientContext;
}

namespace transaction {

/**
 * If the connection is in AUTO_COMMIT mode, any query over the connection will be wrapped around
 * a transaction and committed (even if the query is READ_ONLY).
 * If the connection is in MANUAL transaction mode, which happens only if an application
 * manually begins a transaction (see below), then an application has to manually commit or
 * rollback the transaction by calling commit() or rollback().
 *
 * AUTO_COMMIT is the default mode when a Connection is created. If an application calls
 * begin[ReadOnly/Write]Transaction at any point, the mode switches to MANUAL. This creates
 * an "active transaction" in the connection. When a connection is in MANUAL mode and the
 * active transaction is rolled back or committed, then the active transaction is removed (so
 * the connection no longer has an active transaction), and the mode automatically switches
 * back to AUTO_COMMIT.
 * Note: When a Connection object is deconstructed, if the connection has an active (manual)
 * transaction, then the active transaction is rolled back.
 */
enum class TransactionMode : uint8_t { AUTO = 0, MANUAL = 1 };

class KUZU_API TransactionContext {
public:
    explicit TransactionContext(main::ClientContext& clientContext);
    ~TransactionContext();

    bool isAutoTransaction() const { return mode == TransactionMode::AUTO; }

    void beginReadTransaction();
    void beginWriteTransaction();
    void beginAutoTransaction(bool readOnlyStatement);
    void beginRecoveryTransaction();
    void validateManualTransaction(bool readOnlyStatement) const;

    void commit();
    void rollback();

    TransactionMode getTransactionMode() const { return mode; }
    bool hasActiveTransaction() const { return activeTransaction != nullptr; }
    Transaction* getActiveTransaction() const { return activeTransaction; }

    void clearTransaction();

private:
    void beginTransactionInternal(TransactionType transactionType);

private:
    std::mutex mtx;
    main::ClientContext& clientContext;
    TransactionMode mode;
    Transaction* activeTransaction;
};

} // namespace transaction
} // namespace kuzu

#include <string>
#include <vector>


namespace kuzu {
namespace common {

enum class FileType : uint8_t {
    UNKNOWN = 0,
    CSV = 1,
    PARQUET = 2,
    NPY = 3,
};

struct FileTypeInfo {
    FileType fileType = FileType::UNKNOWN;
    std::string fileTypeStr;
};

struct FileTypeUtils {
    static FileType getFileTypeFromExtension(std::string_view extension);
    static std::string toString(FileType fileType);
    static FileType fromString(std::string fileType);
};

struct FileScanInfo {
    static constexpr const char* FILE_FORMAT_OPTION_NAME = "FILE_FORMAT";

    FileTypeInfo fileTypeInfo;
    std::vector<std::string> filePaths;
    case_insensitive_map_t<Value> options;

    FileScanInfo() : fileTypeInfo{FileType::UNKNOWN, ""} {}
    FileScanInfo(FileTypeInfo fileTypeInfo, std::vector<std::string> filePaths)
        : fileTypeInfo{std::move(fileTypeInfo)}, filePaths{std::move(filePaths)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(FileScanInfo);

    uint32_t getNumFiles() const { return filePaths.size(); }
    std::string getFilePath(idx_t fileIdx) const {
        KU_ASSERT(fileIdx < getNumFiles());
        return filePaths[fileIdx];
    }

    template<typename T>
    T getOption(std::string optionName, T defaultValue) const {
        const auto optionIt = options.find(optionName);
        if (optionIt != options.end()) {
            return optionIt->second.getValue<T>();
        } else {
            return defaultValue;
        }
    }

private:
    FileScanInfo(const FileScanInfo& other)
        : fileTypeInfo{other.fileTypeInfo}, filePaths{other.filePaths}, options{other.options} {}
};

} // namespace common
} // namespace kuzu

#include <string>
#include <variant>
#include <vector>


namespace kuzu {
namespace common {
class ValueVector;
}
namespace storage {
class ColumnChunkData;
}

namespace processor {

template<typename T>
concept DataSource =
    std::same_as<storage::ColumnChunkData, T> || std::same_as<common::ValueVector, T>;

struct KUZU_API WarningSourceData {
    // we should stick to integral types here as each value essentially adds a column to the output
    // when reading from a file
    using DataType = std::variant<uint64_t, uint32_t>;

    static constexpr size_t BLOCK_IDX_IDX = 0;
    static constexpr size_t OFFSET_IN_BLOCK_IDX = 1;
    static constexpr size_t NUM_BLOCK_VALUES = 2;

    WarningSourceData() : WarningSourceData(0) {}
    explicit WarningSourceData(uint64_t numSourceSpecificValues);

    template<std::integral... Types>
    void dumpTo(uint64_t& blockIdx, uint32_t& offsetInBlock, Types&... vars) const;

    template<std::integral... Types>
    static WarningSourceData constructFrom(uint64_t blockIdx, uint32_t offsetInBlock,
        Types... newValues);

    uint64_t getBlockIdx() const;
    uint32_t getOffsetInBlock() const;

    template<DataSource T>
    static WarningSourceData constructFromData(const std::vector<T*>& chunks, common::idx_t pos);

    std::array<DataType, common::CopyConstants::MAX_NUM_WARNING_DATA_COLUMNS> values;
    uint64_t numValues;
};

struct LineContext {
    uint64_t startByteOffset;
    uint64_t endByteOffset;

    bool isCompleteLine;

    void setNewLine(uint64_t start);
    void setEndOfLine(uint64_t end);
};

// If parsing in parallel during parsing we may not be able to determine line numbers
// Thus we have additional fields that can be used to determine line numbers + reconstruct lines
// After parsing this will be used to populate a PopulatedCopyFromError instance
struct KUZU_API CopyFromFileError {
    CopyFromFileError(std::string message, WarningSourceData warningData, bool completedLine = true,
        bool mustThrow = false);

    std::string message;
    bool completedLine;
    WarningSourceData warningData;

    bool mustThrow;

    bool operator<(const CopyFromFileError& o) const;
};

struct PopulatedCopyFromError {
    std::string message;
    std::string filePath;
    std::string skippedLineOrRecord;
    uint64_t lineNumber;
};

template<std::integral... Types>
void WarningSourceData::dumpTo(uint64_t& blockIdx, uint32_t& offsetInBlock, Types&... vars) const {
    static_assert(sizeof...(Types) + NUM_BLOCK_VALUES <= std::tuple_size_v<decltype(values)>);
    KU_ASSERT(sizeof...(Types) + NUM_BLOCK_VALUES == numValues);
    common::TypeUtils::paramPackForEach(
        [this](auto idx, auto& value) {
            value = std::get<std::decay_t<decltype(value)>>(values[idx]);
        },
        blockIdx, offsetInBlock, vars...);
}

template<std::integral... Types>
WarningSourceData WarningSourceData::constructFrom(uint64_t blockIdx, uint32_t offsetInBlock,
    Types... newValues) {
    static_assert(sizeof...(Types) + NUM_BLOCK_VALUES <= std::tuple_size_v<decltype(values)>,
        "For performance reasons the number of warning metadata columns has a "
        "statically-defined limit, modify "
        "'common::CopyConstants::WARNING_DATA_MAX_NUM_COLUMNS' if you wish to increase it.");

    WarningSourceData ret{sizeof...(Types) + NUM_BLOCK_VALUES};
    common::TypeUtils::paramPackForEach([&ret](auto idx, auto value) { ret.values[idx] = value; },
        blockIdx, offsetInBlock, newValues...);
    return ret;
}

} // namespace processor
} // namespace kuzu

#include <cstdint>
#include <memory>
#include <string>
#include <vector>


namespace kuzu {
namespace processor {

/**
 * @brief Stores a vector of Values.
 */
class FlatTuple {
public:
    void addValue(std::unique_ptr<common::Value> value);

    /**
     * @return number of values in the FlatTuple.
     */
    KUZU_API uint32_t len() const;

    /**
     * @param idx value index to get.
     * @return the value stored at idx.
     */
    KUZU_API common::Value* getValue(uint32_t idx) const;

    KUZU_API std::string toString();

    /**
     * @param colsWidth The length of each column
     * @param delimiter The delimiter to separate each value.
     * @param maxWidth The maximum length of each column. Only the first maxWidth number of
     * characters of each column will be displayed.
     * @return all values in string format.
     */
    KUZU_API std::string toString(const std::vector<uint32_t>& colsWidth,
        const std::string& delimiter = "|", uint32_t maxWidth = -1);

private:
    std::vector<std::unique_ptr<common::Value>> values;
};

} // namespace processor
} // namespace kuzu

#include <string>


namespace kuzu {
namespace common {
class Value;
enum class LogicalTypeID : uint8_t;
} // namespace common

namespace main {

class ClientContext;
struct SystemConfig;

typedef void (*set_context)(ClientContext* context, const common::Value& parameter);
typedef common::Value (*get_setting)(const ClientContext* context);

enum class OptionType : uint8_t { CONFIGURATION = 0, EXTENSION = 1 };

struct Option {
    std::string name;
    common::LogicalTypeID parameterType;
    OptionType optionType;
    bool isConfidential;

    Option(std::string name, common::LogicalTypeID parameterType, OptionType optionType,
        bool isConfidential)
        : name{std::move(name)}, parameterType{parameterType}, optionType{optionType},
          isConfidential{isConfidential} {}

    virtual ~Option() = default;
};

struct ConfigurationOption final : Option {
    set_context setContext;
    get_setting getSetting;

    ConfigurationOption(std::string name, common::LogicalTypeID parameterType,
        set_context setContext, get_setting getSetting)
        : Option{std::move(name), parameterType, OptionType::CONFIGURATION,
              false /* isConfidential */},
          setContext{setContext}, getSetting{getSetting} {}
};

struct ExtensionOption final : Option {
    common::Value defaultValue;

    ExtensionOption(std::string name, common::LogicalTypeID parameterType,
        common::Value defaultValue, bool isConfidential)
        : Option{std::move(name), parameterType, OptionType::EXTENSION, isConfidential},
          defaultValue{std::move(defaultValue)} {}
};

struct DBConfig {
    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    bool readOnly;
    uint64_t maxDBSize;
    bool enableMultiWrites;
    bool autoCheckpoint;
    uint64_t checkpointThreshold;
    bool forceCheckpointOnClose;
    bool enableSpillingToDisk;
#if defined(__APPLE__)
    uint32_t threadQos;
#endif

    explicit DBConfig(const SystemConfig& systemConfig);

    static ConfigurationOption* getOptionByName(const std::string& optionName);
    KUZU_API static bool isDBPathInMemory(const std::string& dbPath);
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace common {

struct CSVOption {
    // TODO(Xiyang): Add newline character option and delimiter can be a string.
    char escapeChar;
    char delimiter;
    char quoteChar;
    bool hasHeader;
    uint64_t skipNum;
    uint64_t sampleSize;
    bool allowUnbracedList;
    bool ignoreErrors;

    bool autoDetection;
    // These fields aim to identify whether the options are set by user, or set by default.
    bool setEscape;
    bool setDelim;
    bool setQuote;
    bool setHeader;
    std::vector<std::string> nullStrings;

    CSVOption()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER},
          skipNum{CopyConstants::DEFAULT_CSV_SKIP_NUM},
          sampleSize{CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE},
          allowUnbracedList{CopyConstants::DEFAULT_CSV_ALLOW_UNBRACED_LIST},
          ignoreErrors(CopyConstants::DEFAULT_IGNORE_ERRORS),
          autoDetection{CopyConstants::DEFAULT_CSV_AUTO_DETECT},
          setEscape{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          setDelim{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          setQuote{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          setHeader{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          nullStrings{CopyConstants::DEFAULT_CSV_NULL_STRINGS[0]} {}

    EXPLICIT_COPY_DEFAULT_MOVE(CSVOption);

    // TODO: COPY FROM and COPY TO should support transform special options, like '\'.
    std::unordered_map<std::string, std::string> toOptionsMap(const bool& parallel) const {
        std::unordered_map<std::string, std::string> result;
        result["parallel"] = parallel ? "true" : "false";
        if (setHeader) {
            result["header"] = hasHeader ? "true" : "false";
        }
        if (setEscape) {
            result["escape"] = stringFormat("escape='\\{}'", escapeChar);
        }
        if (setDelim) {
            result["delim"] = stringFormat("delim='{}'", delimiter);
        }
        if (setQuote) {
            result["quote"] = stringFormat("quote='\\{}'", quoteChar);
        }
        if (autoDetection != CopyConstants::DEFAULT_CSV_AUTO_DETECT) {
            result["auto_detect"] = autoDetection ? "true" : "false";
        }
        return result;
    }

    static std::string toCypher(const std::unordered_map<std::string, std::string>& options) {
        if (options.empty()) {
            return "";
        }
        std::string result = "";
        for (const auto& [key, value] : options) {
            if (!result.empty()) {
                result += ", ";
            }
            result += key + "=" + value;
        }
        return "(" + result + ")";
    }

    // Explicit copy constructor
    CSVOption(const CSVOption& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          hasHeader{other.hasHeader}, skipNum{other.skipNum},
          sampleSize{other.sampleSize == 0 ?
                         CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE :
                         other.sampleSize}, // Set to DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE if
                                            // sampleSize is 0
          allowUnbracedList{other.allowUnbracedList}, ignoreErrors{other.ignoreErrors},
          autoDetection{other.autoDetection}, setEscape{other.setEscape}, setDelim{other.setDelim},
          setQuote{other.setQuote}, setHeader{other.setHeader}, nullStrings{other.nullStrings} {}
};

struct CSVReaderConfig {
    CSVOption option;
    bool parallel;

    CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig);

    static CSVReaderConfig construct(const case_insensitive_map_t<Value>& options);

private:
    CSVReaderConfig(const CSVReaderConfig& other)
        : option{other.option.copy()}, parallel{other.parallel} {}
};

} // namespace common
} // namespace kuzu

#include <optional>
#include <utility>


namespace kuzu {
namespace common {

class Value;

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class KUZU_API ValueVector {
    friend class ListVector;
    friend class ListAuxiliaryBuffer;
    friend class StructVector;
    friend class StringVector;
    friend class ArrowColumnVector;

public:
    explicit ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager = nullptr,
        std::shared_ptr<DataChunkState> dataChunkState = nullptr);
    explicit ValueVector(LogicalTypeID dataTypeID, storage::MemoryManager* memoryManager = nullptr)
        : ValueVector(LogicalType(dataTypeID), memoryManager) {
        KU_ASSERT(dataTypeID != LogicalTypeID::LIST);
    }

    DELETE_COPY_AND_MOVE(ValueVector);
    ~ValueVector() = default;

    template<typename T>
    std::optional<T> firstNonNull() const {
        sel_t selectedSize = state->getSelSize();
        if (selectedSize == 0) {
            return std::nullopt;
        }
        if (hasNoNullsGuarantee()) {
            return getValue<T>(state->getSelVector()[0]);
        } else {
            for (size_t i = 0; i < selectedSize; i++) {
                auto pos = state->getSelVector()[i];
                if (!isNull(pos)) {
                    return std::make_optional(getValue<T>(pos));
                }
            }
        }
        return std::nullopt;
    }

    template<class Func>
    void forEachNonNull(Func&& func) const {
        if (hasNoNullsGuarantee()) {
            state->getSelVector().forEach(func);
        } else {
            state->getSelVector().forEach([&](auto i) {
                if (!isNull(i)) {
                    func(i);
                }
            });
        }
    }

    uint32_t countNonNull() const;

    void setState(const std::shared_ptr<DataChunkState>& state_);

    void setAllNull() { nullMask.setAllNull(); }
    void setAllNonNull() { nullMask.setAllNonNull(); }
    // On return true, there are no null. On return false, there may or may not be nulls.
    bool hasNoNullsGuarantee() const { return nullMask.hasNoNullsGuarantee(); }
    void setNullRange(uint32_t startPos, uint32_t len, bool value) {
        nullMask.setNullFromRange(startPos, len, value);
    }
    const NullMask& getNullMask() const { return nullMask; }
    void setNull(uint32_t pos, bool isNull);
    uint8_t isNull(uint32_t pos) const { return nullMask.isNull(pos); }
    void setAsSingleNullEntry() {
        state->getSelVectorUnsafe().setSelSize(1);
        setNull(state->getSelVector()[0], true);
    }

    bool setNullFromBits(const uint64_t* srcNullEntries, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy, bool invert = false);

    uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

    // TODO(Guodong): Rename this to getValueRef
    template<typename T>
    const T& getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    T& getValue(uint32_t pos) {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    void setValue(uint32_t pos, T val);
    // copyFromRowData assumes rowData is non-NULL.
    void copyFromRowData(uint32_t pos, const uint8_t* rowData);
    // copyToRowData assumes srcVectorData is non-NULL.
    void copyToRowData(uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer) const;
    // copyFromVectorData assumes srcVectorData is non-NULL.
    void copyFromVectorData(uint8_t* dstData, const ValueVector* srcVector,
        const uint8_t* srcVectorData);
    void copyFromVectorData(uint64_t dstPos, const ValueVector* srcVector, uint64_t srcPos);
    void copyFromValue(uint64_t pos, const Value& value);

    std::unique_ptr<Value> getAsValue(uint64_t pos) const;

    uint8_t* getData() const { return valueBuffer.get(); }

    offset_t readNodeOffset(uint32_t pos) const {
        KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
        return getValue<nodeID_t>(pos).offset;
    }

    void resetAuxiliaryBuffer();

    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);

    void serialize(Serializer& ser) const;
    static std::unique_ptr<ValueVector> deSerialize(Deserializer& deSer, storage::MemoryManager* mm,
        std::shared_ptr<DataChunkState> dataChunkState);

    SelectionVector* getSelVectorPtr() const {
        return state ? &state->getSelVectorUnsafe() : nullptr;
    }

private:
    uint32_t getDataTypeSize(const LogicalType& type);
    void initializeValueBuffer();

public:
    LogicalType dataType;
    std::shared_ptr<DataChunkState> state;

private:
    std::unique_ptr<uint8_t[]> valueBuffer;
    NullMask nullMask;
    uint32_t numBytesPerValue;
    std::unique_ptr<AuxiliaryBuffer> auxiliaryBuffer;
};

class KUZU_API StringVector {
public:
    static inline InMemOverflowBuffer* getInMemOverflowBuffer(ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
        return ku_dynamic_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getOverflowBuffer();
    }

    static void addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr);
    static void addString(ValueVector* vector, uint32_t vectorPos, const char* srcStr,
        uint64_t length);
    static void addString(ValueVector* vector, uint32_t vectorPos, std::string_view srcStr);
    // Add empty string with space reserved for the provided size
    // Returned value can be modified to set the string contents
    static ku_string_t& reserveString(ValueVector* vector, uint32_t vectorPos, uint64_t length);
    static void reserveString(ValueVector* vector, ku_string_t& dstStr, uint64_t length);
    static void addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr);
    static void addString(ValueVector* vector, ku_string_t& dstStr, const char* srcStr,
        uint64_t length);
    static void addString(kuzu::common::ValueVector* vector, ku_string_t& dstStr,
        const std::string& srcStr);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
};

struct KUZU_API BlobVector {
    static void addBlob(ValueVector* vector, uint32_t pos, const char* data, uint32_t length) {
        StringVector::addString(vector, pos, data, length);
    } // namespace common
    static void addBlob(ValueVector* vector, uint32_t pos, const uint8_t* data, uint64_t length) {
        StringVector::addString(vector, pos, reinterpret_cast<const char*>(data), length);
    }
}; // namespace kuzu

// ListVector is used for both LIST and ARRAY physical type
class KUZU_API ListVector {
public:
    static const ListAuxiliaryBuffer& getAuxBuffer(const ValueVector& vector) {
        return vector.auxiliaryBuffer->constCast<ListAuxiliaryBuffer>();
    }
    static ListAuxiliaryBuffer& getAuxBufferUnsafe(const ValueVector& vector) {
        return vector.auxiliaryBuffer->cast<ListAuxiliaryBuffer>();
    }
    // If you call setDataVector during initialize, there must be a followed up
    // copyListEntryAndBufferMetaData at runtime.
    // TODO(Xiyang): try to merge setDataVector & copyListEntryAndBufferMetaData
    static void setDataVector(const ValueVector* vector, std::shared_ptr<ValueVector> dataVector) {
        KU_ASSERT(validateType(*vector));
        auto& listBuffer = getAuxBufferUnsafe(*vector);
        listBuffer.setDataVector(std::move(dataVector));
    }
    static void copyListEntryAndBufferMetaData(ValueVector& vector,
        const SelectionVector& selVector, const ValueVector& other,
        const SelectionVector& otherSelVector);
    static ValueVector* getDataVector(const ValueVector* vector) {
        KU_ASSERT(validateType(*vector));
        return getAuxBuffer(*vector).getDataVector();
    }
    static std::shared_ptr<ValueVector> getSharedDataVector(const ValueVector* vector) {
        KU_ASSERT(validateType(*vector));
        return getAuxBuffer(*vector).getSharedDataVector();
    }
    static uint64_t getDataVectorSize(const ValueVector* vector) {
        KU_ASSERT(validateType(*vector));
        return getAuxBuffer(*vector).getSize();
    }
    static uint8_t* getListValues(const ValueVector* vector, const list_entry_t& listEntry) {
        KU_ASSERT(validateType(*vector));
        auto dataVector = getDataVector(vector);
        return dataVector->getData() + dataVector->getNumBytesPerValue() * listEntry.offset;
    }
    static uint8_t* getListValuesWithOffset(const ValueVector* vector,
        const list_entry_t& listEntry, offset_t elementOffsetInList) {
        KU_ASSERT(validateType(*vector));
        return getListValues(vector, listEntry) +
               elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
    }
    static list_entry_t addList(ValueVector* vector, uint64_t listSize) {
        KU_ASSERT(validateType(*vector));
        return getAuxBufferUnsafe(*vector).addList(listSize);
    }
    static void resizeDataVector(ValueVector* vector, uint64_t numValues) {
        KU_ASSERT(validateType(*vector));
        getAuxBufferUnsafe(*vector).resize(numValues);
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
    static void appendDataVector(ValueVector* dstVector, ValueVector* srcDataVector,
        uint64_t numValuesToAppend);
    static void sliceDataVector(ValueVector* vectorToSlice, uint64_t offset, uint64_t numValues);

private:
    static bool validateType(const ValueVector& vector) {
        switch (vector.dataType.getPhysicalType()) {
        case PhysicalTypeID::LIST:
        case PhysicalTypeID::ARRAY:
            return true;
        default:
            return false;
        }
    }
};

class StructVector {
public:
    static const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors(
        const ValueVector* vector) {
        return ku_dynamic_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getFieldVectors();
    }

    static std::shared_ptr<ValueVector> getFieldVector(const ValueVector* vector,
        struct_field_idx_t idx) {
        return ku_dynamic_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getFieldVectorShared(idx);
    }

    static ValueVector* getFieldVectorRaw(const ValueVector& vector, const std::string& fieldName) {
        auto idx = StructType::getFieldIdx(vector.dataType, fieldName);
        return ku_dynamic_cast<StructAuxiliaryBuffer*>(vector.auxiliaryBuffer.get())
            ->getFieldVectorPtr(idx);
    }

    static void referenceVector(ValueVector* vector, struct_field_idx_t idx,
        std::shared_ptr<ValueVector> vectorToReference) {
        ku_dynamic_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->referenceChildVector(idx, std::move(vectorToReference));
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, const uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
};

class UnionVector {
public:
    static inline ValueVector* getTagVector(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        return StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX).get();
    }

    static inline ValueVector* getValVector(const ValueVector* vector, union_field_idx_t fieldIdx) {
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        return StructVector::getFieldVector(vector, UnionType::getInternalFieldIdx(fieldIdx)).get();
    }

    static inline void referenceVector(ValueVector* vector, union_field_idx_t fieldIdx,
        std::shared_ptr<ValueVector> vectorToReference) {
        StructVector::referenceVector(vector, UnionType::getInternalFieldIdx(fieldIdx),
            std::move(vectorToReference));
    }

    static inline void setTagField(ValueVector& vector, SelectionVector& sel,
        union_field_idx_t tag) {
        KU_ASSERT(vector.dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        for (auto i = 0u; i < sel.getSelSize(); i++) {
            vector.setValue<struct_field_idx_t>(sel[i], tag);
        }
    }
};

class MapVector {
public:
    static inline ValueVector* getKeyVector(const ValueVector* vector) {
        return StructVector::getFieldVector(ListVector::getDataVector(vector), 0 /* keyVectorPos */)
            .get();
    }

    static inline ValueVector* getValueVector(const ValueVector* vector) {
        return StructVector::getFieldVector(ListVector::getDataVector(vector), 1 /* valVectorPos */)
            .get();
    }

    static inline uint8_t* getMapKeys(const ValueVector* vector, const list_entry_t& listEntry) {
        auto keyVector = getKeyVector(vector);
        return keyVector->getData() + keyVector->getNumBytesPerValue() * listEntry.offset;
    }

    static inline uint8_t* getMapValues(const ValueVector* vector, const list_entry_t& listEntry) {
        auto valueVector = getValueVector(vector);
        return valueVector->getData() + valueVector->getNumBytesPerValue() * listEntry.offset;
    }
};

} // namespace common
} // namespace kuzu

#include <vector>


namespace kuzu {
namespace binder {
class LiteralExpression;
class Binder;
} // namespace binder
namespace main {
class ClientContext;
}

namespace common {
class Value;
}

namespace function {

using optional_params_t = common::case_insensitive_map_t<common::Value>;

struct TableFunction;

struct ExtraTableFuncBindInput {
    virtual ~ExtraTableFuncBindInput() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }
};

struct KUZU_API TableFuncBindInput {
    binder::expression_vector params;
    optional_params_t optionalParams;
    binder::expression_vector optionalParamsLegacy;
    std::unique_ptr<ExtraTableFuncBindInput> extraInput = nullptr;
    binder::Binder* binder = nullptr;
    std::vector<parser::YieldVariable> yieldVariables;

    TableFuncBindInput() = default;

    void addLiteralParam(common::Value value);

    std::shared_ptr<binder::Expression> getParam(common::idx_t idx) const { return params[idx]; }
    common::Value getValue(common::idx_t idx) const;
    template<typename T>
    T getLiteralVal(common::idx_t idx) const;
};

struct KUZU_API ExtraScanTableFuncBindInput : ExtraTableFuncBindInput {
    common::FileScanInfo fileScanInfo;
    std::vector<std::string> expectedColumnNames;
    std::vector<common::LogicalType> expectedColumnTypes;
    TableFunction* tableFunction = nullptr;
};

} // namespace function
} // namespace kuzu

#include <functional>
#include <mutex>
#include <vector>


namespace kuzu {
namespace common {
class ValueVector;
}
namespace storage {
class ColumnChunkData;
}

namespace processor {

class SerialCSVReader;

struct WarningInfo {
    uint64_t queryID;
    PopulatedCopyFromError warning;

    WarningInfo(PopulatedCopyFromError warning, uint64_t queryID)
        : queryID(queryID), warning(std::move(warning)) {}
};

using populate_func_t = std::function<PopulatedCopyFromError(CopyFromFileError, common::idx_t)>;
using get_file_idx_func_t = std::function<common::idx_t(const CopyFromFileError&)>;

class KUZU_API WarningContext {
public:
    explicit WarningContext(main::ClientConfig* clientConfig);

    void appendWarningMessages(const std::vector<CopyFromFileError>& messages);

    void populateWarnings(uint64_t queryID, populate_func_t populateFunc = {},
        get_file_idx_func_t getFileIdxFunc = {});
    void defaultPopulateAllWarnings(uint64_t queryID);

    const std::vector<WarningInfo>& getPopulatedWarnings() const;
    uint64_t getWarningCount(uint64_t queryID);
    void clearPopulatedWarnings();

    void setIgnoreErrorsForCurrentQuery(bool ignoreErrors);
    // NOTE: this function only works if the logical operator is COPY FROM
    // for other operators setIgnoreErrorsForCurrentQuery() is not called
    bool getIgnoreErrorsOption() const;

private:
    std::mutex mtx;
    main::ClientConfig* clientConfig;
    std::vector<CopyFromFileError> unpopulatedWarnings;
    std::vector<WarningInfo> populatedWarnings;
    uint64_t queryWarningCount;
    uint64_t numStoredWarnings;
    bool ignoreErrorsOption;
};

} // namespace processor
} // namespace kuzu

#include <string>

namespace kuzu {
namespace main {

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
    friend class Connection;
    friend class ClientContext;
    class QueryResultIterator {
    private:
        QueryResult* currentResult;

    public:
        QueryResultIterator() = default;

        explicit QueryResultIterator(QueryResult* startResult) : currentResult(startResult) {}

        void operator++() {
            if (currentResult) {
                currentResult = currentResult->nextQueryResult.get();
            }
        }

        bool isEnd() const { return currentResult == nullptr; }

        bool hasNextQueryResult() const { return currentResult->nextQueryResult != nullptr; }

        QueryResult* getCurrentResult() const { return currentResult; }
    };

public:
    /**
     * @brief Used to create a QueryResult object for the failing query.
     */
    KUZU_API QueryResult();

    explicit QueryResult(const PreparedSummary& preparedSummary);
    /**
     * @brief Deconstructs the QueryResult object.
     */
    KUZU_API ~QueryResult();
    /**
     * @return query is executed successfully or not.
     */
    KUZU_API bool isSuccess() const;
    /**
     * @return error message of the query execution if the query fails.
     */
    KUZU_API std::string getErrorMessage() const;
    /**
     * @return number of columns in query result.
     */
    KUZU_API size_t getNumColumns() const;
    /**
     * @return name of each column in query result.
     */
    KUZU_API std::vector<std::string> getColumnNames() const;
    /**
     * @return dataType of each column in query result.
     */
    KUZU_API std::vector<common::LogicalType> getColumnDataTypes() const;
    /**
     * @return num of tuples in query result.
     */
    KUZU_API uint64_t getNumTuples() const;
    /**
     * @return query summary which stores the execution time, compiling time, plan and query
     * options.
     */
    KUZU_API QuerySummary* getQuerySummary() const;
    /**
     * @return whether there are more tuples to read.
     */
    KUZU_API bool hasNext() const;
    /**
     * @return whether there are more query results to read.
     */
    KUZU_API bool hasNextQueryResult() const;
    /**
     * @return get next query result to read (for multiple query statements).
     */
    KUZU_API QueryResult* getNextQueryResult();

    std::unique_ptr<QueryResult> nextQueryResult;
    /**
     * @return next flat tuple in the query result. Note that to reduce resource allocation, all
     * calls to getNext() reuse the same FlatTuple object. Since its contents will be overwritten,
     * please complete processing a FlatTuple or make a copy of its data before calling getNext()
     * again.
     */
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();
    /**
     * @return string of first query result.
     */
    KUZU_API std::string toString() const;

    /**
     * @brief Resets the result tuple iterator.
     */
    KUZU_API void resetIterator();

    processor::FactorizedTable* getTable() { return factorizedTable.get(); }

    /**
     * @brief Returns the arrow schema of the query result.
     * @return datatypes of the columns as an arrow schema
     *
     * It is the caller's responsibility to call the release function to release the underlying data
     * If converting to another arrow type, this this is usually handled automatically.
     */
    KUZU_API std::unique_ptr<ArrowSchema> getArrowSchema() const;

    /**
     * @brief Returns the next chunk of the query result as an arrow array.
     * @param chunkSize number of tuples to return in the chunk.
     * @return An arrow array representation of the next chunkSize tuples of the query result.
     *
     * The ArrowArray internally stores an arrow struct with fields for each of the columns.
     * This can be converted to a RecordBatch with arrow's ImportRecordBatch function
     *
     * It is the caller's responsibility to call the release function to release the underlying data
     * If converting to another arrow type, this this is usually handled automatically.
     */
    KUZU_API std::unique_ptr<ArrowArray> getNextArrowChunk(int64_t chunkSize);

private:
    void setColumnHeader(std::vector<std::string> columnNames,
        std::vector<common::LogicalType> columnTypes);
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_);
    void validateQuerySucceed() const;
    std::pair<std::unique_ptr<processor::FlatTuple>, std::unique_ptr<processor::FlatTupleIterator>>
    getIterator() const;
    void checkDatabaseClosedOrThrow() const;

private:
    // execution status
    bool success = true;
    std::string errMsg;

    // header information
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnDataTypes;
    // data
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::shared_ptr<processor::FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;

    // query iterator
    QueryResultIterator queryResultIterator;

    // database life cycle manager
    std::shared_ptr<common::DatabaseLifeCycleManager> dbLifeCycleManager;
};

} // namespace main
} // namespace kuzu

#include <memory>
#include <mutex>
#include <vector>

#if defined(__APPLE__)
#include <pthread/qos.h>
#endif

namespace kuzu {
namespace common {
class FileSystem;
enum class LogicalTypeID : uint8_t;
} // namespace common

namespace catalog {
class CatalogEntry;
} // namespace catalog

namespace function {
struct Function;
} // namespace function

namespace extension {
struct ExtensionUtils;
class ExtensionManager;
} // namespace extension

namespace storage {
class StorageExtension;
} // namespace storage

namespace main {
struct ExtensionOption;
class DatabaseManager;

/**
 * @brief Stores runtime configuration for creating or opening a Database
 */
struct KUZU_API SystemConfig {
    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Max size of the buffer pool in bytes.
     *        The larger the buffer pool, the more data from the database files is kept in memory,
     *        reducing the amount of File I/O
     * @param maxNumThreads The maximum number of threads to use during query execution
     * @param enableCompression Whether or not to compress data on-disk for supported types
     * @param readOnly If true, the database is opened read-only. No write transaction is
     * allowed on the `Database` object. Multiple read-only `Database` objects can be created with
     * the same database path. If false, the database is opened read-write. Under this mode,
     * there must not be multiple `Database` objects created with the same database path.
     * @param maxDBSize The maximum size of the database in bytes. Note that this is introduced
     * temporarily for now to get around with the default 8TB mmap address space limit some
     * environment. This will be removed once we implemente a better solution later. The value is
     * default to 1 << 43 (8TB) under 64-bit environment and 1GB under 32-bit one (see
     * `DEFAULT_VM_REGION_MAX_SIZE`).
     * @param autoCheckpoint If true, the database will automatically checkpoint when the size of
     * the WAL file exceeds the checkpoint threshold.
     * @param checkpointThreshold The threshold of the WAL file size in bytes. When the size of the
     * WAL file exceeds this threshold, the database will checkpoint if autoCheckpoint is true.
     * @param forceCheckpointOnClose If true, the database will force checkpoint when closing.
     */
    explicit SystemConfig(uint64_t bufferPoolSize = -1u, uint64_t maxNumThreads = 0,
        bool enableCompression = true, bool readOnly = false, uint64_t maxDBSize = -1u,
        bool autoCheckpoint = true, uint64_t checkpointThreshold = 16777216 /* 16MB */,
        bool forceCheckpointOnClose = true
#if defined(__APPLE__)
        ,
        uint32_t threadQos = QOS_CLASS_DEFAULT
#endif
    );

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    bool readOnly;
    uint64_t maxDBSize;
    bool autoCheckpoint;
    uint64_t checkpointThreshold;
    bool forceCheckpointOnClose;
#if defined(__APPLE__)
    uint32_t threadQos;
#endif
};

/**
 * @brief Database class is the main class of Kuzu. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class ClientContext;
    friend class Connection;
    friend class StorageDriver;
    friend class testing::BaseGraphTest;
    friend class testing::PrivateGraphTest;
    friend class transaction::TransactionContext;
    friend struct extension::ExtensionUtils;

public:
    /**
     * @brief Creates a database object.
     * @param databasePath Database path. If left empty, or :memory: is specified, this will create
     *        an in-memory database.
     * @param systemConfig System configurations (buffer pool size and max num threads).
     */
    KUZU_API explicit Database(std::string_view databasePath,
        SystemConfig systemConfig = SystemConfig());
    /**
     * @brief Destructs the database object.
     */
    KUZU_API ~Database();

    KUZU_API void registerFileSystem(std::unique_ptr<common::FileSystem> fs);

    KUZU_API void registerStorageExtension(std::string name,
        std::unique_ptr<storage::StorageExtension> storageExtension);

    KUZU_API void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue, bool isConfidential = false);

    KUZU_API catalog::Catalog* getCatalog() { return catalog.get(); }

    const DBConfig& getConfig() const { return dbConfig; }

    std::vector<storage::StorageExtension*> getStorageExtensions();

    uint64_t getNextQueryID();

private:
    using construct_bm_func_t =
        std::function<std::unique_ptr<storage::BufferManager>(const Database&)>;

    struct QueryIDGenerator {
        uint64_t queryID = 0;
        std::mutex queryIDLock;
    };

    static std::unique_ptr<storage::BufferManager> initBufferManager(const Database& db);
    void initMembers(std::string_view dbPath, construct_bm_func_t initBmFunc = initBufferManager);

    // factory method only to be used for tests
    Database(std::string_view databasePath, SystemConfig systemConfig,
        construct_bm_func_t constructBMFunc);

    void validatePathInReadOnly() const;

private:
    std::string databasePath;
    DBConfig dbConfig;
    std::unique_ptr<common::VirtualFileSystem> vfs;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<common::FileInfo> lockFile;
    std::unique_ptr<DatabaseManager> databaseManager;
    std::unique_ptr<extension::ExtensionManager> extensionManager;
    QueryIDGenerator queryIDGenerator;
    std::shared_ptr<common::DatabaseLifeCycleManager> dbLifeCycleManager;
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace function {

struct CastFunctionBindData : public FunctionBindData {
    // We don't allow configuring delimiters, ... in CAST function.
    // For performance purpose, we generate a default option object during binding time.
    common::CSVOption option;
    // TODO(Mahn): the following field should be removed once we refactor fixed list.
    uint64_t numOfEntries;

    explicit CastFunctionBindData(common::LogicalType dataType)
        : FunctionBindData{std::move(dataType)}, numOfEntries(0) {}

    inline std::unique_ptr<FunctionBindData> copy() const override {
        auto result = std::make_unique<CastFunctionBindData>(resultType.copy());
        result->numOfEntries = numOfEntries;
        result->option = option.copy();
        return result;
    }
};

} // namespace function
} // namespace kuzu

#include <memory>
#include <vector>


namespace kuzu {
namespace common {

// A DataChunk represents tuples as a set of value vectors and a selector array.
// The data chunk represents a subset of a relation i.e., a set of tuples as
// lists of the same length. It is appended into DataChunks and passed as intermediate
// representations between operators.
// A data chunk further contains a DataChunkState, which keeps the data chunk's size, selector, and
// currIdx (used when flattening and implies the value vector only contains the elements at currIdx
// of each value vector).
class KUZU_API DataChunk {
public:
    DataChunk() : DataChunk{0} {}
    explicit DataChunk(uint32_t numValueVectors)
        : DataChunk(numValueVectors, std::make_shared<DataChunkState>()){};

    DataChunk(uint32_t numValueVectors, const std::shared_ptr<DataChunkState>& state)
        : valueVectors(numValueVectors), state{state} {};
    DELETE_COPY_DEFAULT_MOVE(DataChunk);

    void insert(uint32_t pos, std::shared_ptr<ValueVector> valueVector);

    void resetAuxiliaryBuffer();

    uint32_t getNumValueVectors() const { return valueVectors.size(); }

    const ValueVector& getValueVector(uint64_t valueVectorPos) const {
        return *valueVectors[valueVectorPos];
    }
    ValueVector& getValueVectorMutable(uint64_t valueVectorPos) const {
        return *valueVectors[valueVectorPos];
    }

public:
    std::vector<std::shared_ptr<ValueVector>> valueVectors;
    std::shared_ptr<DataChunkState> state;
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace function {

/**
 * Binary operator assumes function with null returns null. This does NOT applies to binary boolean
 * operations (e.g. AND, OR, XOR).
 */

struct BinaryFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* /*leftValueVector*/, common::ValueVector* /*rightValueVector*/,
        common::ValueVector* /*resultValueVector*/, uint64_t /*resultPos*/, void* /*dataPtr*/) {
        OP::operation(left, right, result);
    }
};

struct BinaryListStructFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        common::ValueVector* resultValueVector, uint64_t /*resultPos*/, void* /*dataPtr*/) {
        OP::operation(left, right, result, *leftValueVector, *rightValueVector, *resultValueVector);
    }
};

struct BinaryMapCreationFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        common::ValueVector* resultValueVector, uint64_t /*resultPos*/, void* dataPtr) {
        OP::operation(left, right, result, *leftValueVector, *rightValueVector, *resultValueVector,
            dataPtr);
    }
};

struct BinaryListExtractFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        common::ValueVector* resultValueVector, uint64_t resultPos, void* /*dataPtr*/) {
        OP::operation(left, right, result, *leftValueVector, *rightValueVector, *resultValueVector,
            resultPos);
    }
};

struct BinaryStringFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* /*leftValueVector*/, common::ValueVector* /*rightValueVector*/,
        common::ValueVector* resultValueVector, uint64_t /*resultPos*/, void* /*dataPtr*/) {
        OP::operation(left, right, result, *resultValueVector);
    }
};

struct BinaryComparisonFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        common::ValueVector* /*resultValueVector*/, uint64_t /*resultPos*/, void* /*dataPtr*/) {
        OP::operation(left, right, result, leftValueVector, rightValueVector);
    }
};

struct BinaryUDFFunctionWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* /*leftValueVector*/, common::ValueVector* /*rightValueVector*/,
        common::ValueVector* /*resultValueVector*/, uint64_t /*resultPos*/, void* dataPtr) {
        OP::operation(left, right, result, dataPtr);
    }
};

struct BinarySelectWithBindDataWrapper {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename OP>
    static void operation(LEFT_TYPE& left, RIGHT_TYPE& right, uint8_t& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        void* dataPtr) {
        OP::operation(left, right, result, *leftValueVector, *rightValueVector, *leftValueVector,
            dataPtr);
    }
};

struct BinaryFunctionExecutor {

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static inline void executeOnValue(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& resultValueVector, uint64_t lPos, uint64_t rPos, uint64_t resPos,
        void* dataPtr) {
        OP_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos],
            ((RESULT_TYPE*)resultValueVector.getData())[resPos], &left, &right, &resultValueVector,
            resPos, dataPtr);
    }

    static inline std::tuple<common::sel_t, common::sel_t, common::sel_t> getSelectedPositions(
        common::SelectionVector* leftSelVector, common::SelectionVector* rightSelVector,
        common::SelectionVector* resultSelVector, common::sel_t selPos, bool leftFlat,
        bool rightFlat) {
        common::sel_t lPos = (*leftSelVector)[leftFlat ? 0 : selPos];
        common::sel_t rPos = (*rightSelVector)[rightFlat ? 0 : selPos];
        common::sel_t resPos = (*resultSelVector)[leftFlat && rightFlat ? 0 : selPos];
        return {lPos, rPos, resPos};
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeOnSelectedValues(common::ValueVector& left,
        common::SelectionVector* leftSelVector, common::ValueVector& right,
        common::SelectionVector* rightSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        const bool leftFlat = left.state->isFlat();
        const bool rightFlat = right.state->isFlat();

        const bool allNullsGuaranteed = (rightFlat && right.isNull((*rightSelVector)[0])) ||
                                        (leftFlat && left.isNull((*leftSelVector)[0]));
        if (allNullsGuaranteed) {
            result.setAllNull();
        } else {
            const bool noNullsGuaranteed = (leftFlat || left.hasNoNullsGuarantee()) &&
                                           (rightFlat || right.hasNoNullsGuarantee());
            if (noNullsGuaranteed) {
                result.setAllNonNull();
            }

            const auto numSelectedValues =
                leftFlat ? rightSelVector->getSelSize() : leftSelVector->getSelSize();
            for (common::sel_t selPos = 0; selPos < numSelectedValues; ++selPos) {
                auto [lPos, rPos, resPos] = getSelectedPositions(leftSelVector, rightSelVector,
                    resultSelVector, selPos, leftFlat, rightFlat);
                if (noNullsGuaranteed) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(left,
                        right, result, lPos, rPos, resPos, dataPtr);
                } else {
                    result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
                    if (!result.isNull(resPos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(left,
                            right, result, lPos, rPos, resPos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& left, common::SelectionVector* leftSelVector,
        common::ValueVector& right, common::SelectionVector* rightSelVector,
        common::ValueVector& result, common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        executeOnSelectedValues<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(left,
            leftSelVector, right, rightSelVector, result, resultSelVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& left, common::SelectionVector* leftSelVector,
        common::ValueVector& right, common::SelectionVector* rightSelVector,
        common::ValueVector& result, common::SelectionVector* resultSelVector) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryFunctionWrapper>(left,
            leftSelVector, right, rightSelVector, result, resultSelVector, nullptr /* dataPtr */);
    }

    struct BinarySelectWrapper {
        template<typename LEFT_TYPE, typename RIGHT_TYPE, typename OP>
        static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, uint8_t& result,
            common::ValueVector* /*leftValueVector*/, common::ValueVector* /*rightValueVector*/,
            void* /*dataPtr*/) {
            OP::operation(left, right, result);
        }
    };

    struct BinaryComparisonSelectWrapper {
        template<typename LEFT_TYPE, typename RIGHT_TYPE, typename OP>
        static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, uint8_t& result,
            common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
            void* /*dataPtr*/) {
            OP::operation(left, right, result, leftValueVector, rightValueVector);
        }
    };

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static void selectOnValue(common::ValueVector& left, common::ValueVector& right, uint64_t lPos,
        uint64_t rPos, uint64_t resPos, uint64_t& numSelectedValues,
        std::span<common::sel_t> selectedPositionsBuffer, void* dataPtr) {
        uint8_t resultValue = 0;
        SELECT_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos], resultValue,
            &left, &right, dataPtr);
        selectedPositionsBuffer[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static uint64_t selectBothFlat(common::ValueVector& left, common::ValueVector& right,
        void* dataPtr) {
        auto lPos = left.state->getSelVector()[0];
        auto rPos = right.state->getSelVector()[0];
        uint8_t resultValue = 0;
        if (!left.isNull(lPos) && !right.isNull(rPos)) {
            SELECT_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos],
                resultValue, &left, &right, dataPtr);
        }
        return resultValue == true;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC, typename SELECT_WRAPPER>
    static bool selectFlatUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector, void* dataPtr) {
        auto lPos = left.state->getSelVector()[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& rightSelVector = right.state->getSelVector();
        if (left.isNull(lPos)) {
            return numSelectedValues;
        } else if (right.hasNoNullsGuarantee()) {
            rightSelVector.forEach([&](auto i) {
                selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, lPos, i, i,
                    numSelectedValues, selectedPositionsBuffer, dataPtr);
            });
        } else {
            rightSelVector.forEach([&](auto i) {
                if (!right.isNull(i)) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, lPos, i,
                        i, numSelectedValues, selectedPositionsBuffer, dataPtr);
                }
            });
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC, typename SELECT_WRAPPER>
    static bool selectUnFlatFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector, void* dataPtr) {
        auto rPos = right.state->getSelVector()[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& leftSelVector = left.state->getSelVector();
        if (right.isNull(rPos)) {
            return numSelectedValues;
        } else if (left.hasNoNullsGuarantee()) {
            leftSelVector.forEach([&](auto i) {
                selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, i, rPos, i,
                    numSelectedValues, selectedPositionsBuffer, dataPtr);
            });
        } else {
            leftSelVector.forEach([&](auto i) {
                if (!left.isNull(i)) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, i, rPos,
                        i, numSelectedValues, selectedPositionsBuffer, dataPtr);
                }
            });
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    // Right, left, and result vectors share the same selectedPositions.
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static bool selectBothUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector, void* dataPtr) {
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getMutableBuffer();
        auto& leftSelVector = left.state->getSelVector();
        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
            leftSelVector.forEach([&](auto i) {
                selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, i, i, i,
                    numSelectedValues, selectedPositionsBuffer, dataPtr);
            });
        } else {
            leftSelVector.forEach([&](auto i) {
                auto isNull = left.isNull(i) || right.isNull(i);
                if (!isNull) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right, i, i, i,
                        numSelectedValues, selectedPositionsBuffer, dataPtr);
                }
            });
        }
        selVector.setSelSize(numSelectedValues);
        return numSelectedValues > 0;
    }

    // BOOLEAN (AND, OR, XOR)
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC,
        typename OP_WRAPPER = BinarySelectWrapper>
    static bool select(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector, void* dataPtr) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, OP_WRAPPER>(left, right, dataPtr);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, OP_WRAPPER>(left, right, selVector,
                dataPtr);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, OP_WRAPPER>(left, right, selVector,
                dataPtr);
        } else {
            return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, OP_WRAPPER>(left, right, selVector,
                dataPtr);
        }
    }

    // COMPARISON (GT, GTE, LT, LTE, EQ, NEQ)
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static bool selectComparison(common::ValueVector& left, common::ValueVector& right,
        common::SelectionVector& selVector, void* dataPtr) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(left,
                right, dataPtr);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector, dataPtr);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector, dataPtr);
        } else {
            return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector, dataPtr);
        }
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace function {

struct ConstFunctionExecutor {

    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result, common::SelectionVector& sel) {
        KU_ASSERT(result.state->isFlat());
        auto resultValues = (RESULT_TYPE*)result.getData();
        auto idx = sel[0];
        KU_ASSERT(idx == 0);
        OP::operation(resultValues[idx]);
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace function {

struct PointerFunctionExecutor {
    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result, common::SelectionVector& sel, void* dataPtr) {
        if (sel.isUnfiltered()) {
            for (auto i = 0u; i < sel.getSelSize(); i++) {
                OP::operation(result.getValue<RESULT_TYPE>(i), dataPtr);
            }
        } else {
            for (auto i = 0u; i < sel.getSelSize(); i++) {
                auto pos = sel[i];
                OP::operation(result.getValue<RESULT_TYPE>(pos), dataPtr);
            }
        }
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace function {

struct TernaryFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* /*resultValueVector*/, void* /*dataPtr*/) {
        OP::operation(a, b, c, result);
    }
};

struct TernaryStringFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* resultValueVector, void* /*dataPtr*/) {
        OP::operation(a, b, c, result, *(common::ValueVector*)resultValueVector);
    }
};

struct TernaryRegexFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* resultValueVector, void* dataPtr) {
        OP::operation(a, b, c, result, *(common::ValueVector*)resultValueVector, dataPtr);
    }
};

struct TernaryListFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* aValueVector, void* resultValueVector, void* /*dataPtr*/) {
        OP::operation(a, b, c, result, *(common::ValueVector*)aValueVector,
            *(common::ValueVector*)resultValueVector);
    }
};

struct TernaryUDFFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* /*resultValueVector*/, void* dataPtr) {
        OP::operation(a, b, c, result, dataPtr);
    }
};

struct TernaryFunctionExecutor {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, uint64_t aPos, uint64_t bPos,
        uint64_t cPos, uint64_t resPos, void* dataPtr) {
        auto resValues = (RESULT_TYPE*)result.getData();
        OP_WRAPPER::template operation<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            ((A_TYPE*)a.getData())[aPos], ((B_TYPE*)b.getData())[bPos],
            ((C_TYPE*)c.getData())[cPos], resValues[resPos], (void*)&a, (void*)&result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllFlat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        common::SelectionVector* cSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        auto aPos = (*aSelVector)[0];
        auto bPos = (*bSelVector)[0];
        auto cPos = (*cSelVector)[0];
        auto resPos = (*resultSelVector)[0];
        result.setNull(resPos, a.isNull(aPos) || b.isNull(bPos) || c.isNull(cPos));
        if (!result.isNull(resPos)) {
            executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result,
                aPos, bPos, cPos, resPos, dataPtr);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatFlatUnflat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        common::SelectionVector* cSelVector, common::ValueVector& result, common::SelectionVector*,
        void* dataPtr) {
        auto aPos = (*aSelVector)[0];
        auto bPos = (*bSelVector)[0];
        if (a.isNull(aPos) || b.isNull(bPos)) {
            result.setAllNull();
        } else if (c.hasNoNullsGuarantee()) {
            if (cSelVector->isUnfiltered()) {
                for (auto i = 0u; i < cSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < cSelVector->getSelSize(); ++i) {
                    auto pos = (*cSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (cSelVector->isUnfiltered()) {
                for (auto i = 0u; i < cSelVector->getSelSize(); ++i) {
                    result.setNull(i, c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < cSelVector->getSelSize(); ++i) {
                    auto pos = (*cSelVector)[i];
                    result.setNull(pos, c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatUnflat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        [[maybe_unused]] common::SelectionVector* cSelVector, common::ValueVector& result,
        common::SelectionVector*, void* dataPtr) {
        KU_ASSERT(bSelVector == cSelVector);
        auto aPos = (*aSelVector)[0];
        if (a.isNull(aPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (bSelVector->isUnfiltered()) {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, i, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    auto pos = (*bSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector->isUnfiltered()) {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    result.setNull(i, b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, i, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    auto pos = (*bSelVector)[i];
                    result.setNull(pos, b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatFlat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        common::SelectionVector* cSelVector, common::ValueVector& result, common::SelectionVector*,
        void* dataPtr) {
        auto aPos = (*aSelVector)[0];
        auto cPos = (*cSelVector)[0];
        if (a.isNull(aPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee()) {
            if (bSelVector->isUnfiltered()) {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    auto pos = (*bSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, aPos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector->isUnfiltered()) {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    result.setNull(i, b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector->getSelSize(); ++i) {
                    auto pos = (*bSelVector)[i];
                    result.setNull(pos, b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, aPos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllUnFlat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, [[maybe_unused]] common::SelectionVector* bSelVector,
        common::ValueVector& c, [[maybe_unused]] common::SelectionVector* cSelVector,
        common::ValueVector& result, common::SelectionVector*, void* dataPtr) {
        KU_ASSERT(aSelVector == bSelVector && bSelVector == cSelVector);
        if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (aSelVector->isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector->getSelSize(); i++) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, i, i, i, i, dataPtr);
                }
            } else {
                for (uint64_t i = 0; i < aSelVector->getSelSize(); i++) {
                    auto pos = (*aSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, pos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector->isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector->getSelSize(); i++) {
                    result.setNull(i, a.isNull(i) || b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, i, i, i, i, dataPtr);
                    }
                }
            } else {
                for (uint64_t i = 0; i < aSelVector->getSelSize(); i++) {
                    auto pos = (*aSelVector)[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, pos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatFlat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        common::SelectionVector* cSelVector, common::ValueVector& result, common::SelectionVector*,
        void* dataPtr) {
        auto bPos = (*bSelVector)[0];
        auto cPos = (*cSelVector)[0];
        if (b.isNull(bPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee()) {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, i, bPos, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*aSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, pos, bPos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, i, bPos, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*aSelVector)[i];
                    result.setNull(pos, a.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, pos, bPos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatUnflat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        [[maybe_unused]] common::SelectionVector* cSelVector, common::ValueVector& result,
        common::SelectionVector*, void* dataPtr) {
        KU_ASSERT(aSelVector == cSelVector);
        auto bPos = (*bSelVector)[0];
        if (b.isNull(bPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, i, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*aSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, pos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, i, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*bSelVector)[i];
                    result.setNull(pos, a.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, pos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatUnFlatFlat(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, [[maybe_unused]] common::SelectionVector* bSelVector,
        common::ValueVector& c, common::SelectionVector* cSelVector, common::ValueVector& result,
        common::SelectionVector*, void* dataPtr) {
        KU_ASSERT(aSelVector == bSelVector);
        auto cPos = (*cSelVector)[0];
        if (c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee()) {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, i, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*aSelVector)[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                        result, pos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector->isUnfiltered()) {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i) || b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, i, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector->getSelSize(); ++i) {
                    auto pos = (*aSelVector)[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b,
                            c, result, pos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& a, common::SelectionVector* aSelVector,
        common::ValueVector& b, common::SelectionVector* bSelVector, common::ValueVector& c,
        common::SelectionVector* cSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeAllFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, aSelVector, b,
                bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeFlatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeFlatUnflatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeFlatUnflatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeAllUnFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, aSelVector,
                b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeUnflatUnFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeUnflatFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeUnflatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a,
                aSelVector, b, bSelVector, c, cSelVector, result, resultSelVector, dataPtr);
        } else {
            KU_ASSERT(false);
        }
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace storage {
class Table;
}

namespace main {

class ClientContext;
class KUZU_API StorageDriver {
public:
    explicit StorageDriver(Database* database);

    ~StorageDriver();

    void scan(const std::string& nodeName, const std::string& propertyName,
        common::offset_t* offsets, size_t numOffsets, uint8_t* result, size_t numThreads);

    // TODO: Should merge following two functions into a single one.
    uint64_t getNumNodes(const std::string& nodeName) const;
    uint64_t getNumRels(const std::string& relName) const;

private:
    void scanColumn(storage::Table* table, common::column_id_t columnID,
        const common::offset_t* offsets, size_t size, uint8_t* result) const;

private:
    std::unique_ptr<ClientContext> clientContext;
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

struct UnaryFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos));
    }
};

struct UnarySequenceFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t /* resultPos */, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos), resultVector_, dataPtr);
    }
};

struct UnaryStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_);
    }
};

struct UnaryStructFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* /*inputVector*/, uint64_t /*inputPos*/, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_, dataPtr);
    }
};

struct UnaryCastStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto resultVector_ = (common::ValueVector*)resultVector;
        // TODO(Ziyi): the reinterpret_cast is not safe since we don't always pass
        // CastFunctionBindData
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_->getValue<RESULT_TYPE>(resultPos), resultVector_, inputPos,
            &reinterpret_cast<CastFunctionBindData*>(dataPtr)->option);
    }
};

struct UnaryNestedTypeFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryCastUnionFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_, dataPtr);
    }
};

struct UnaryUDFFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), dataPtr);
    }
};

struct UnaryFunctionExecutor {

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& inputVector, uint64_t inputPos,
        common::ValueVector& resultVector, uint64_t resultPos, void* dataPtr) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>((void*)&inputVector,
            inputPos, (void*)&resultVector, resultPos, dataPtr);
    }

    static std::pair<common::sel_t, common::sel_t> getSelectedPos(common::idx_t selIdx,
        common::SelectionVector* operandSelVector, common::SelectionVector* resultSelVector,
        bool operandIsUnfiltered, bool resultIsUnfiltered) {
        common::sel_t operandPos = operandIsUnfiltered ? selIdx : (*operandSelVector)[selIdx];
        common::sel_t resultPos = resultIsUnfiltered ? selIdx : (*resultSelVector)[selIdx];
        return {operandPos, resultPos};
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnSelectedValues(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        const bool noNullsGuaranteed = operand.hasNoNullsGuarantee();
        if (noNullsGuaranteed) {
            result.setAllNonNull();
        }

        const bool operandIsUnfiltered = operandSelVector->isUnfiltered();
        const bool resultIsUnfiltered = resultSelVector->isUnfiltered();

        for (auto i = 0u; i < operandSelVector->getSelSize(); i++) {
            const auto [operandPos, resultPos] = getSelectedPos(i, operandSelVector,
                resultSelVector, operandIsUnfiltered, resultIsUnfiltered);
            if (noNullsGuaranteed) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, operandPos,
                    result, resultPos, dataPtr);
            } else {
                result.setNull(resultPos, operand.isNull(operandPos));
                if (!result.isNull(resultPos)) {
                    executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, operandPos,
                        result, resultPos, dataPtr);
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (operand.state->isFlat()) {
            auto inputPos = (*operandSelVector)[0];
            auto resultPos = (*resultSelVector)[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(resultPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, inputPos,
                    result, resultPos, dataPtr);
            }
        } else {
            executeOnSelectedValues<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand,
                operandSelVector, result, resultSelVector, dataPtr);
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& operand, common::SelectionVector* operandSelVector,
        common::ValueVector& result, common::SelectionVector* resultSelVector) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(operand,
            operandSelVector, result, resultSelVector, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeSequence(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        auto inputPos = (*operandSelVector)[0];
        auto resultPos = (*resultSelVector)[0];
        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, UnarySequenceFunctionWrapper>(operand,
            inputPos, result, resultPos, dataPtr);
    }
};

} // namespace function
} // namespace kuzu

#include <unordered_set>


namespace kuzu {
namespace processor {

class ResultSet {
public:
    ResultSet() : ResultSet(0) {}
    explicit ResultSet(common::idx_t numDataChunks) : multiplicity{1}, dataChunks(numDataChunks) {}
    ResultSet(ResultSetDescriptor* resultSetDescriptor, storage::MemoryManager* memoryManager);

    void insert(common::idx_t pos, std::shared_ptr<common::DataChunk> dataChunk) {
        KU_ASSERT(dataChunks.size() > pos);
        dataChunks[pos] = std::move(dataChunk);
    }

    std::shared_ptr<common::DataChunk> getDataChunk(data_chunk_pos_t dataChunkPos) {
        return dataChunks[dataChunkPos];
    }
    std::shared_ptr<common::ValueVector> getValueVector(const DataPos& dataPos) const {
        return dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
    }

    // Our projection does NOT explicitly remove dataChunk from resultSet. Therefore, caller should
    // always provide a set of positions when reading from multiple dataChunks.
    uint64_t getNumTuples(const std::unordered_set<uint32_t>& dataChunksPosInScope) {
        return getNumTuplesWithoutMultiplicity(dataChunksPosInScope) * multiplicity;
    }

    uint64_t getNumTuplesWithoutMultiplicity(
        const std::unordered_set<uint32_t>& dataChunksPosInScope);

public:
    uint64_t multiplicity;
    std::vector<std::shared_ptr<common::DataChunk>> dataChunks;
};

} // namespace processor
} // namespace kuzu


namespace kuzu {
namespace function {

// Evaluate function at compile time, e.g. struct_extraction.
using scalar_func_compile_exec_t =
    std::function<void(FunctionBindData*, const std::vector<std::shared_ptr<common::ValueVector>>&,
        std::shared_ptr<common::ValueVector>&)>;
// Execute function.
using scalar_func_exec_t =
    std::function<void(const std::vector<std::shared_ptr<common::ValueVector>>&,
        const std::vector<common::SelectionVector*>&, common::ValueVector&,
        common::SelectionVector*, void*)>;
// Execute boolean function and write result to selection vector. Fast path for filter.
using scalar_func_select_t = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&, void*)>;

struct KUZU_API ScalarFunction : public ScalarOrAggregateFunction {
    scalar_func_exec_t execFunc = nullptr;
    scalar_func_select_t selectFunc = nullptr;
    scalar_func_compile_exec_t compileFunc = nullptr;

    ScalarFunction() = default;
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID} {}
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_func_exec_t execFunc)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          execFunc{std::move(execFunc)} {}
    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_func_exec_t execFunc,
        scalar_func_select_t selectFunc)
        : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          execFunc{std::move(execFunc)}, selectFunc{std::move(selectFunc)} {}

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryFunctionWrapper>(*params[0], paramSelVectors[0], *params[1], paramSelVectors[1],
            *params[2], paramSelVectors[2], result, resultSelVector, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryStringFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], *params[2], paramSelVectors[2], result, resultSelVector, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryRegexExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryRegexFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], *params[2], paramSelVectors[2], result, resultSelVector, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryListFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], *params[2], paramSelVectors[2], result, resultSelVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(*params[0],
            paramSelVectors[0], *params[1], paramSelVectors[1], result, resultSelVector);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryStringFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], result, resultSelVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryListStructFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], result, resultSelVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecWithBindData(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
            BinaryMapCreationFunctionWrapper>(*params[0], paramSelVectors[0], *params[1],
            paramSelVectors[1], result, resultSelVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector, void* dataPtr) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(*params[0], *params[1],
            selVector, dataPtr);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectWithBindData(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector, void* dataPtr) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC,
            BinarySelectWithBindDataWrapper>(*params[0], *params[1], selVector, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            *params[0], paramSelVectors[0], result, resultSelVector, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnarySequenceExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSequence<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0],
            paramSelVectors[0], result, resultSelVector, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStringFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
            nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryCastStringFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
            dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastFunctionWrapper>(
            *params[0], paramSelVectors[0], result, resultSelVector, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastUnionExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryCastUnionFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
            dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryExecNestedTypeFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryNestedTypeFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
            dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        const std::vector<common::SelectionVector*>& paramSelVectors, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStructFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
            dataPtr);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void NullaryExecFunction(
        [[maybe_unused]] const std::vector<std::shared_ptr<common::ValueVector>>& params,
        [[maybe_unused]] const std::vector<common::SelectionVector*>& paramSelVectors,
        common::ValueVector& result, common::SelectionVector* resultSelVector,
        void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.empty() && paramSelVectors.empty());
        ConstFunctionExecutor::execute<RESULT_TYPE, FUNC>(result, *resultSelVector);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void NullaryAuxilaryExecFunction(
        [[maybe_unused]] const std::vector<std::shared_ptr<common::ValueVector>>& params,
        [[maybe_unused]] const std::vector<common::SelectionVector*>& paramSelVectors,
        common::ValueVector& result, common::SelectionVector* resultSelVector, void* dataPtr) {
        KU_ASSERT(params.empty() && paramSelVectors.empty());
        PointerFunctionExecutor::execute<RESULT_TYPE, FUNC>(result, *resultSelVector, dataPtr);
    }

    virtual std::unique_ptr<ScalarFunction> copy() const {
        return std::make_unique<ScalarFunction>(*this);
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu::common {
class Profiler;
class NumericMetric;
class TimeMetric;
} // namespace kuzu::common
namespace kuzu {
namespace processor {
struct ExecutionContext;

using physical_op_id = uint32_t;

enum class PhysicalOperatorType : uint8_t {
    ALTER,
    AGGREGATE,
    AGGREGATE_FINALIZE,
    AGGREGATE_SCAN,
    ATTACH_DATABASE,
    BATCH_INSERT,
    COPY_TO,
    CREATE_MACRO,
    CREATE_SEQUENCE,
    CREATE_TABLE,
    CREATE_TYPE,
    CROSS_PRODUCT,
    DETACH_DATABASE,
    DELETE_,
    DROP,
    DUMMY_SINK,
    DUMMY_SIMPLE_SINK,
    EMPTY_RESULT,
    EXPORT_DATABASE,
    FILTER,
    FLATTEN,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    IMPORT_DATABASE,
    INDEX_LOOKUP,
    INSERT,
    INTERSECT_BUILD,
    INTERSECT,
    INSTALL_EXTENSION,
    LIMIT,
    LOAD_EXTENSION,
    MERGE,
    MULTIPLICITY_REDUCER,
    PARTITIONER,
    PATH_PROPERTY_PROBE,
    PRIMARY_KEY_SCAN_NODE_TABLE,
    PROJECTION,
    PROFILE,
    RECURSIVE_EXTEND,
    RESULT_COLLECTOR,
    SCAN_NODE_TABLE,
    SCAN_REL_TABLE,
    SEMI_MASKER,
    SET_PROPERTY,
    SKIP,
    STANDALONE_CALL,
    TABLE_FUNCTION_CALL,
    TOP_K,
    TOP_K_SCAN,
    TRANSACTION,
    ORDER_BY,
    ORDER_BY_MERGE,
    ORDER_BY_SCAN,
    UNION_ALL_SCAN,
    UNWIND,
    USE_DATABASE,
    UNINSTALL_EXTENSION,
};

class PhysicalOperator;
struct PhysicalOperatorUtils {
    static std::string operatorToString(const PhysicalOperator* physicalOp);
    KUZU_API static std::string operatorTypeToString(PhysicalOperatorType operatorType);
};

struct OperatorMetrics {
    common::TimeMetric& executionTime;
    common::NumericMetric& numOutputTuple;

    OperatorMetrics(common::TimeMetric& executionTime, common::NumericMetric& numOutputTuple)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple} {}
};

using physical_op_vector_t = std::vector<std::unique_ptr<PhysicalOperator>>;

class KUZU_API PhysicalOperator {
public:
    // Leaf operator
    PhysicalOperator(PhysicalOperatorType operatorType, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : id{id}, operatorType{operatorType}, resultSet(nullptr), printInfo{std::move(printInfo)} {}
    // Unary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child,
        physical_op_id id, std::unique_ptr<OPPrintInfo> printInfo);
    // Binary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> left,
        std::unique_ptr<PhysicalOperator> right, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo);
    PhysicalOperator(PhysicalOperatorType operatorType, physical_op_vector_t children,
        physical_op_id id, std::unique_ptr<OPPrintInfo> printInfo);

    virtual ~PhysicalOperator() = default;

    physical_op_id getOperatorID() const { return id; }

    PhysicalOperatorType getOperatorType() const { return operatorType; }

    virtual bool isSource() const { return false; }
    virtual bool isSink() const { return false; }
    virtual bool isParallel() const { return true; }

    void addChild(std::unique_ptr<PhysicalOperator> op) { children.push_back(std::move(op)); }
    PhysicalOperator* getChild(common::idx_t idx) const { return children[idx].get(); }
    common::idx_t getNumChildren() const { return children.size(); }
    std::unique_ptr<PhysicalOperator> moveUnaryChild();

    // Global state is initialized once.
    void initGlobalState(ExecutionContext* context);
    // Local state is initialized for each thread.
    void initLocalState(ResultSet* resultSet, ExecutionContext* context);

    bool getNextTuple(ExecutionContext* context);

    virtual void finalize(ExecutionContext* context);

    std::unordered_map<std::string, std::string> getProfilerKeyValAttributes(
        common::Profiler& profiler) const;
    std::vector<std::string> getProfilerAttributes(common::Profiler& profiler) const;

    const OPPrintInfo* getPrintInfo() const { return printInfo.get(); }

    virtual std::unique_ptr<PhysicalOperator> copy() = 0;

    virtual double getProgress(ExecutionContext* context) const;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
    template<class TARGET>
    const TARGET& constCast() {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

protected:
    virtual void initGlobalStateInternal(ExecutionContext* /*context*/) {}
    virtual void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {}
    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuplesInternal(ExecutionContext* context) = 0;

    std::string getTimeMetricKey() const { return "time-" + std::to_string(id); }
    std::string getNumTupleMetricKey() const { return "numTuple-" + std::to_string(id); }

    void registerProfilingMetrics(common::Profiler* profiler);

    double getExecutionTime(common::Profiler& profiler) const;
    uint64_t getNumOutputTuples(common::Profiler& profiler) const;

    virtual void finalizeInternal(ExecutionContext* /*context*/) {}

protected:
    physical_op_id id;
    std::unique_ptr<OperatorMetrics> metrics;
    PhysicalOperatorType operatorType;

    physical_op_vector_t children;
    ResultSet* resultSet;
    std::unique_ptr<OPPrintInfo> printInfo;
};

} // namespace processor
} // namespace kuzu


namespace kuzu {
namespace function {

struct UnaryUDFExecutor {
    template<class OPERAND_TYPE, class RESULT_TYPE>
    static inline void operation(OPERAND_TYPE& input, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*unary_udf_func)(OPERAND_TYPE);
        auto unaryUDFFunc = (unary_udf_func)udfFunc;
        result = unaryUDFFunc(input);
    }
};

struct BinaryUDFExecutor {
    template<class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        void* udfFunc) {
        typedef RESULT_TYPE (*binary_udf_func)(LEFT_TYPE, RIGHT_TYPE);
        auto binaryUDFFunc = (binary_udf_func)udfFunc;
        result = binaryUDFFunc(left, right);
    }
};

struct TernaryUDFExecutor {
    template<class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* udfFunc) {
        typedef RESULT_TYPE (*ternary_udf_func)(A_TYPE, B_TYPE, C_TYPE);
        auto ternaryUDFFunc = (ternary_udf_func)udfFunc;
        result = ternaryUDFFunc(a, b, c);
    }
};

struct UDF {
    template<typename T>
    static bool templateValidateType(const common::LogicalTypeID& type) {
        auto logicalType = common::LogicalType{type};
        auto physicalType = logicalType.getPhysicalType();
        auto physicalTypeMatch = common::TypeUtils::visit(physicalType,
            []<typename T1>(T1) { return std::is_same<T, T1>::value; });
        auto logicalTypeMatch = common::TypeUtils::visit(logicalType,
            []<typename T1>(T1) { return std::is_same<T, T1>::value; });
        return logicalTypeMatch || physicalTypeMatch;
    }

    template<typename T>
    static void validateType(const common::LogicalTypeID& type) {
        if (!templateValidateType<T>(type)) {
            throw common::CatalogException{
                "Incompatible udf parameter/return type and templated type."};
        }
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createEmptyParameterExecFunc(RESULT_TYPE (*)(Args...),
        const std::vector<common::LogicalTypeID>&) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE>
    static function::scalar_func_exec_t createEmptyParameterExecFunc(RESULT_TYPE (*udfFunc)(),
        const std::vector<common::LogicalTypeID>&) {
        KU_UNUSED(udfFunc); // Disable compiler warnings.
        return [udfFunc](
                   [[maybe_unused]] const std::vector<std::shared_ptr<common::ValueVector>>& params,
                   [[maybe_unused]] const std::vector<common::SelectionVector*>& paramSelVectors,
                   common::ValueVector& result, common::SelectionVector* resultSelVector,
                   void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.empty() && paramSelVectors.empty());
            for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
                auto resultPos = (*resultSelVector)[i];
                result.copyFromValue(resultPos, common::Value(udfFunc()));
            }
        };
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createUnaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename OPERAND_TYPE>
    static function::scalar_func_exec_t createUnaryExecFunc(RESULT_TYPE (*udfFunc)(OPERAND_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 1) {
            throw common::CatalogException{
                "Expected exactly one parameter type for unary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<OPERAND_TYPE>(parameterTypes[0]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                const std::vector<common::SelectionVector*>& paramSelVectors,
                common::ValueVector& result, common::SelectionVector* resultSelVector,
                void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 1);
            UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, UnaryUDFExecutor,
                UnaryUDFFunctionWrapper>(*params[0], paramSelVectors[0], result, resultSelVector,
                (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createBinaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename LEFT_TYPE, typename RIGHT_TYPE>
    static function::scalar_func_exec_t createBinaryExecFunc(
        RESULT_TYPE (*udfFunc)(LEFT_TYPE, RIGHT_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 2) {
            throw common::CatalogException{
                "Expected exactly two parameter types for binary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<LEFT_TYPE>(parameterTypes[0]);
        validateType<RIGHT_TYPE>(parameterTypes[1]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                const std::vector<common::SelectionVector*>& paramSelVectors,
                common::ValueVector& result, common::SelectionVector* resultSelVector,
                void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 2);
            BinaryFunctionExecutor::executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE,
                BinaryUDFExecutor, BinaryUDFFunctionWrapper>(*params[0], paramSelVectors[0],
                *params[1], paramSelVectors[1], result, resultSelVector, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createTernaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename A_TYPE, typename B_TYPE, typename C_TYPE>
    static function::scalar_func_exec_t createTernaryExecFunc(
        RESULT_TYPE (*udfFunc)(A_TYPE, B_TYPE, C_TYPE),
        std::vector<common::LogicalTypeID> parameterTypes) {
        if (parameterTypes.size() != 3) {
            throw common::CatalogException{
                "Expected exactly three parameter types for ternary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<A_TYPE>(parameterTypes[0]);
        validateType<B_TYPE>(parameterTypes[1]);
        validateType<C_TYPE>(parameterTypes[2]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                const std::vector<common::SelectionVector*>& paramSelVectors,
                common::ValueVector& result, common::SelectionVector* resultSelVector,
                void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 3);
            TernaryFunctionExecutor::executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE,
                TernaryUDFExecutor, TernaryUDFFunctionWrapper>(*params[0], paramSelVectors[0],
                *params[1], paramSelVectors[1], *params[2], paramSelVectors[2], result,
                resultSelVector, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename TR, typename... Args>
    static scalar_func_exec_t getScalarExecFunc(TR (*udfFunc)(Args...),
        std::vector<common::LogicalTypeID> parameterTypes) {
        constexpr auto numArgs = sizeof...(Args);
        switch (numArgs) {
        case 0:
            return createEmptyParameterExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 1:
            return createUnaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 2:
            return createBinaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 3:
            return createTernaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        default:
            throw common::BinderException("UDF function only supported until ternary!");
        }
    }

    template<typename T>
    static common::LogicalTypeID getParameterType() {
        if (std::is_same<T, bool>()) {
            return common::LogicalTypeID::BOOL;
        } else if (std::is_same<T, int8_t>()) {
            return common::LogicalTypeID::INT8;
        } else if (std::is_same<T, int16_t>()) {
            return common::LogicalTypeID::INT16;
        } else if (std::is_same<T, int32_t>()) {
            return common::LogicalTypeID::INT32;
        } else if (std::is_same<T, int64_t>()) {
            return common::LogicalTypeID::INT64;
        } else if (std::is_same<T, common::int128_t>()) {
            return common::LogicalTypeID::INT128;
        } else if (std::is_same<T, uint8_t>()) {
            return common::LogicalTypeID::UINT8;
        } else if (std::is_same<T, uint16_t>()) {
            return common::LogicalTypeID::UINT16;
        } else if (std::is_same<T, uint32_t>()) {
            return common::LogicalTypeID::UINT32;
        } else if (std::is_same<T, uint64_t>()) {
            return common::LogicalTypeID::UINT64;
        } else if (std::is_same<T, float>()) {
            return common::LogicalTypeID::FLOAT;
        } else if (std::is_same<T, double>()) {
            return common::LogicalTypeID::DOUBLE;
        } else if (std::is_same<T, common::ku_string_t>()) {
            return common::LogicalTypeID::STRING;
        } else {
            KU_UNREACHABLE;
        }
    }

    template<typename TA>
    static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
        arguments.push_back(getParameterType<TA>());
    }

    template<typename TA, typename TB, typename... Args>
    static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
        arguments.push_back(getParameterType<TA>());
        getParameterTypesRecursive<TB, Args...>(arguments);
    }

    template<typename... Args>
    static std::vector<common::LogicalTypeID> getParameterTypes() {
        std::vector<common::LogicalTypeID> parameterTypes;
        if constexpr (sizeof...(Args) > 0) {
            getParameterTypesRecursive<Args...>(parameterTypes);
        }
        return parameterTypes;
    }

    template<typename TR, typename... Args>
    static function_set getFunction(std::string name, TR (*udfFunc)(Args...),
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
        function_set definitions;
        if (returnType == common::LogicalTypeID::STRING) {
            KU_UNREACHABLE;
        }
        validateType<TR>(returnType);
        scalar_func_exec_t scalarExecFunc = getScalarExecFunc<TR, Args...>(udfFunc, parameterTypes);
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            std::move(parameterTypes), returnType, std::move(scalarExecFunc)));
        return definitions;
    }

    template<typename TR, typename... Args>
    static function_set getFunction(std::string name, TR (*udfFunc)(Args...)) {
        return getFunction<TR, Args...>(std::move(name), udfFunc, getParameterTypes<Args...>(),
            getParameterType<TR>());
    }

    template<typename TR, typename... Args>
    static function_set getVectorizedFunction(std::string name, scalar_func_exec_t execFunc) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            getParameterTypes<Args...>(), getParameterType<TR>(), std::move(execFunc)));
        return definitions;
    }

    static function_set getVectorizedFunction(std::string name, scalar_func_exec_t execFunc,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            std::move(parameterTypes), returnType, std::move(execFunc)));
        return definitions;
    }
};

} // namespace function
} // namespace kuzu

#include <mutex>


namespace kuzu {
namespace binder {
class BoundReadingClause;
}
namespace parser {
struct YieldVariable;
class ParsedExpression;
} // namespace parser

namespace planner {
class LogicalOperator;
class LogicalPlan;
class Planner;
} // namespace planner

namespace processor {
struct ExecutionContext;
class PlanMapper;
} // namespace processor

namespace function {

struct TableFuncBindInput;
struct TableFuncBindData;

// Shared state
struct KUZU_API TableFuncSharedState {
    common::row_idx_t numRows = 0;
    // This for now is only used for QueryHNSWIndex.
    // TODO(Guodong): This is not a good way to pass semiMasks to QueryHNSWIndex function.
    // However, to avoid function specific logic when we handle semi mask in mapper, so we can move
    // HNSW into an extension, we have to let semiMasks be owned by a base class.
    common::NodeOffsetMaskMap semiMasks;
    std::mutex mtx;

    explicit TableFuncSharedState() = default;
    explicit TableFuncSharedState(common::row_idx_t numRows) : numRows{numRows} {}
    virtual ~TableFuncSharedState() = default;
    virtual uint64_t getNumRows() const { return numRows; }

    common::table_id_map_t<common::SemiMask*> getSemiMasks() const { return semiMasks.getMasks(); }

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

// Local state
struct TableFuncLocalState {
    virtual ~TableFuncLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

// Execution input
struct TableFuncInput {
    TableFuncBindData* bindData;
    TableFuncLocalState* localState;
    TableFuncSharedState* sharedState;
    processor::ExecutionContext* context;

    TableFuncInput() = default;
    TableFuncInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
        TableFuncSharedState* sharedState, processor::ExecutionContext* context)
        : bindData{bindData}, localState{localState}, sharedState{sharedState}, context{context} {}
    DELETE_COPY_DEFAULT_MOVE(TableFuncInput);
};

// Execution output.
// We might want to merge this with TableFuncLocalState. Also not all table function output vectors
// in a single dataChunk, e.g. FTableScan. In future, if we have more cases, we should consider
// make TableFuncOutput pure virtual.
struct TableFuncOutput {
    common::DataChunk dataChunk;

    explicit TableFuncOutput(common::DataChunk dataChunk) : dataChunk{std::move(dataChunk)} {}
    virtual ~TableFuncOutput() = default;

    void resetState();
    void setOutputSize(common::offset_t size) const;
};

struct KUZU_API TableFuncInitSharedStateInput final {
    TableFuncBindData* bindData;
    processor::ExecutionContext* context;

    TableFuncInitSharedStateInput(TableFuncBindData* bindData, processor::ExecutionContext* context)
        : bindData{bindData}, context{context} {}
};

// Init local state
struct TableFuncInitLocalStateInput {
    TableFuncSharedState& sharedState;
    TableFuncBindData& bindData;
    main::ClientContext* clientContext;

    TableFuncInitLocalStateInput(TableFuncSharedState& sharedState, TableFuncBindData& bindData,
        main::ClientContext* clientContext)
        : sharedState{sharedState}, bindData{bindData}, clientContext{clientContext} {}
};

// Init output
struct TableFuncInitOutputInput {
    std::vector<processor::DataPos> outColumnPositions;
    processor::ResultSet& resultSet;

    TableFuncInitOutputInput(std::vector<processor::DataPos> outColumnPositions,
        processor::ResultSet& resultSet)
        : outColumnPositions{std::move(outColumnPositions)}, resultSet{resultSet} {}
};

using table_func_bind_t = std::function<std::unique_ptr<TableFuncBindData>(main::ClientContext*,
    const TableFuncBindInput*)>;
using table_func_t =
    std::function<common::offset_t(const TableFuncInput&, TableFuncOutput& output)>;
using table_func_init_shared_t =
    std::function<std::shared_ptr<TableFuncSharedState>(const TableFuncInitSharedStateInput&)>;
using table_func_init_local_t =
    std::function<std::unique_ptr<TableFuncLocalState>(const TableFuncInitLocalStateInput&)>;
using table_func_init_output_t =
    std::function<std::unique_ptr<TableFuncOutput>(const TableFuncInitOutputInput&)>;
using table_func_can_parallel_t = std::function<bool()>;
using table_func_progress_t = std::function<double(TableFuncSharedState* sharedState)>;
using table_func_finalize_t =
    std::function<void(const processor::ExecutionContext*, TableFuncSharedState*)>;
using table_func_rewrite_t =
    std::function<std::string(main::ClientContext&, const TableFuncBindData& bindData)>;
using table_func_get_logical_plan_t =
    std::function<void(planner::Planner*, const binder::BoundReadingClause&,
        std::vector<std::shared_ptr<binder::Expression>>, planner::LogicalPlan&)>;
using table_func_get_physical_plan_t = std::function<std::unique_ptr<processor::PhysicalOperator>(
    processor::PlanMapper*, const planner::LogicalOperator*)>;
using table_func_infer_input_types =
    std::function<std::vector<common::LogicalType>(const binder::expression_vector&)>;

struct KUZU_API TableFunction final : Function {
    table_func_t tableFunc = nullptr;
    table_func_bind_t bindFunc = nullptr;
    table_func_init_shared_t initSharedStateFunc = nullptr;
    table_func_init_local_t initLocalStateFunc = nullptr;
    table_func_init_output_t initOutputFunc = nullptr;
    table_func_can_parallel_t canParallelFunc = [] { return true; };
    table_func_progress_t progressFunc = [](TableFuncSharedState*) { return 0.0; };
    table_func_finalize_t finalizeFunc = [](auto, auto) {};
    table_func_rewrite_t rewriteFunc = nullptr;
    table_func_get_logical_plan_t getLogicalPlanFunc = getLogicalPlan;
    table_func_get_physical_plan_t getPhysicalPlanFunc = getPhysicalPlan;
    table_func_infer_input_types inferInputTypes = nullptr;

    TableFunction() {}
    TableFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
        : Function{std::move(name), std::move(inputTypes)} {}
    ~TableFunction() override;
    TableFunction(const TableFunction&) = default;
    TableFunction& operator=(const TableFunction& other) = default;
    DEFAULT_BOTH_MOVE(TableFunction);

    std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    std::unique_ptr<TableFunction> copy() const { return std::make_unique<TableFunction>(*this); }

    // Init local state func
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(
        const TableFuncInitLocalStateInput& input);
    // Init shared state func
    static std::unique_ptr<TableFuncSharedState> initEmptySharedState(
        const TableFuncInitSharedStateInput& input);
    // Init output func
    static std::unique_ptr<TableFuncOutput> initSingleDataChunkScanOutput(
        const TableFuncInitOutputInput& input);
    // Utility functions
    static std::vector<std::string> extractYieldVariables(const std::vector<std::string>& names,
        const std::vector<parser::YieldVariable>& yieldVariables);
    // Get logical plan func
    static void getLogicalPlan(planner::Planner* planner,
        const binder::BoundReadingClause& boundReadingClause, binder::expression_vector predicates,
        planner::LogicalPlan& plan);
    // Get physical plan func
    static std::unique_ptr<processor::PhysicalOperator> getPhysicalPlan(
        processor::PlanMapper* planMapper, const planner::LogicalOperator* logicalOp);
    // Table func
    static common::offset_t emptyTableFunc(const TableFuncInput& input, TableFuncOutput& output);
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace function {

struct ScanReplacementData {
    TableFunction func;
    TableFuncBindInput bindInput;
};

using scan_replace_handle_t = uint8_t*;
using handle_lookup_func_t = std::function<std::vector<scan_replace_handle_t>(const std::string&)>;
using scan_replace_func_t =
    std::function<std::unique_ptr<ScanReplacementData>(std::span<scan_replace_handle_t>)>;

struct ScanReplacement {
    explicit ScanReplacement(handle_lookup_func_t lookupFunc, scan_replace_func_t replaceFunc)
        : lookupFunc(std::move(lookupFunc)), replaceFunc{std::move(replaceFunc)} {}

    handle_lookup_func_t lookupFunc;
    scan_replace_func_t replaceFunc;
};

} // namespace function
} // namespace kuzu

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>


namespace kuzu {
namespace parser {
class StandaloneCallRewriter;
} // namespace parser

namespace binder {
class Binder;
class ExpressionBinder;
} // namespace binder

namespace common {
class RandomEngine;
class TaskScheduler;
class ProgressBar;
} // namespace common

namespace extension {
class ExtensionManager;
} // namespace extension

namespace processor {
class ImportDB;
class TableFunctionCall;
} // namespace processor

namespace graph {
class GraphEntrySet;
}

namespace main {
struct DBConfig;
class Database;
class DatabaseManager;
class AttachedKuzuDatabase;
struct SpillToDiskSetting;
struct ExtensionOption;
class EmbeddedShell;

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so the profiler
 * is not maintained in the client context.
 */
class KUZU_API ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class binder::ExpressionBinder;
    friend class processor::ImportDB;
    friend class processor::TableFunctionCall;
    friend class parser::StandaloneCallRewriter;
    friend struct SpillToDiskSetting;
    friend class EmbeddedShell;
    friend class extension::ExtensionManager;

public:
    explicit ClientContext(Database* database);
    ~ClientContext();

    // Client config
    const ClientConfig* getClientConfig() const { return &clientConfig; }
    ClientConfig* getClientConfigUnsafe() { return &clientConfig; }
    const DBConfig* getDBConfig() const { return &dbConfig; }
    DBConfig* getDBConfigUnsafe() { return &dbConfig; }
    common::Value getCurrentSetting(const std::string& optionName) const;
    // Timer and timeout
    void interrupt() { activeQuery.interrupted = true; }
    bool interrupted() const { return activeQuery.interrupted; }
    bool hasTimeout() const { return clientConfig.timeoutInMS != 0; }
    void setQueryTimeOut(uint64_t timeoutInMS);
    uint64_t getQueryTimeOut() const;
    void startTimer();
    uint64_t getTimeoutRemainingInMS() const;
    void resetActiveQuery() { activeQuery.reset(); }

    // Parallelism
    void setMaxNumThreadForExec(uint64_t numThreads);
    uint64_t getMaxNumThreadForExec() const;

    // Transaction.
    transaction::Transaction* getTransaction() const;
    transaction::TransactionContext* getTransactionContext() const;

    // Progress bar
    common::ProgressBar* getProgressBar() const;

    // Replace function.
    void addScanReplace(function::ScanReplacement scanReplacement);
    std::unique_ptr<function::ScanReplacementData> tryReplaceByName(
        const std::string& objectName) const;
    std::unique_ptr<function::ScanReplacementData> tryReplaceByHandle(
        function::scan_replace_handle_t handle) const;
    // Extension
    void setExtensionOption(std::string name, common::Value value);
    const ExtensionOption* getExtensionOption(std::string optionName) const;
    std::string getExtensionDir() const;

    // Database component getters.
    std::string getDatabasePath() const;
    Database* getDatabase() const { return localDatabase; }
    common::TaskScheduler* getTaskScheduler() const;
    DatabaseManager* getDatabaseManager() const;
    storage::StorageManager* getStorageManager() const;
    storage::MemoryManager* getMemoryManager() const;
    extension::ExtensionManager* getExtensionManager() const;
    storage::WAL* getWAL() const;
    catalog::Catalog* getCatalog() const;
    transaction::TransactionManager* getTransactionManagerUnsafe() const;
    common::VirtualFileSystem* getVFSUnsafe() const;
    common::RandomEngine* getRandomEngine() const;
    bool isInMemory() const;

    static std::string getEnvVariable(const std::string& name);
    static std::string getUserHomeDir();

    void setDefaultDatabase(AttachedKuzuDatabase* defaultDatabase_);
    bool hasDefaultDatabase() const;
    void setUseInternalCatalogEntry(bool useInternalCatalogEntry) {
        this->useInternalCatalogEntry_ = useInternalCatalogEntry;
    }
    bool useInternalCatalogEntry() const {
        return clientConfig.enableInternalCatalog ? true : useInternalCatalogEntry_;
    }

    void addScalarFunction(std::string name, function::function_set definitions);
    void removeScalarFunction(const std::string& name);

    processor::WarningContext& getWarningContextUnsafe();
    const processor::WarningContext& getWarningContext() const;

    graph::GraphEntrySet& getGraphEntrySetUnsafe();

    const graph::GraphEntrySet& getGraphEntrySet() const;

    void cleanUp();

    // Query.
    std::unique_ptr<PreparedStatement> prepareWithParams(std::string_view query,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams = {});
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams,
        std::optional<uint64_t> queryID = std::nullopt);
    std::unique_ptr<QueryResult> query(std::string_view queryStatement,
        std::optional<uint64_t> queryID = std::nullopt);

private:
    struct TransactionHelper {
        enum class TransactionCommitAction : uint8_t {
            COMMIT_IF_NEW,
            COMMIT_IF_AUTO,
            COMMIT_NEW_OR_AUTO,
            NOT_COMMIT
        };
        static bool commitIfNew(TransactionCommitAction action) {
            return action == TransactionCommitAction::COMMIT_IF_NEW ||
                   action == TransactionCommitAction::COMMIT_NEW_OR_AUTO;
        }
        static bool commitIfAuto(TransactionCommitAction action) {
            return action == TransactionCommitAction::COMMIT_IF_AUTO ||
                   action == TransactionCommitAction::COMMIT_NEW_OR_AUTO;
        }
        static TransactionCommitAction getAction(bool commitIfNew, bool commitIfAuto);
        static void runFuncInTransaction(transaction::TransactionContext& context,
            const std::function<void()>& fun, bool readOnlyStatement, bool isTransactionStatement,
            TransactionCommitAction action);
    };

    static std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);
    static std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);
    static void bindParametersNoLock(const PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);
    void validateTransaction(const PreparedStatement& preparedStatement) const;

    std::vector<std::shared_ptr<parser::Statement>> parseQuery(std::string_view query);

    std::unique_ptr<PreparedStatement> prepareNoLock(
        std::shared_ptr<parser::Statement> parsedStatement, bool shouldCommitNewTransaction,
        std::optional<std::unordered_map<std::string, std::shared_ptr<common::Value>>> inputParams =
            std::nullopt);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_unique<common::Value>((T)arg.second);
        params.insert({name, std::move(val)});
        return executeWithParams(preparedStatement, std::move(params), args...);
    }

    std::unique_ptr<QueryResult> executeNoLock(PreparedStatement* preparedStatement,
        std::optional<uint64_t> queryID = std::nullopt);

    std::unique_ptr<QueryResult> queryNoLock(std::string_view query,
        std::optional<uint64_t> queryID = std::nullopt);

    bool canExecuteWriteQuery() const;

    std::unique_ptr<QueryResult> handleFailedExecution(std::optional<uint64_t> queryID,
        const std::exception& e) const;

    // Client side configurable settings.
    ClientConfig clientConfig;
    // Database configurable settings.
    DBConfig& dbConfig;
    // Current query.
    ActiveQuery activeQuery;
    // Transaction context.
    std::unique_ptr<transaction::TransactionContext> transactionContext;
    // Replace external object as pointer Value;
    std::vector<function::ScanReplacement> scanReplacements;
    // Extension configurable settings.
    std::unordered_map<std::string, common::Value> extensionOptionValues;
    // Random generator for UUID.
    std::unique_ptr<common::RandomEngine> randomEngine;
    // Local database.
    Database* localDatabase;
    // Remote database.
    AttachedKuzuDatabase* remoteDatabase;
    // Progress bar.
    std::unique_ptr<common::ProgressBar> progressBar;
    // Warning information
    processor::WarningContext warningContext;
    // Graph entries
    std::unique_ptr<graph::GraphEntrySet> graphEntrySet;
    std::mutex mtx;
    // Whether the query can access internal tables/sequences or not.
    bool useInternalCatalogEntry_ = false;
    // Whether the transaction should be rolled back on destruction. If the parent database is
    // closed, the rollback should be prevented or it will SEGFAULT.
    bool preventTransactionRollbackOnDestruction = false;
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace main {

/**
 * @brief Connection is used to interact with a Database instance. Each Connection is thread-safe.
 * Multiple connections can connect to the same Database instance in a multi-threaded environment.
 */
class Connection {
    friend class testing::BaseGraphTest;
    friend class testing::PrivateGraphTest;
    friend class testing::TestHelper;
    friend class benchmark::Benchmark;
    friend class ConnectionExecuteAsyncWorker;
    friend class ConnectionQueryAsyncWorker;

public:
    /**
     * @brief Creates a connection to the database.
     * @param database A pointer to the database instance that this connection will be connected to.
     */
    KUZU_API explicit Connection(Database* database);
    /**
     * @brief Destructs the connection.
     */
    KUZU_API ~Connection();
    /**
     * @brief Sets the maximum number of threads to use for execution in the current connection.
     * @param numThreads The number of threads to use for execution in the current connection.
     */
    KUZU_API void setMaxNumThreadForExec(uint64_t numThreads);
    /**
     * @brief Returns the maximum number of threads to use for execution in the current connection.
     * @return the maximum number of threads to use for execution in the current connection.
     */
    KUZU_API uint64_t getMaxNumThreadForExec();

    /**
     * @brief Executes the given query and returns the result.
     * @param query The query to execute.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> query(std::string_view query);

    /**
     * @brief Prepares the given query and returns the prepared statement.
     * @param query The query to prepare.
     * @return the prepared statement.
     */
    KUZU_API std::unique_ptr<PreparedStatement> prepare(std::string_view query);

    /**
     * @brief Prepares the given query and returns the prepared statement.
     * @param query The query to prepare.
     * @param inputParams The parameter pack where each arg is a pair with the first element
     * being parameter name and second element being parameter value. The only parameters that are
     * relevant during prepare are ones that will be substituted with a scan source. Any other
     * parameters will either be ignored or will cause an error to be thrown.
     * @return the prepared statement.
     */
    KUZU_API std::unique_ptr<PreparedStatement> prepareWithParams(std::string_view query,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams);

    /**
     * @brief Executes the given prepared statement with args and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param args The parameter pack where each arg is a std::pair with the first element being
     * parameter name and second element being parameter value.
     * @return the result of the query.
     */
    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(PreparedStatement* preparedStatement,
        std::pair<std::string, Args>... args) {
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParameters;
        return executeWithParams(preparedStatement, std::move(inputParameters), args...);
    }
    /**
     * @brief Executes the given prepared statement with inputParams and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param inputParams The parameter pack where each arg is a std::pair with the first element
     * being parameter name and second element being parameter value.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams);
    /**
     * @brief interrupts all queries currently executing within this connection.
     */
    KUZU_API void interrupt();

    /**
     * @brief sets the query timeout value of the current connection. A value of zero (the default)
     * disables the timeout.
     */
    KUZU_API void setQueryTimeOut(uint64_t timeoutInMS);

    // Note: this function throws exception if creating scalar function fails.
    template<typename TR, typename... Args>
    void createScalarFunction(std::string name, TR (*udfFunc)(Args...)) {
        addScalarFunction(name, function::UDF::getFunction<TR, Args...>(name, udfFunc));
    }

    // Note: this function throws exception if creating scalar function fails.
    template<typename TR, typename... Args>
    void createScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypes,
        common::LogicalTypeID returnType, TR (*udfFunc)(Args...)) {
        addScalarFunction(name, function::UDF::getFunction<TR, Args...>(name, udfFunc,
                                    std::move(parameterTypes), returnType));
    }

    void addUDFFunctionSet(std::string name, function::function_set func) {
        addScalarFunction(name, std::move(func));
    }

    void removeUDFFunction(std::string name) { removeScalarFunction(name); }

    template<typename TR, typename... Args>
    void createVectorizedFunction(std::string name, function::scalar_func_exec_t scalarFunc) {
        addScalarFunction(name,
            function::UDF::getVectorizedFunction<TR, Args...>(name, std::move(scalarFunc)));
    }

    void createVectorizedFunction(std::string name,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType,
        function::scalar_func_exec_t scalarFunc) {
        addScalarFunction(name, function::UDF::getVectorizedFunction(name, std::move(scalarFunc),
                                    std::move(parameterTypes), returnType));
    }

    ClientContext* getClientContext() { return clientContext.get(); };

private:
    std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);

    std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        return clientContext->executeWithParams(preparedStatement, std::move(params), arg, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    KUZU_API void addScalarFunction(std::string name, function::function_set definitions);
    KUZU_API void removeScalarFunction(std::string name);

    std::unique_ptr<QueryResult> queryWithID(std::string_view query, uint64_t queryID);

    std::unique_ptr<QueryResult> executeWithParamsWithID(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams,
        uint64_t queryID);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::shared_ptr<common::DatabaseLifeCycleManager> dbLifeCycleManager;
};

} // namespace main
} // namespace kuzu

