#pragma once

#ifndef KUZU_API
#define KUZU_API
#endif


#include <cstdint>
#include <string>


namespace kuzu {
namespace common {

struct timestamp_t;
struct date_t;

KUZU_API enum class DatePartSpecifier : uint8_t {
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
};

KUZU_API struct interval_t {
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
    KUZU_API static constexpr const int32_t MONTHS_PER_YEAR = 12;
    KUZU_API static constexpr const int64_t MSECS_PER_SEC = 1000;
    KUZU_API static constexpr const int32_t SECS_PER_MINUTE = 60;
    KUZU_API static constexpr const int32_t MINS_PER_HOUR = 60;
    KUZU_API static constexpr const int32_t HOURS_PER_DAY = 24;
    // only used for interval comparison/ordering purposes, in which case a month counts as 30 days
    KUZU_API static constexpr const int64_t DAYS_PER_MONTH = 30;
    KUZU_API static constexpr const int64_t MONTHS_PER_QUARTER = 3;
    KUZU_API static constexpr const int64_t MONTHS_PER_MILLENIUM = 12000;
    KUZU_API static constexpr const int64_t MONTHS_PER_CENTURY = 1200;
    KUZU_API static constexpr const int64_t MONTHS_PER_DECADE = 120;

    KUZU_API static constexpr const int64_t MICROS_PER_MSEC = 1000;
    KUZU_API static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    KUZU_API static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    KUZU_API static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    KUZU_API static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
    KUZU_API static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

    KUZU_API static constexpr const int64_t NANOS_PER_MICRO = 1000;

    KUZU_API static void addition(interval_t& result, uint64_t number, std::string specifierStr);
    KUZU_API static void parseIntervalField(
        std::string buf, uint64_t& pos, uint64_t len, interval_t& result);
    KUZU_API static interval_t FromCString(const char* str, uint64_t len);
    KUZU_API static std::string toString(interval_t interval);
    KUZU_API static bool GreaterThan(const interval_t& left, const interval_t& right);
    KUZU_API static void NormalizeIntervalEntries(
        interval_t input, int64_t& months, int64_t& days, int64_t& micros);
    KUZU_API static void TryGetDatePartSpecifier(
        std::string specifier_p, DatePartSpecifier& result);
    KUZU_API static int32_t getIntervalPart(DatePartSpecifier specifier, interval_t& timestamp);
    KUZU_API static int64_t getMicro(const interval_t& val);
    KUZU_API static int64_t getNanoseconds(const interval_t& val);
};

} // namespace common
} // namespace kuzu


#include <cstdint>


namespace kuzu {
namespace common {

struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

using table_id_t = uint64_t;
using offset_t = uint64_t;
constexpr table_id_t INVALID_TABLE_ID = UINT64_MAX;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;

// System representation for internalID.
KUZU_API struct internalID_t {
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

} // namespace common
} // namespace kuzu


#include <stdexcept>
#include <string>

namespace kuzu {
namespace common {

class Exception : public std::exception {
public:
    explicit Exception(std::string msg) : exception(), exception_message_(std::move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

    // TODO(Guodong): this is being used in both loader and node table. A better way to do this
    // could be throw this error msg during insert.
    static std::string getExistedPKExceptionMsg(const std::string& pkString) {
        auto result = "A node is created with an existed primary key " + pkString +
                      ", which violates the uniqueness constraint of the primary key property.";
        return result;
    };

private:
    std::string exception_message_;
};

class ParserException : public Exception {
public:
    explicit ParserException(const std::string& msg) : Exception("Parser exception: " + msg){};
};

class BinderException : public Exception {
public:
    explicit BinderException(const std::string& msg) : Exception("Binder exception: " + msg){};
};

class ConversionException : public Exception {
public:
    explicit ConversionException(const std::string& msg) : Exception(msg){};
};

class CopyException : public Exception {
public:
    explicit CopyException(const std::string& msg) : Exception("Copy exception: " + msg){};
};

class CatalogException : public Exception {
public:
    explicit CatalogException(const std::string& msg) : Exception("Catalog exception: " + msg){};
};

class HashIndexException : public Exception {
public:
    explicit HashIndexException(const std::string& msg)
        : Exception("HashIndex exception: " + msg){};
};

class StorageException : public Exception {
public:
    explicit StorageException(const std::string& msg) : Exception("Storage exception: " + msg){};
};

class BufferManagerException : public Exception {
public:
    explicit BufferManagerException(const std::string& msg)
        : Exception("Buffer manager exception: " + msg){};
};

class InternalException : public Exception {
public:
    explicit InternalException(const std::string& msg) : Exception(msg){};
};

class NotImplementedException : public Exception {
public:
    explicit NotImplementedException(const std::string& msg) : Exception(msg){};
};

class RuntimeException : public Exception {
public:
    explicit RuntimeException(const std::string& msg) : Exception("Runtime exception: " + msg){};
};

class ConnectionException : public Exception {
public:
    explicit ConnectionException(const std::string& msg) : Exception(msg){};
};

class TransactionManagerException : public Exception {
public:
    explicit TransactionManagerException(const std::string& msg) : Exception(msg){};
};

class InterruptException : public Exception {
public:
    explicit InterruptException() : Exception("Interrupted."){};
};

class TestException : public Exception {
public:
    explicit TestException(const std::string& msg) : Exception("Test exception: " + msg){};
};

} // namespace common
} // namespace kuzu


#include <cstdint>
#include <string>


namespace kuzu {
namespace common {

// Type used to represent time (microseconds)
KUZU_API struct dtime_t {
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
    KUZU_API static dtime_t FromCString(const char* buf, uint64_t len);
    KUZU_API static bool TryConvertTime(
        const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);

    // Convert a time object to a string in the format "hh:mm:ss"
    KUZU_API static std::string toString(dtime_t time);

    KUZU_API static dtime_t FromTime(
        int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);

    // Extract the time from a given timestamp object
    KUZU_API static void Convert(
        dtime_t time, int32_t& out_hour, int32_t& out_min, int32_t& out_sec, int32_t& out_micros);

    KUZU_API static bool IsValid(
        int32_t hour, int32_t minute, int32_t second, int32_t milliseconds);
};

} // namespace common
} // namespace kuzu



namespace kuzu {
namespace common {

struct timestamp_t;

// System representation of dates as the number of days since 1970-01-01.
KUZU_API struct date_t {
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

    // Convert a string in the format "YYYY-MM-DD" to a date object
    KUZU_API static date_t FromCString(const char* str, uint64_t len);
    // Convert a date object to a string in the format "YYYY-MM-DD"
    KUZU_API static std::string toString(date_t date);
    // Try to convert text in a buffer to a date; returns true if parsing was successful
    KUZU_API static bool TryConvertDate(
        const char* buf, uint64_t len, uint64_t& pos, date_t& result);

    // private:
    // Returns true if (year) is a leap year, and false otherwise
    KUZU_API static bool IsLeapYear(int32_t year);
    // Returns true if the specified (year, month, day) combination is a valid
    // date
    KUZU_API static bool IsValid(int32_t year, int32_t month, int32_t day);
    // Extract the year, month and day from a given date object
    KUZU_API static void Convert(
        date_t date, int32_t& out_year, int32_t& out_month, int32_t& out_day);
    // Create a Date object from a specified (year, month, day) combination
    KUZU_API static date_t FromDate(int32_t year, int32_t month, int32_t day);

    // Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
    KUZU_API static bool ParseDoubleDigit(
        const char* buf, uint64_t len, uint64_t& pos, int32_t& result);

    KUZU_API static int32_t MonthDays(int32_t year, int32_t month);

    KUZU_API static std::string getDayName(date_t& date);

    KUZU_API static std::string getMonthName(date_t& date);

    KUZU_API static date_t getLastDay(date_t& date);

    KUZU_API static int32_t getDatePart(DatePartSpecifier specifier, date_t& date);

    KUZU_API static date_t trunc(DatePartSpecifier specifier, date_t& date);

    KUZU_API static int64_t getEpochNanoSeconds(const date_t& date);

private:
    static void ExtractYearOffset(int32_t& n, int32_t& year, int32_t& year_offset);
};

} // namespace common
} // namespace kuzu


#include <cassert>
#include <cmath>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace kuzu {
namespace common {

using sel_t = uint16_t;
using hash_t = uint64_t;
using page_idx_t = uint32_t;
using frame_idx_t = page_idx_t;
using page_offset_t = uint32_t;
constexpr page_idx_t INVALID_PAGE_IDX = UINT32_MAX;
using page_group_idx_t = uint32_t;
using frame_group_idx_t = page_group_idx_t;
using property_id_t = uint32_t;
constexpr property_id_t INVALID_PROPERTY_ID = UINT32_MAX;
using column_id_t = property_id_t;
constexpr column_id_t INVALID_COLUMN_ID = INVALID_PROPERTY_ID;
using vector_idx_t = uint32_t;
constexpr vector_idx_t INVALID_VECTOR_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
using field_idx_t = uint64_t;
using struct_field_idx_t = uint64_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT64_MAX;
using tuple_idx_t = uint64_t;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
};

struct list_entry_t {
    common::offset_t offset;
    uint64_t size;

    list_entry_t() : offset{INVALID_OFFSET}, size{UINT64_MAX} {}
    list_entry_t(common::offset_t offset, uint64_t size) : offset{offset}, size{size} {}
};

struct struct_entry_t {
    int64_t pos;
};

KUZU_API enum class LogicalTypeID : uint8_t {
    ANY = 0,
    NODE = 10,
    REL = 11,
    RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 13,

    // fixed size types
    BOOL = 22,
    INT64 = 23,
    INT32 = 24,
    INT16 = 25,
    DOUBLE = 26,
    FLOAT = 27,
    DATE = 28,
    TIMESTAMP = 29,
    INTERVAL = 30,
    FIXED_LIST = 31,

    INTERNAL_ID = 40,

    // variable size types
    STRING = 50,
    VAR_LIST = 52,
    STRUCT = 53,
};

enum class PhysicalTypeID : uint8_t {
    // Fixed size types.
    ANY = 0,
    BOOL = 1,
    INT64 = 2,
    INT32 = 3,
    INT16 = 4,
    DOUBLE = 5,
    FLOAT = 6,
    INTERVAL = 7,
    INTERNAL_ID = 9,

    // Variable size types.
    STRING = 20,
    FIXED_LIST = 21,
    VAR_LIST = 22,
    STRUCT = 23,
};

struct PhysicalTypeUtils {
    static std::string physicalTypeToString(PhysicalTypeID physicalType);
};

class LogicalType;

class ExtraTypeInfo {
public:
    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;
    virtual ~ExtraTypeInfo() = default;
};

class VarListTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    VarListTypeInfo() = default;
    explicit VarListTypeInfo(std::unique_ptr<LogicalType> childType)
        : childType{std::move(childType)} {}
    inline LogicalType* getChildType() const { return childType.get(); }
    bool operator==(const VarListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

protected:
    std::unique_ptr<LogicalType> childType;
};

class FixedListTypeInfo : public VarListTypeInfo {
    friend class SerDeser;

public:
    FixedListTypeInfo() = default;
    explicit FixedListTypeInfo(
        std::unique_ptr<LogicalType> childType, uint64_t fixedNumElementsInList)
        : VarListTypeInfo{std::move(childType)}, fixedNumElementsInList{fixedNumElementsInList} {}
    inline uint64_t getNumElementsInList() const { return fixedNumElementsInList; }
    bool operator==(const FixedListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    uint64_t fixedNumElementsInList;
};

class StructField {
    friend class SerDeser;

public:
    StructField() : type{std::make_unique<LogicalType>()} {}
    StructField(std::string name, std::unique_ptr<LogicalType> type);

    inline bool operator!=(const StructField& other) const { return !(*this == other); }
    inline std::string getName() const { return name; }
    inline LogicalType* getType() const { return type.get(); }

    bool operator==(const StructField& other) const;
    std::unique_ptr<StructField> copy() const;

private:
    std::string name;
    std::unique_ptr<LogicalType> type;
};

class StructTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<std::unique_ptr<StructField>> fields);

    struct_field_idx_t getStructFieldIdx(std::string fieldName) const;
    std::vector<LogicalType*> getChildrenTypes() const;
    std::vector<std::string> getChildrenNames() const;
    std::vector<StructField*> getStructFields() const;

    bool operator==(const kuzu::common::StructTypeInfo& other) const;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    std::vector<std::unique_ptr<StructField>> fields;
    std::unordered_map<std::string, struct_field_idx_t> fieldNameToIdxMap;
};

class LogicalType {
    friend class SerDeser;
    friend class LogicalTypeUtils;
    friend class StructType;
    friend class VarListType;
    friend class FixedListType;

public:
    KUZU_API LogicalType() : typeID{LogicalTypeID::ANY}, extraTypeInfo{nullptr} {};
    KUZU_API explicit LogicalType(LogicalTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {
        setPhysicalType();
    };
    KUZU_API LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo)
        : typeID{typeID}, extraTypeInfo{std::move(extraTypeInfo)} {
        setPhysicalType();
    };
    KUZU_API LogicalType(const LogicalType& other);
    KUZU_API LogicalType(LogicalType&& other) noexcept;

    KUZU_API LogicalType& operator=(const LogicalType& other);

    KUZU_API bool operator==(const LogicalType& other) const;

    KUZU_API bool operator!=(const LogicalType& other) const;

    KUZU_API LogicalType& operator=(LogicalType&& other) noexcept;

    KUZU_API inline LogicalTypeID getLogicalTypeID() const { return typeID; }

    inline PhysicalTypeID getPhysicalType() const { return physicalType; }

    std::unique_ptr<LogicalType> copy();

private:
    void setPhysicalType();

private:
    LogicalTypeID typeID;
    PhysicalTypeID physicalType;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
};

struct VarListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
               type->getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL);
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(type->extraTypeInfo.get());
        return varListTypeInfo->getChildType();
    }
};

struct FixedListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getChildType();
    }

    static inline uint64_t getNumElementsInList(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getNumElementsInList();
    }
};

struct StructType {
    static inline std::vector<LogicalType*> getFieldTypes(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenTypes();
    }

    static inline std::vector<std::string> getFieldNames(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenNames();
    }

    static inline uint64_t getNumFields(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::STRUCT);
        return getFieldTypes(type).size();
    }

    static inline std::vector<StructField*> getFields(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFields();
    }

    static inline struct_field_idx_t getFieldIdx(const LogicalType* type, std::string& key) {
        assert(type->getLogicalTypeID() == LogicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFieldIdx(key);
    }
};

class LogicalTypeUtils {
public:
    KUZU_API static std::string dataTypeToString(const LogicalType& dataType);
    KUZU_API static std::string dataTypeToString(LogicalTypeID dataTypeID);
    static std::string dataTypesToString(const std::vector<LogicalType>& dataTypes);
    static std::string dataTypesToString(const std::vector<LogicalTypeID>& dataTypeIDs);
    KUZU_API static LogicalType dataTypeFromString(const std::string& dataTypeString);
    static uint32_t getFixedTypeSize(kuzu::common::PhysicalTypeID physicalType);
    static bool isNumerical(const LogicalType& dataType);
    static std::vector<LogicalType> getAllValidComparableLogicalTypes();
    static std::vector<LogicalTypeID> getNumericalLogicalTypeIDs();
    static std::vector<LogicalTypeID> getAllValidLogicTypeIDs();

private:
    static LogicalTypeID dataTypeIDFromString(const std::string& dataTypeIDString);
    static std::vector<std::string> parseStructFields(const std::string& structTypeStr);
};

enum class DBFileType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu


#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>


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

    double getDuration() {
        if (finished) {
            auto duration = stopTime - startTime;
            return (double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        }
        throw Exception("Timer is still running.");
    }

    int64_t getElapsedTimeInMS() {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = now - startTime;
        return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> stopTime;
    bool finished = false;
};

} // namespace common
} // namespace kuzu



namespace kuzu {
namespace common {

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
KUZU_API struct timestamp_t {
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
    KUZU_API static timestamp_t FromCString(const char* str, uint64_t len);

    // Convert a timestamp object to a std::string in the format "YYYY-MM-DD hh:mm:ss".
    KUZU_API static std::string toString(timestamp_t timestamp);

    KUZU_API static date_t GetDate(timestamp_t timestamp);

    KUZU_API static dtime_t GetTime(timestamp_t timestamp);

    // Create a Timestamp object from a specified (date, time) combination.
    KUZU_API static timestamp_t FromDatetime(date_t date, dtime_t time);

    // Extract the date and time from a given timestamp object.
    KUZU_API static void Convert(timestamp_t timestamp, date_t& out_date, dtime_t& out_time);

    // Create a Timestamp object from the specified epochMs.
    KUZU_API static timestamp_t FromEpochMs(int64_t epochMs);

    // Create a Timestamp object from the specified epochSec.
    KUZU_API static timestamp_t FromEpochSec(int64_t epochSec);

    KUZU_API static int32_t getTimestampPart(DatePartSpecifier specifier, timestamp_t& timestamp);

    KUZU_API static timestamp_t trunc(DatePartSpecifier specifier, timestamp_t& date);

    KUZU_API static int64_t getEpochNanoSeconds(const timestamp_t& timestamp);

    KUZU_API static bool TryParseUTCOffset(
        const char* str, uint64_t& pos, uint64_t len, int& hour_offset, int& minute_offset);

private:
    static std::string getTimestampConversionExceptionMsg(const char* str, uint64_t len) {
        return "Error occurred during parsing timestamp. Given: \"" + std::string(str, len) +
               "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
    }
};

} // namespace common
} // namespace kuzu


#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

struct ku_string_t {

    static const uint64_t PREFIX_LENGTH = 4;
    static const uint64_t INLINED_SUFFIX_LENGTH = 8;
    static const uint64_t SHORT_STR_LENGTH = PREFIX_LENGTH + INLINED_SUFFIX_LENGTH;

    uint32_t len;
    uint8_t prefix[PREFIX_LENGTH];
    union {
        uint8_t data[INLINED_SUFFIX_LENGTH];
        uint64_t overflowPtr;
    };

    ku_string_t() : len{0}, overflowPtr{0} {}

    static bool isShortString(uint32_t len) { return len <= SHORT_STR_LENGTH; }

    inline const uint8_t* getData() const {
        return isShortString(len) ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    // These functions do *NOT* allocate/resize the overflow buffer, it only copies the content and
    // set the length.
    void set(const std::string& value);
    void set(const char* value, uint64_t length);
    void set(const ku_string_t& value);

    std::string getAsShortString() const;
    std::string getAsString() const;

    bool operator==(const ku_string_t& rhs) const;

    inline bool operator!=(const ku_string_t& rhs) const { return !(*this == rhs); }

    bool operator>(const ku_string_t& rhs) const;

    inline bool operator>=(const ku_string_t& rhs) const { return (*this > rhs) || (*this == rhs); }

    inline bool operator<(const ku_string_t& rhs) const { return !(*this >= rhs); }

    inline bool operator<=(const ku_string_t& rhs) const { return !(*this > rhs); }
};

} // namespace common
} // namespace kuzu



namespace kuzu {
namespace common {

struct ku_list_t {

public:
    ku_list_t() : size{0}, overflowPtr{0} {}
    ku_list_t(uint64_t size, uint64_t overflowPtr) : size{size}, overflowPtr{overflowPtr} {}

    void set(const uint8_t* values, const LogicalType& dataType) const;

private:
    friend class InMemOverflowBufferUtils;

    void set(const std::vector<uint8_t*>& parameters, LogicalTypeID childTypeId);

public:
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace kuzu


#include <mutex>


namespace kuzu {
namespace common {

/**
 * Note that metrics are not thread safe.
 */
class Metric {

public:
    explicit Metric(bool enabled) : enabled{enabled} {}

    virtual ~Metric() = default;

public:
    bool enabled;
};

class TimeMetric : public Metric {

public:
    explicit TimeMetric(bool enable);

    void start();
    void stop();

    double getElapsedTimeMS() const;

public:
    double accumulatedTime;
    bool isStarted;
    Timer timer;
};

class NumericMetric : public Metric {

public:
    explicit NumericMetric(bool enable);

    void increase(uint64_t value);

    void incrementByOne();

public:
    uint64_t accumulatedValue;
};

} // namespace common
} // namespace kuzu


#include <cstdint>
#include <string>
#include <unordered_map>

namespace kuzu {
namespace common {

constexpr uint64_t DEFAULT_VECTOR_CAPACITY_LOG_2 = 11;
constexpr uint64_t DEFAULT_VECTOR_CAPACITY = (uint64_t)1 << DEFAULT_VECTOR_CAPACITY_LOG_2;

constexpr double DEFAULT_HT_LOAD_FACTOR = 1.5;
constexpr uint32_t VAR_LENGTH_EXTEND_MAX_DEPTH = 30;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 500;

constexpr uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS = 5000000;

const std::string INTERNAL_ID_SUFFIX = "_id";
const std::string INTERNAL_LENGTH_SUFFIX = "_length";

enum PageSizeClass : uint8_t {
    PAGE_4KB = 0,
    PAGE_256KB = 1,
};

// Currently the system supports files with 2 different pages size, which we refer to as
// PAGE_4KB_SIZE and PAGE_256KB_SIZE. PAGE_4KB_SIZE is the default size of the page which is the
// unit of read/write to the database files, such as to store columns or lists. For now, this value
// cannot be changed. But technically it can change from 2^12 to 2^16. 2^12 lower bound is assuming
// the OS page size is 4K. 2^16 is because currently we leave 11 fixed number of bits for
// relOffInPage and the maximum number of bytes needed for an edge is 20 bytes so 11 + log_2(20)
// = 15.xxx, so certainly over 2^16-size pages, we cannot utilize the page for storing adjacency
// lists.
struct BufferPoolConstants {
    static constexpr uint64_t PAGE_4KB_SIZE_LOG2 = 12;
    static constexpr uint64_t PAGE_4KB_SIZE = (std::uint64_t)1 << PAGE_4KB_SIZE_LOG2;
    // Page size for files with large pages, e.g., temporary files that are used by operators that
    // may require large amounts of memory.
    static constexpr uint64_t PAGE_256KB_SIZE_LOG2 = 18;
    static constexpr uint64_t PAGE_256KB_SIZE = (std::uint64_t)1 << PAGE_256KB_SIZE_LOG2;
    // If a user does not specify a max size for BM, we by default set the max size of BM to
    // maxPhyMemSize * DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM.
    static constexpr double DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM = 0.8;
    // For each PURGE_EVICTION_QUEUE_INTERVAL candidates added to the eviction queue, we will call
    // `removeNonEvictableCandidates` to remove candidates that are not evictable. See
    // `EvictionQueue::removeNonEvictableCandidates()` for more details.
    static constexpr uint64_t EVICTION_QUEUE_PURGING_INTERVAL = 1024;
    // The default max size for a VMRegion.
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 43; // (8TB)

    static constexpr uint64_t DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING = 1ull << 26; // (64MB)
};

struct StorageConstants {
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";
    static constexpr char COLUMN_FILE_SUFFIX[] = ".col";
    static constexpr char LISTS_FILE_SUFFIX[] = ".lists";
    static constexpr char WAL_FILE_SUFFIX[] = ".wal";
    static constexpr char INDEX_FILE_SUFFIX[] = ".hindex";
    static constexpr char NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME[] =
        "nodes.statistics_and_deleted.ids";
    static constexpr char NODES_STATISTICS_FILE_NAME_FOR_WAL[] =
        "nodes.statistics_and_deleted.ids.wal";
    static constexpr char RELS_METADATA_FILE_NAME[] = "rels.statistics";
    static constexpr char RELS_METADATA_FILE_NAME_FOR_WAL[] = "rels.statistics.wal";
    static constexpr char CATALOG_FILE_NAME[] = "catalog.bin";
    static constexpr char CATALOG_FILE_NAME_FOR_WAL[] = "catalog.bin.wal";

    // The number of pages that we add at one time when we need to grow a file.
    static constexpr uint64_t PAGE_GROUP_SIZE_LOG2 = 10;
    static constexpr uint64_t PAGE_GROUP_SIZE = (uint64_t)1 << PAGE_GROUP_SIZE_LOG2;
    static constexpr uint64_t PAGE_IDX_IN_GROUP_MASK = ((uint64_t)1 << PAGE_GROUP_SIZE_LOG2) - 1;
};

struct ListsMetadataConstants {
    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;
    constexpr static uint16_t LISTS_CHUNK_SIZE = 1 << LISTS_CHUNK_SIZE_LOG_2;
    // All pageLists (defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;
    static constexpr uint32_t PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE = PAGE_LIST_GROUP_SIZE + 1;
};

// Hash Index Configurations
struct HashIndexConstants {
    static constexpr uint8_t SLOT_CAPACITY = 3;
};

struct CopyConstants {
    // Size (in bytes) of the chunks to be read in Node/Rel Copier
    static constexpr uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

    // Number of tasks to be assigned in a batch when reading files.
    static constexpr uint64_t NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH = 200;

    // Lower bound for number of incomplete tasks in copier to trigger scheduling a new batch.
    static constexpr uint64_t MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE = 50;

    // Number of rows per block for npy files
    static constexpr uint64_t NUM_ROWS_PER_BLOCK_FOR_NPY = 2048;

    // Default configuration for csv file parsing
    static constexpr const char* STRING_CSV_PARSING_OPTIONS[5] = {
        "ESCAPE", "DELIM", "QUOTE", "LIST_BEGIN", "LIST_END"};
    static constexpr char DEFAULT_CSV_ESCAPE_CHAR = '\\';
    static constexpr char DEFAULT_CSV_DELIMITER = ',';
    static constexpr char DEFAULT_CSV_QUOTE_CHAR = '"';
    static constexpr char DEFAULT_CSV_LIST_BEGIN_CHAR = '[';
    static constexpr char DEFAULT_CSV_LIST_END_CHAR = ']';
    static constexpr bool DEFAULT_CSV_HAS_HEADER = false;
};

struct LoggerConstants {
    enum class LoggerEnum : uint8_t {
        DATABASE = 0,
        CSV_READER = 1,
        LOADER = 2,
        PROCESSOR = 3,
        BUFFER_MANAGER = 4,
        CATALOG = 5,
        STORAGE = 6,
        TRANSACTION_MANAGER = 7,
        WAL = 8,
    };
};

struct PlannerKnobs {
    static constexpr double NON_EQUALITY_PREDICATE_SELECTIVITY = 0.1;
    static constexpr double EQUALITY_PREDICATE_SELECTIVITY = 0.01;
    static constexpr uint64_t BUILD_PENALTY = 2;
    // Avoid doing probe to build SIP if we have to accumulate a probe side that is much bigger than
    // build side. Also avoid doing build to probe SIP if probe side is not much bigger than build.
    static constexpr uint64_t ACC_HJ_PROBE_BUILD_RATIO = 5;
};

struct ClientContextConstants {
    // We disable query timeout by default.
    static constexpr uint64_t TIMEOUT_IN_MS = 0;
};

} // namespace common
} // namespace kuzu



#include <cstdint>

namespace kuzu {

namespace testing {
class ApiTest;
class BaseGraphTest;
class TestHelper;
class TestRunner;
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
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
} // namespace common

namespace storage {
class MemoryManager;
class BufferManager;
class StorageManager;
class WAL;
enum class WALReplayMode : uint8_t;
} // namespace storage

namespace planner {
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
enum class TransactionType : uint8_t;
enum class TransactionAction : uint8_t;
class TransactionManager;
} // namespace transaction

} // namespace kuzu

namespace spdlog {
class logger;
namespace level {
enum level_enum : int;
} // namespace level
} // namespace spdlog

//     __ _____ _____ _____
//  __|  |   __|     |   | |  JSON for Modern C++
// |  |  |__   |  |  | | | |  version 3.11.2
// |_____|_____|_____|_|___|  https://github.com/nlohmann/json
//
// SPDX-FileCopyrightText: 2013-2022 Niels Lohmann <https://nlohmann.me>
// SPDX-License-Identifier: MIT

#ifndef INCLUDE_NLOHMANN_JSON_FWD_HPP_
#define INCLUDE_NLOHMANN_JSON_FWD_HPP_

#include <cstdint> // int64_t, uint64_t
#include <map> // map
#include <memory> // allocator
#include <string> // string
#include <vector> // vector

// #include <nlohmann/detail/abi_macros.hpp>
//     __ _____ _____ _____
//  __|  |   __|     |   | |  JSON for Modern C++
// |  |  |__   |  |  | | | |  version 3.11.2
// |_____|_____|_____|_|___|  https://github.com/nlohmann/json
//
// SPDX-FileCopyrightText: 2013-2022 Niels Lohmann <https://nlohmann.me>
// SPDX-License-Identifier: MIT



// This file contains all macro definitions affecting or depending on the ABI

#ifndef JSON_SKIP_LIBRARY_VERSION_CHECK
    #if defined(NLOHMANN_JSON_VERSION_MAJOR) && defined(NLOHMANN_JSON_VERSION_MINOR) && defined(NLOHMANN_JSON_VERSION_PATCH)
        #if NLOHMANN_JSON_VERSION_MAJOR != 3 || NLOHMANN_JSON_VERSION_MINOR != 11 || NLOHMANN_JSON_VERSION_PATCH != 2
            #warning "Already included a different version of the library!"
        #endif
    #endif
#endif

#define NLOHMANN_JSON_VERSION_MAJOR 3   // NOLINT(modernize-macro-to-enum)
#define NLOHMANN_JSON_VERSION_MINOR 11  // NOLINT(modernize-macro-to-enum)
#define NLOHMANN_JSON_VERSION_PATCH 2   // NOLINT(modernize-macro-to-enum)

#ifndef JSON_DIAGNOSTICS
    #define JSON_DIAGNOSTICS 0
#endif

#ifndef JSON_USE_LEGACY_DISCARDED_VALUE_COMPARISON
    #define JSON_USE_LEGACY_DISCARDED_VALUE_COMPARISON 0
#endif

#if JSON_DIAGNOSTICS
    #define NLOHMANN_JSON_ABI_TAG_DIAGNOSTICS _diag
#else
    #define NLOHMANN_JSON_ABI_TAG_DIAGNOSTICS
#endif

#if JSON_USE_LEGACY_DISCARDED_VALUE_COMPARISON
    #define NLOHMANN_JSON_ABI_TAG_LEGACY_DISCARDED_VALUE_COMPARISON _ldvcmp
#else
    #define NLOHMANN_JSON_ABI_TAG_LEGACY_DISCARDED_VALUE_COMPARISON
#endif

#ifndef NLOHMANN_JSON_NAMESPACE_NO_VERSION
    #define NLOHMANN_JSON_NAMESPACE_NO_VERSION 0
#endif

// Construct the namespace ABI tags component
#define NLOHMANN_JSON_ABI_TAGS_CONCAT_EX(a, b) json_abi ## a ## b
#define NLOHMANN_JSON_ABI_TAGS_CONCAT(a, b) \
    NLOHMANN_JSON_ABI_TAGS_CONCAT_EX(a, b)

#define NLOHMANN_JSON_ABI_TAGS                                       \
    NLOHMANN_JSON_ABI_TAGS_CONCAT(                                   \
            NLOHMANN_JSON_ABI_TAG_DIAGNOSTICS,                       \
            NLOHMANN_JSON_ABI_TAG_LEGACY_DISCARDED_VALUE_COMPARISON)

// Construct the namespace version component
#define NLOHMANN_JSON_NAMESPACE_VERSION_CONCAT_EX(major, minor, patch) \
    _v ## major ## _ ## minor ## _ ## patch
#define NLOHMANN_JSON_NAMESPACE_VERSION_CONCAT(major, minor, patch) \
    NLOHMANN_JSON_NAMESPACE_VERSION_CONCAT_EX(major, minor, patch)

#if NLOHMANN_JSON_NAMESPACE_NO_VERSION
#define NLOHMANN_JSON_NAMESPACE_VERSION
#else
#define NLOHMANN_JSON_NAMESPACE_VERSION                                 \
    NLOHMANN_JSON_NAMESPACE_VERSION_CONCAT(NLOHMANN_JSON_VERSION_MAJOR, \
                                           NLOHMANN_JSON_VERSION_MINOR, \
                                           NLOHMANN_JSON_VERSION_PATCH)
#endif

// Combine namespace components
#define NLOHMANN_JSON_NAMESPACE_CONCAT_EX(a, b) a ## b
#define NLOHMANN_JSON_NAMESPACE_CONCAT(a, b) \
    NLOHMANN_JSON_NAMESPACE_CONCAT_EX(a, b)

#ifndef NLOHMANN_JSON_NAMESPACE
#define NLOHMANN_JSON_NAMESPACE               \
    nlohmann::NLOHMANN_JSON_NAMESPACE_CONCAT( \
            NLOHMANN_JSON_ABI_TAGS,           \
            NLOHMANN_JSON_NAMESPACE_VERSION)
#endif

#ifndef NLOHMANN_JSON_NAMESPACE_BEGIN
#define NLOHMANN_JSON_NAMESPACE_BEGIN                \
    namespace nlohmann                               \
    {                                                \
    inline namespace NLOHMANN_JSON_NAMESPACE_CONCAT( \
                NLOHMANN_JSON_ABI_TAGS,              \
                NLOHMANN_JSON_NAMESPACE_VERSION)     \
    {
#endif

#ifndef NLOHMANN_JSON_NAMESPACE_END
#define NLOHMANN_JSON_NAMESPACE_END                                     \
    }  /* namespace (inline namespace) NOLINT(readability/namespace) */ \
    }  // namespace nlohmann
#endif


/*!
@brief namespace for Niels Lohmann
@see https://github.com/nlohmann
@since version 1.0.0
*/
NLOHMANN_JSON_NAMESPACE_BEGIN

/*!
@brief default JSONSerializer template argument

This serializer ignores the template arguments and uses ADL
([argument-dependent lookup](https://en.cppreference.com/w/cpp/language/adl))
for serialization.
*/
template<typename T = void, typename SFINAE = void>
struct adl_serializer;

/// a class to store JSON values
/// @sa https://json.nlohmann.me/api/basic_json/
template<template<typename U, typename V, typename... Args> class ObjectType =
         std::map,
         template<typename U, typename... Args> class ArrayType = std::vector,
         class StringType = std::string, class BooleanType = bool,
         class NumberIntegerType = std::int64_t,
         class NumberUnsignedType = std::uint64_t,
         class NumberFloatType = double,
         template<typename U> class AllocatorType = std::allocator,
         template<typename T, typename SFINAE = void> class JSONSerializer =
         adl_serializer,
         class BinaryType = std::vector<std::uint8_t>>
class basic_json;

/// @brief JSON Pointer defines a string syntax for identifying a specific value within a JSON document
/// @sa https://json.nlohmann.me/api/json_pointer/
template<typename RefStringType>
class json_pointer;

/*!
@brief default specialization
@sa https://json.nlohmann.me/api/json/
*/
using json = basic_json<>;

/// @brief a minimal map-like container that preserves insertion order
/// @sa https://json.nlohmann.me/api/ordered_map/
template<class Key, class T, class IgnoredLess, class Allocator>
struct ordered_map;

/// @brief specialization that maintains the insertion order of object keys
/// @sa https://json.nlohmann.me/api/ordered_json/
using ordered_json = basic_json<nlohmann::ordered_map>;

NLOHMANN_JSON_NAMESPACE_END

#endif  // INCLUDE_NLOHMANN_JSON_FWD_HPP_


#include <memory>
#include <unordered_map>


namespace kuzu {
namespace common {

class Profiler {

public:
    TimeMetric* registerTimeMetric(const std::string& key);

    NumericMetric* registerNumericMetric(const std::string& key);

    double sumAllTimeMetricsWithKey(const std::string& key);

    uint64_t sumAllNumericMetricsWithKey(const std::string& key);

private:
    void addMetric(const std::string& key, std::unique_ptr<Metric> metric);

public:
    std::mutex mtx;
    bool enabled;
    std::unordered_map<std::string, std::vector<std::unique_ptr<Metric>>> metrics;
};

} // namespace common
} // namespace kuzu


#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>


namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

class LoggerUtils {
public:
    // Note: create logger is not thread safe.
    static void createLogger(LoggerConstants::LoggerEnum loggerEnum);
    static std::shared_ptr<spdlog::logger> getLogger(LoggerConstants::LoggerEnum loggerEnum);
    static void dropLogger(LoggerConstants::LoggerEnum loggerEnum);

private:
    static std::string getLoggerName(LoggerConstants::LoggerEnum loggerEnum);
};

template<typename FROM, typename TO>
std::unique_ptr<TO> ku_static_unique_pointer_cast(std::unique_ptr<FROM>&& old) {
    return std::unique_ptr<TO>{static_cast<TO*>(old.release())};
}
template<class FROM, class TO>
std::shared_ptr<TO> ku_reinterpret_pointer_cast(const std::shared_ptr<FROM>& r) {
    return std::shared_ptr<TO>(
        r, reinterpret_cast<typename std::shared_ptr<TO>::element_type*>(r.get()));
}

class BitmaskUtils {

public:
    static inline uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        assert(numBits <= 64);
        return numBits == 64 ? UINT64_MAX : ((uint64_t)1 << numBits) - 1;
    }
};

static uint64_t nextPowerOfTwo(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

static bool isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

} // namespace common
} // namespace kuzu


#include <sstream>


namespace kuzu {
namespace common {

class TypeUtils {

public:
    static uint32_t convertToUint32(const char* data);
    static bool convertToBoolean(const char* data);

    static inline std::string toString(bool boolVal) { return boolVal ? "True" : "False"; }
    static inline std::string toString(int64_t val) { return std::to_string(val); }
    static inline std::string toString(int32_t val) { return std::to_string(val); }
    static inline std::string toString(int16_t val) { return std::to_string(val); }
    static inline std::string toString(double_t val) { return std::to_string(val); }
    static inline std::string toString(float_t val) { return std::to_string(val); }
    static inline std::string toString(const internalID_t& val) {
        return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
    }
    static inline std::string toString(const date_t& val) { return Date::toString(val); }
    static inline std::string toString(const timestamp_t& val) { return Timestamp::toString(val); }
    static inline std::string toString(const interval_t& val) { return Interval::toString(val); }
    static inline std::string toString(const ku_string_t& val) { return val.getAsString(); }
    static inline std::string toString(const std::string& val) { return val; }
    static std::string toString(const list_entry_t& val, void* valueVector);
    static std::string toString(const struct_entry_t& val, void* valueVector);

    static inline void encodeOverflowPtr(
        uint64_t& overflowPtr, page_idx_t pageIdx, uint16_t pageOffset) {
        memcpy(&overflowPtr, &pageIdx, 4);
        memcpy(((uint8_t*)&overflowPtr) + 4, &pageOffset, 2);
    }
    static inline void decodeOverflowPtr(
        uint64_t overflowPtr, page_idx_t& pageIdx, uint16_t& pageOffset) {
        pageIdx = 0;
        memcpy(&pageIdx, &overflowPtr, 4);
        memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 4, 2);
    }

    template<typename T>
    static inline bool isValueEqual(T& left, T& right, void* leftVector, void* rightVector) {
        return left == right;
    }

    template<typename T>
    static T convertStringToNumber(const char* data) {
        std::istringstream iss{data};
        if (iss.str().empty()) {
            throw ConversionException{"Empty string."};
        }
        T retVal;
        iss >> retVal;
        if (iss.fail() || !iss.eof()) {
            throw ConversionException{"Invalid number: " + std::string{data} + "."};
        }
        return retVal;
    }

private:
    static std::string castValueToString(const LogicalType& dataType, uint8_t* value, void* vector);

    static std::string prefixConversionExceptionMessage(const char* data, LogicalTypeID dataTypeID);
};

template<>
bool TypeUtils::isValueEqual(
    list_entry_t& left, list_entry_t& right, void* leftVector, void* rightVector);

} // namespace common
} // namespace kuzu


#include <cassert>
#include <fstream>
#include <sstream>
#include <string>


namespace kuzu {
namespace main {

class OpProfileBox {
public:
    OpProfileBox(std::string opName, std::string paramsName, std::vector<std::string> attributes);

    inline std::string getOpName() const { return opName; }

    inline uint32_t getNumParams() const { return paramsNames.size(); }

    std::string getParamsName(uint32_t idx) const;

    std::string getAttribute(uint32_t idx) const;

    inline uint32_t getNumAttributes() const { return attributes.size(); }

    uint32_t getAttributeMaxLen() const;

private:
    std::string opName;
    std::vector<std::string> paramsNames;
    std::vector<std::string> attributes;
};

class OpProfileTree {
public:
    OpProfileTree(processor::PhysicalOperator* opProfileBoxes, common::Profiler& profiler);

    std::ostringstream printPlanToOstream() const;

private:
    static void calculateNumRowsAndColsForOp(
        processor::PhysicalOperator* op, uint32_t& numRows, uint32_t& numCols);

    uint32_t fillOpProfileBoxes(processor::PhysicalOperator* op, uint32_t rowIdx, uint32_t colIdx,
        uint32_t& maxFieldWidth, common::Profiler& profiler);

    void printOpProfileBoxUpperFrame(uint32_t rowIdx, std::ostringstream& oss) const;

    void printOpProfileBoxes(uint32_t rowIdx, std::ostringstream& oss) const;

    void printOpProfileBoxLowerFrame(uint32_t rowIdx, std::ostringstream& oss) const;

    void prettyPrintPlanTitle(std::ostringstream& oss) const;

    static std::string genHorizLine(uint32_t len);

    inline void validateRowIdxAndColIdx(uint32_t rowIdx, uint32_t colIdx) const {
        assert(0 <= rowIdx && rowIdx < opProfileBoxes.size() && 0 <= colIdx &&
               colIdx < opProfileBoxes[rowIdx].size());
    }

    void insertOpProfileBox(
        uint32_t rowIdx, uint32_t colIdx, std::unique_ptr<OpProfileBox> opProfileBox);

    OpProfileBox* getOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const;

    bool hasOpProfileBox(uint32_t rowIdx, uint32_t colIdx) const {
        return 0 <= rowIdx && rowIdx < opProfileBoxes.size() && 0 <= colIdx &&
               colIdx < opProfileBoxes[rowIdx].size() && getOpProfileBox(rowIdx, colIdx);
    }

    //! Returns true if there is a valid OpProfileBox on the upper left side of the OpProfileBox
    //! located at (rowIdx, colIdx).
    bool hasOpProfileBoxOnUpperLeft(uint32_t rowIdx, uint32_t colIdx) const;

    uint32_t calculateRowHeight(uint32_t rowIdx) const;

private:
    std::vector<std::vector<std::unique_ptr<OpProfileBox>>> opProfileBoxes;
    uint32_t opProfileBoxWidth;
    static constexpr uint32_t INDENT_WIDTH = 3u;
    static constexpr uint32_t BOX_FRAME_WIDTH = 1u;
};

class PlanPrinter {

public:
    PlanPrinter(processor::PhysicalPlan* physicalPlan, std::unique_ptr<common::Profiler> profiler);

    nlohmann::json printPlanToJson();

    std::ostringstream printPlanToOstream();

    static inline std::string getOperatorName(processor::PhysicalOperator* physicalOperator);

    static inline std::string getOperatorParams(processor::PhysicalOperator* physicalOperator);

private:
    nlohmann::json toJson(
        processor::PhysicalOperator* physicalOperator, common::Profiler& profiler);

private:
    processor::PhysicalPlan* physicalPlan;
    std::unique_ptr<common::Profiler> profiler;
};

} // namespace main
} // namespace kuzu



namespace kuzu {
namespace common {

class NodeVal;
class RelVal;

class Value {
public:
    /**
     * @return a NULL value of ANY type.
     */
    KUZU_API static Value createNullValue();
    /**
     * @param dataType the type of the NULL value.
     * @return a NULL value of the given type.
     */
    KUZU_API static Value createNullValue(LogicalType dataType);
    /**
     * @param dataType the type of the non-NULL value.
     * @return a default non-NULL value of the given type.
     */
    KUZU_API static Value createDefaultValue(const LogicalType& dataType);
    /**
     * @param val_ the boolean value to set.
     * @return a Value with BOOL type and val_ value.
     */
    KUZU_API explicit Value(bool val_);
    /**
     * @param val_ the int16_t value to set.
     * @return a Value with INT16 type and val_ value.
     */
    KUZU_API explicit Value(int16_t val_);
    /**
     * @param val_ the int32_t value to set.
     * @return a Value with INT32 type and val_ value.
     */
    KUZU_API explicit Value(int32_t val_);
    /**
     * @param val_ the int64_t value to set.
     * @return a Value with INT64 type and val_ value.
     */
    KUZU_API explicit Value(int64_t val_);
    /**
     * @param val_ the double value to set.
     * @return a Value with DOUBLE type and val_ value.
     */
    KUZU_API explicit Value(double val_);
    /**
     * @param val_ the float value to set.
     * @return a Value with FLOAT type and val_ value.
     */
    KUZU_API explicit Value(float_t val_);
    /**
     * @param val_ the date value to set.
     * @return a Value with DATE type and val_ value.
     */
    KUZU_API explicit Value(date_t val_);
    /**
     * @param val_ the timestamp value to set.
     * @return a Value with TIMESTAMP type and val_ value.
     */
    KUZU_API explicit Value(timestamp_t val_);
    /**
     * @param val_ the interval value to set.
     * @return a Value with INTERVAL type and val_ value.
     */
    KUZU_API explicit Value(interval_t val_);
    /**
     * @param val_ the internalID value to set.
     * @return a Value with INTERNAL_ID type and val_ value.
     */
    KUZU_API explicit Value(internalID_t val_);
    /**
     * @param val_ the string value to set.
     * @return a Value with STRING type and val_ value.
     */
    KUZU_API explicit Value(const char* val_);
    /**
     * @param val_ the string value to set.
     * @return a Value with STRING type and val_ value.
     */
    KUZU_API explicit Value(const std::string& val_);
    /**
     * @param vals the list value to set.
     * @return a Value with dataType type and vals value.
     */
    KUZU_API explicit Value(LogicalType dataType, std::vector<std::unique_ptr<Value>> vals);
    /**
     * @param val_ the node value to set.
     * @return a Value with NODE type and val_ value.
     */
    KUZU_API explicit Value(std::unique_ptr<NodeVal> val_);
    /**
     * @param val_ the rel value to set.
     * @return a Value with REL type and val_ value.
     */
    KUZU_API explicit Value(std::unique_ptr<RelVal> val_);
    /**
     * @param val_ the value to set.
     * @return a Value with dataType type and val_ value.
     */
    KUZU_API explicit Value(LogicalType dataType, const uint8_t* val_);
    /**
     * @param other the value to copy from.
     * @return a Value with the same value as other.
     */
    KUZU_API Value(const Value& other);
    /**
     * @brief Sets the data type of the Value.
     * @param dataType_ the data type to set to.
     */
    KUZU_API void setDataType(const LogicalType& dataType_);
    /**
     * @return the dataType of the value.
     */
    KUZU_API LogicalType getDataType() const;
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
     * @brief Copies from the value.
     * @param value value to copy from.
     */
    KUZU_API void copyValueFrom(const uint8_t* value);
    /**
     * @brief Copies from the other.
     * @param other value to copy from.
     */
    KUZU_API void copyValueFrom(const Value& other);
    /**
     * @return the value of the given type.
     */
    KUZU_API template<class T>
    T getValue() const {
        throw std::runtime_error("Unimplemented template for Value::getValue()");
    }
    /**
     * @return a reference to the value of the given type.
     */
    KUZU_API template<class T>
    T& getValueReference() {
        throw std::runtime_error("Unimplemented template for Value::getValueReference()");
    }
    /**
     * @return a reference to the list value.
     */
    // TODO(Guodong): think how can we template list get functions.
    KUZU_API const std::vector<std::unique_ptr<Value>>& getListValReference() const;
    /**
     * @param value the value to Value object.
     * @return a Value object based on value.
     */
    KUZU_API template<class T>
    static Value createValue(T value) {
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

private:
    Value();
    explicit Value(LogicalType dataType);

    template<typename T>
    static inline void putValuesIntoVector(std::vector<std::unique_ptr<Value>>& fixedListResultVal,
        const uint8_t* fixedList, uint64_t numBytesPerElement) {
        for (auto i = 0; i < fixedListResultVal.size(); ++i) {
            fixedListResultVal[i] =
                std::make_unique<Value>(*(T*)(fixedList + i * numBytesPerElement));
        }
    }

    std::vector<std::unique_ptr<Value>> convertKUVarListToVector(
        ku_list_t& list, const LogicalType& childType) const;
    std::vector<std::unique_ptr<Value>> convertKUFixedListToVector(const uint8_t* fixedList) const;
    std::vector<std::unique_ptr<Value>> convertKUStructToVector(const uint8_t* kuStruct) const;

public:
    LogicalType dataType;
    bool isNull_;

    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        int32_t int32Val;
        int16_t int16Val;
        double doubleVal;
        float floatVal;
        interval_t intervalVal;
        internalID_t internalIDVal;
    } val;
    std::string strVal;
    std::vector<std::unique_ptr<Value>> nestedTypeVal;
    // TODO(Ziyi): remove these two fields once we implemented node/rel using struct.
    std::unique_ptr<NodeVal> nodeVal;
    std::unique_ptr<RelVal> relVal;
};

/**
 * @brief NodeVal represents a node in the graph and stores the nodeID, label and properties of that
 * node.
 */
class NodeVal {
public:
    /**
     * @brief Constructs the NodeVal object with the given idVal and labelVal.
     * @param idVal the nodeID value.
     * @param labelVal the name of the node.
     */
    KUZU_API NodeVal(std::unique_ptr<Value> idVal, std::unique_ptr<Value> labelVal);
    /**
     * @brief Constructs the NodeVal object from the other.
     * @param other the NodeVal to copy from.
     */
    KUZU_API NodeVal(const NodeVal& other);
    /**
     * @brief Adds a property with the given {key,value} pair to the NodeVal.
     * @param key the name of the property.
     * @param value the value of the property.
     */
    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);
    /**
     * @return all properties of the NodeVal.
     */
    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;
    /**
     * @return the nodeID as a Value.
     */
    KUZU_API Value* getNodeIDVal();
    /**
     * @return the name of the node as a Value.
     */
    KUZU_API Value* getLabelVal();
    /**
     * @return the nodeID of the node as a nodeID struct.
     */
    KUZU_API nodeID_t getNodeID() const;
    /**
     * @return the name of the node in string format.
     */
    KUZU_API std::string getLabelName() const;
    /**
     * @return a copy of the current node.
     */
    KUZU_API std::unique_ptr<NodeVal> copy() const;
    /**
     * @return the current node values in string format.
     */
    KUZU_API std::string toString() const;

private:
    std::unique_ptr<Value> idVal;
    std::unique_ptr<Value> labelVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

/**
 * @brief RelVal represents a rel in the graph and stores the relID, src/dst nodes and properties of
 * that rel.
 */
class RelVal {
public:
    /**
     * @brief Constructs the RelVal based on the srcNodeIDVal, dstNodeIDVal and labelVal.
     * @param srcNodeIDVal the src node.
     * @param dstNodeIDVal the dst node.
     * @param labelVal the name of the rel.
     */
    KUZU_API RelVal(std::unique_ptr<Value> srcNodeIDVal, std::unique_ptr<Value> dstNodeIDVal,
        std::unique_ptr<Value> labelVal);
    /**
     * @brief Constructs a RelVal from other.
     * @param other the RelVal to copy from.
     */
    KUZU_API RelVal(const RelVal& other);
    /**
     * @brief Adds a property with the given {key,value} pair to the RelVal.
     * @param key the name of the property.
     * @param value the value of the property.
     */
    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);
    /**
     * @return all properties of the RelVal.
     */
    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;
    /**
     * @return the src nodeID value of the RelVal in Value.
     */
    KUZU_API Value* getSrcNodeIDVal();
    /**
     * @return the dst nodeID value of the RelVal in Value.
     */
    KUZU_API Value* getDstNodeIDVal();
    /**
     * @return the src nodeID value of the RelVal as nodeID struct.
     */
    KUZU_API nodeID_t getSrcNodeID() const;
    /**
     * @return the dst nodeID value of the RelVal as nodeID struct.
     */
    KUZU_API nodeID_t getDstNodeID() const;
    /**
     * @return the name of the RelVal.
     */
    KUZU_API std::string getLabelName();
    /**
     * @return the value of the RelVal in string format.
     */
    KUZU_API std::string toString() const;
    /**
     * @return a copy of the RelVal.
     */
    KUZU_API inline std::unique_ptr<RelVal> copy() const;

private:
    std::unique_ptr<Value> labelVal;
    std::unique_ptr<Value> srcNodeIDVal;
    std::unique_ptr<Value> dstNodeIDVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

/**
 * @return boolean value.
 */
KUZU_API template<>
inline bool Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return int16 value.
 */
KUZU_API template<>
inline int16_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return int32 value.
 */
KUZU_API template<>
inline int32_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return int64 value.
 */
KUZU_API template<>
inline int64_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return float value.
 */
KUZU_API template<>
inline float Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return double value.
 */
KUZU_API template<>
inline double Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return date_t value.
 */
KUZU_API template<>
inline date_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return date_t{val.int32Val};
}

/**
 * @return timestamp_t value.
 */
KUZU_API template<>
inline timestamp_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return timestamp_t{val.int64Val};
}

/**
 * @return interval_t value.
 */
KUZU_API template<>
inline interval_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return internal_t value.
 */
KUZU_API template<>
inline internalID_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return string value.
 */
KUZU_API template<>
inline std::string Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return NodeVal value.
 */
KUZU_API template<>
inline NodeVal Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::NODE);
    return *nodeVal;
}

/**
 * @return RelVal value.
 */
KUZU_API template<>
inline RelVal Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::REL);
    return *relVal;
}

/**
 * @return the reference to the boolean value.
 */
KUZU_API template<>
inline bool& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return the reference to the int16 value.
 */
KUZU_API template<>
inline int16_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return the reference to the int32 value.
 */
KUZU_API template<>
inline int32_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return the reference to the int64 value.
 */
KUZU_API template<>
inline int64_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return the reference to the float value.
 */
KUZU_API template<>
inline float_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return the reference to the double value.
 */
KUZU_API template<>
inline double_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return the reference to the date value.
 */
KUZU_API template<>
inline date_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return *reinterpret_cast<date_t*>(&val.int32Val);
}

/**
 * @return the reference to the timestamp value.
 */
KUZU_API template<>
inline timestamp_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return *reinterpret_cast<timestamp_t*>(&val.int64Val);
}

/**
 * @return the reference to the interval value.
 */
KUZU_API template<>
inline interval_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return the reference to the internal_id value.
 */
KUZU_API template<>
inline nodeID_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return the reference to the string value.
 */
KUZU_API template<>
inline std::string& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return the reference to the NodeVal value.
 */
KUZU_API template<>
inline NodeVal& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::NODE);
    return *nodeVal;
}

/**
 * @return the reference to the RelVal value.
 */
KUZU_API template<>
inline RelVal& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::REL);
    return *relVal;
}

/**
 * @param val the boolean value
 * @return a Value with BOOL type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(bool val) {
    return Value(val);
}

/**
 * @param val the int16 value
 * @return a Value with INT16 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int16_t val) {
    return Value(val);
}

/**
 * @param val the int32 value
 * @return a Value with INT32 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int32_t val) {
    return Value(val);
}

/**
 * @param val the int64 value
 * @return a Value with INT64 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int64_t val) {
    return Value(val);
}

/**
 * @param val the double value
 * @return a Value with DOUBLE type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(double val) {
    return Value(val);
}

/**
 * @param val the date_t value
 * @return a Value with DATE type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(date_t val) {
    return Value(val);
}

/**
 * @param val the timestamp_t value
 * @return a Value with TIMESTAMP type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(timestamp_t val) {
    return Value(val);
}

/**
 * @param val the interval_t value
 * @return a Value with INTERVAL type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(interval_t val) {
    return Value(val);
}

/**
 * @param val the nodeID_t value
 * @return a Value with NODE_ID type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(nodeID_t val) {
    return Value(val);
}

/**
 * @param val the string value
 * @return a Value with STRING type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(std::string val) {
    return Value(val);
}

/**
 * @param val the string value
 * @return a Value with STRING type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(const std::string& val) {
    return Value(val);
}

/**
 * @param val the string value
 * @return a Value with STRING type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(const char* value) {
    return Value(std::string(value));
}

} // namespace common
} // namespace kuzu



namespace kuzu {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a query.
 */
struct PreparedSummary {
    double compilingTime = 0;
    bool isExplain = false;
    bool isProfile = false;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
class QuerySummary {
    friend class Connection;
    friend class benchmark::Benchmark;

public:
    /**
     * @return query compiling time.
     */
    KUZU_API double getCompilingTime() const;
    /**
     * @return query execution time.
     */
    KUZU_API double getExecutionTime() const;
    bool getIsExplain() const;
    bool getIsProfile() const;
    std::ostringstream& getPlanAsOstream();
    /**
     * @return physical plan for query in string format.
     */
    KUZU_API std::string getPlan();
    void setPreparedSummary(PreparedSummary preparedSummary_);

private:
    nlohmann::json& printPlanToJson();

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
    std::unique_ptr<nlohmann::json> planInJson;
    std::ostringstream planInOstream;
};

} // namespace main
} // namespace kuzu



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
    KUZU_API uint32_t len();

    /**
     * @param idx value index to get.
     * @return the value stored at idx.
     */
    KUZU_API common::Value* getValue(uint32_t idx);

    std::string toString();

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



namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(common::LogicalTypeID typeID, std::string name)
        : typeID{typeID}, name{std::move(name)} {}

    common::LogicalTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const common::LogicalType& type, const std::string& name);
};

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
    friend class Connection;

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
    KUZU_API std::vector<std::string> getColumnNames();
    /**
     * @return dataType of each column in query result.
     */
    KUZU_API std::vector<common::LogicalType> getColumnDataTypes();
    /**
     * @return num of tuples in query result.
     */
    KUZU_API uint64_t getNumTuples();
    /**
     * @return query summary which stores the execution time, compiling time, plan and query
     * options.
     */
    KUZU_API QuerySummary* getQuerySummary() const;

    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo();
    /**
     * @return whether there are more tuples to read.
     */
    KUZU_API bool hasNext();
    /**
     * @return next flat tuple in the query result.
     */
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();

    std::string toString();
    /**
     * @brief writes the query result to a csv file.
     * @param fileName name of the csv file.
     * @param delimiter delimiter of the csv file.
     * @param escapeCharacter escape character of the csv file.
     * @param newline newline character of the csv file.
     */
    KUZU_API void writeToCSV(const std::string& fileName, char delimiter = ',',
        char escapeCharacter = '"', char newline = '\n');
    /**
     * @brief Resets the result tuple iterator.
     */
    KUZU_API void resetIterator();

    processor::FactorizedTable* getTable() { return factorizedTable.get(); }

private:
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const std::vector<std::shared_ptr<binder::Expression>>& columns,
        const std::vector<std::vector<std::shared_ptr<binder::Expression>>>&
            expressionToCollectPerColumn);
    void validateQuerySucceed();

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
};

} // namespace main
} // namespace kuzu



namespace kuzu {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning the same query for
 * repeated execution.
 */
class PreparedStatement {
    friend class Connection;
    friend class testing::TestHelper;
    friend class testing::TestRunner;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;

public:
    /**
     * @brief DDL and COPY statements are automatically wrapped in a transaction and committed.
     * As such, they cannot be part of an active transaction.
     * @return the prepared statement is allowed to be part of an active transaction.
     */
    KUZU_API bool allowActiveTransaction() const;
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

    std::vector<std::shared_ptr<binder::Expression>> getExpressionsToCollect();

    inline std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return parameterMap;
    }

private:
    common::StatementType statementType;
    bool success = true;
    bool readOnly = false;
    std::string errMsg;
    PreparedSummary preparedSummary;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
    std::unique_ptr<binder::BoundStatementResult> statementResult;
    std::vector<std::unique_ptr<planner::LogicalPlan>> logicalPlans;
};

} // namespace main
} // namespace kuzu


#include <memory>
#include <thread>


namespace kuzu {
namespace main {

/**
 * @brief Stores buffer pool size and max number of threads configurations.
 */
KUZU_API struct SystemConfig {
    /**
     * @brief Creates a SystemConfig object with default buffer pool size and max num of threads.
     */
    explicit SystemConfig();
    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Max size of the buffer pool in bytes.
     */
    explicit SystemConfig(uint64_t bufferPoolSize);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
};

/**
 * @brief Database class is the main class of KùzuDB. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class StorageDriver;
    friend class kuzu::testing::BaseGraphTest;

public:
    /**
     * @brief Creates a database object with default buffer pool size and max num threads.
     * @param databaseConfig Database path.
     */
    KUZU_API explicit Database(std::string databasePath);
    /**
     * @brief Creates a database object.
     * @param databasePath Database path.
     * @param systemConfig System configurations (buffer pool size and max num threads).
     */
    KUZU_API Database(std::string databasePath, SystemConfig systemConfig);
    /**
     * @brief Destructs the database object.
     */
    KUZU_API ~Database();

    /**
     * @brief Sets the logging level of the database instance.
     * @param loggingLevel New logging level. (Supported logging levels are: "info", "debug",
     * "err").
     */
    static void setLoggingLevel(std::string loggingLevel);

private:
    void initDBDirAndCoreFilesIfNecessary() const;
    static void initLoggers();
    static void dropLoggers();

    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commit(transaction::Transaction* transaction, bool skipCheckpointForTestingRecovery);
    void rollback(transaction::Transaction* transaction, bool skipCheckpointForTestingRecovery);
    void checkpointAndClearWAL(storage::WALReplayMode walReplayMode);
    void rollbackAndClearWAL();
    void recoverIfNecessary();

private:
    std::string databasePath;
    SystemConfig systemConfig;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<storage::WAL> wal;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace main
} // namespace kuzu


#include <atomic>
#include <cstdint>
#include <memory>


namespace kuzu {
namespace main {

struct ActiveQuery {
    explicit ActiveQuery();

    std::atomic<bool> interrupted;
    common::Timer timer;
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;

public:
    explicit ClientContext();

    ~ClientContext() = default;

    inline void interrupt() { activeQuery->interrupted = true; }

    bool isInterrupted() const { return activeQuery->interrupted; }

    inline bool isTimeOut() { return activeQuery->timer.getElapsedTimeInMS() > timeoutInMS; }

    inline bool isTimeOutEnabled() const { return timeoutInMS != 0; }

    void startTimingIfEnabled();

private:
    uint64_t numThreadsForExecution;
    std::unique_ptr<ActiveQuery> activeQuery;
    uint64_t timeoutInMS;
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
    friend class kuzu::testing::ApiTest;
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::testing::TestRunner;
    friend class kuzu::benchmark::Benchmark;

public:
    /**
     * If the connection is in AUTO_COMMIT mode any query over the connection will be wrapped around
     * a transaction and committed (even if the query is READ_ONLY).
     * If the connection is in MANUAL transaction mode, which happens only if an application
     * manually begins a transaction (see below), then an application has to manually commit or
     * rollback the transaction by calling commit() or rollback().
     *
     * AUTO_COMMIT is the default mode when a Connection is created. If an application calls
     * begin[ReadOnly/Write]Transaction at any point, the mode switches to MANUAL. This creates
     * an "active transaction" in the connection. When a connection is in MANUAL mode and the
     * active transaction is rolled back or committed, then the active transaction is removed (so
     * the connection no longer has an active transaction) and the mode automatically switches
     * back to AUTO_COMMIT.
     * Note: When a Connection object is deconstructed, if the connection has an active (manual)
     * transaction, then the active transaction is rolled back.
     */
    enum class ConnectionTransactionMode : uint8_t { AUTO_COMMIT = 0, MANUAL = 1 };

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
     * @brief Manually starts a new read-only transaction in the current connection.
     */
    KUZU_API void beginReadOnlyTransaction();
    /**
     * @brief Manually starts a new write transaction in the current connection.
     */
    KUZU_API void beginWriteTransaction();
    /**
     * @brief Manually commits the current transaction.
     */
    KUZU_API void commit();
    /**
     * @brief Manually rollbacks the current transaction.
     */
    KUZU_API void rollback();
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
    KUZU_API std::unique_ptr<QueryResult> query(const std::string& query);
    /**
     * @brief Prepares the given query and returns the prepared statement.
     * @param query The query to prepare.
     * @return the prepared statement.
     */
    KUZU_API std::unique_ptr<PreparedStatement> prepare(const std::string& query);
    /**
     * @brief Executes the given prepared statement with args and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param args The parameter pack where each arg is a std::pair with the first element being
     * parameter name and second element being parameter value.
     * @return the result of the query.
     */
    KUZU_API template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, std::pair<std::string, Args>... args) {
        std::unordered_map<std::string, std::shared_ptr<common::Value>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }
    /**
     * @brief Executes the given prepared statement with inputParams and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param inputParams The parameter pack where each arg is a std::pair with the first element
     * being parameter name and second element being parameter value.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);
    /**
     * @return all node table names in string format.
     */
    KUZU_API std::string getNodeTableNames();
    /**
     * @return all rel table names in string format.
     */
    KUZU_API std::string getRelTableNames();
    /**
     * @param nodeTableName The name of the node table.
     * @return all property names of the given table.
     */
    KUZU_API std::string getNodePropertyNames(const std::string& tableName);
    /**
     * @param relTableName The name of the rel table.
     * @return all property names of the given table.
     */
    KUZU_API std::string getRelPropertyNames(const std::string& relTableName);

    /**
     * @brief interrupts all queries currently executed within this connection.
     */
    KUZU_API void interrupt();

    /**
     * @brief sets the query timeout value of the current connection. A value of zero (the default)
     * disables the timeout.
     */
    KUZU_API void setQueryTimeOut(uint64_t timeoutInMS);

protected:
    ConnectionTransactionMode getTransactionMode();
    void setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode);

    std::unique_ptr<QueryResult> query(const std::string& query, const std::string& encodedJoin);
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void commitButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void rollbackButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    transaction::Transaction* getActiveTransaction();
    // used in API test

    uint64_t getActiveTransactionID();
    bool hasActiveTransaction();
    void commitNoLock();
    void rollbackIfNecessaryNoLock();

    void beginTransactionNoLock(transaction::TransactionType type);

    void commitOrRollbackNoLock(
        transaction::TransactionAction action, bool skipCheckpointForTesting = false);

    std::unique_ptr<QueryResult> queryResultWithError(std::string& errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(const std::string& query,
        bool enumerateAllPlans = false, std::string joinOrder = std::string{});

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_shared<common::Value>((T)arg.second);
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    void beginTransactionIfAutoCommit(PreparedStatement* preparedStatement);

private:
    inline std::unique_ptr<QueryResult> getQueryResultWithError(std::string exceptionMessage) {
        rollbackIfNecessaryNoLock();
        return queryResultWithError(exceptionMessage);
    }

protected:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::unique_ptr<transaction::Transaction> activeTransaction;
    ConnectionTransactionMode transactionMode;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu



