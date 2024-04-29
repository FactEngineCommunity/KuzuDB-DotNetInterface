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

#include <cstdint>
#include <cstring>
#include <string>

namespace kuzu {
namespace common {

struct ku_string_t {

    static constexpr uint64_t PREFIX_LENGTH = 4;
    static constexpr uint64_t INLINED_SUFFIX_LENGTH = 8;
    static constexpr uint64_t SHORT_STR_LENGTH = PREFIX_LENGTH + INLINED_SUFFIX_LENGTH;

    uint32_t len;
    uint8_t prefix[PREFIX_LENGTH];
    union {
        uint8_t data[INLINED_SUFFIX_LENGTH];
        uint64_t overflowPtr;
    };

    ku_string_t() : len{0}, overflowPtr{0} {}
    ku_string_t(const char* value, uint64_t length);

    static bool isShortString(uint32_t len) { return len <= SHORT_STR_LENGTH; }

    inline const uint8_t* getData() const {
        return isShortString(len) ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    // These functions do *NOT* allocate/resize the overflow buffer, it only copies the content and
    // set the length.
    void set(const std::string& value);
    void set(const char* value, uint64_t length);
    void set(const ku_string_t& value);
    inline void setShortString(const char* value, uint64_t length) {
        this->len = length;
        memcpy(prefix, value, length);
    }
    inline void setLongString(const char* value, uint64_t length) {
        this->len = length;
        memcpy(prefix, value, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), value, length);
    }
    inline void setShortString(const ku_string_t& value) {
        this->len = value.len;
        memcpy(prefix, value.prefix, value.len);
    }
    inline void setLongString(const ku_string_t& value) {
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

namespace testing {
class BaseGraphTest;
class PrivateGraphTest;
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

namespace spdlog {
class logger;
namespace level {
enum level_enum : int;
} // namespace level
} // namespace spdlog

#include <cstdint>

namespace kuzu {
namespace common {

enum class ColumnDataFormat : uint8_t { REGULAR = 0, CSR = 1 };

}
} // namespace kuzu

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class TableType : uint8_t {
    UNKNOWN = 0,
    NODE = 1,
    REL = 2,
    RDF = 3,
    REL_GROUP = 4,
};

struct TableTypeUtils {
    static std::string toString(TableType tableType);
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <string_view>

namespace kuzu {
namespace common {

extern const char* KUZU_VERSION;

constexpr uint64_t DEFAULT_VECTOR_CAPACITY_LOG_2 = 11;
constexpr uint64_t DEFAULT_VECTOR_CAPACITY = (uint64_t)1 << DEFAULT_VECTOR_CAPACITY_LOG_2;

constexpr double DEFAULT_HT_LOAD_FACTOR = 1.5;
constexpr uint32_t DEFAULT_VAR_LENGTH_EXTEND_MAX_DEPTH = 30;
constexpr bool DEFAULT_ENABLE_SEMI_MASK = true;

// This is the default thread sleep time we use when a thread,
// e.g., a worker thread is in TaskScheduler, needs to block.
constexpr uint64_t THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS = 500;

constexpr uint64_t DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS = 5000000;

// Note that some places use std::bit_ceil to calculate resizes,
// which won't work for values other than 2. If this is changed, those will need to be updated
constexpr uint64_t CHUNK_RESIZE_RATIO = 2;

struct InternalKeyword {
    static constexpr char ANONYMOUS[] = "";
    static constexpr char ID[] = "_ID";
    static constexpr char LABEL[] = "_LABEL";
    static constexpr char SRC[] = "_SRC";
    static constexpr char DST[] = "_DST";
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
#ifdef __32BIT__
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 30; // (1GB)
#else
    static constexpr uint64_t DEFAULT_VM_REGION_MAX_SIZE = (uint64_t)1 << 43; // (8TB)
#endif

    static constexpr uint64_t DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING = 1ull << 26; // (64MB)
};

struct StorageConstants {
    static constexpr char OVERFLOW_FILE_SUFFIX[] = ".ovf";
    static constexpr char WAL_FILE_SUFFIX[] = ".wal";
    static constexpr char INDEX_FILE_SUFFIX[] = ".hindex";
    static constexpr char NODES_STATISTICS_AND_DELETED_IDS_FILE_NAME[] =
        "nodes.statistics_and_deleted.ids";
    static constexpr char NODES_STATISTICS_FILE_NAME_FOR_WAL[] =
        "nodes.statistics_and_deleted.ids.wal";
    static constexpr char RELS_METADATA_FILE_NAME[] = "rels.statistics";
    static constexpr char RELS_METADATA_FILE_NAME_FOR_WAL[] = "rels.statistics.wal";
    static constexpr char CATALOG_FILE_NAME[] = "catalog.kz";
    static constexpr char CATALOG_FILE_NAME_FOR_WAL[] = "catalog.kz.wal";
    static constexpr char DATA_FILE_NAME[] = "data.kz";
    static constexpr char METADATA_FILE_NAME[] = "metadata.kz";
    static constexpr char LOCK_FILE_NAME[] = ".lock";

    // The number of pages that we add at one time when we need to grow a file.
    static constexpr uint64_t PAGE_GROUP_SIZE_LOG2 = 10;
    static constexpr uint64_t PAGE_GROUP_SIZE = (uint64_t)1 << PAGE_GROUP_SIZE_LOG2;
    static constexpr uint64_t PAGE_IDX_IN_GROUP_MASK = ((uint64_t)1 << PAGE_GROUP_SIZE_LOG2) - 1;

    static constexpr uint64_t NODE_GROUP_SIZE_LOG2 = 17; // 64 * 2048 nodes per group
    static constexpr uint64_t NODE_GROUP_SIZE = (uint64_t)1 << NODE_GROUP_SIZE_LOG2;

    static constexpr double PACKED_CSR_DENSITY = 0.8;
    static constexpr double LEAF_LOW_CSR_DENSITY = 0.1;
    static constexpr double LEAF_HIGH_CSR_DENSITY = 1.0;
    // The number of CSR lists in a segment.
    static constexpr uint64_t CSR_SEGMENT_SIZE_LOG2 = 10;
    static constexpr uint64_t CSR_SEGMENT_SIZE = (uint64_t)1 << CSR_SEGMENT_SIZE_LOG2;

    static constexpr bool TRUNCATE_OVER_LARGE_STRINGS = true;
};

// Hash Index Configurations
struct HashIndexConstants {
    static constexpr uint16_t SLOT_CAPACITY_BYTES = 256;
    static constexpr double MAX_LOAD_FACTOR = 0.8;
};

struct CopyConstants {
    // Initial size of buffer for CSV Reader.
    static constexpr uint64_t INITIAL_BUFFER_SIZE = 16384;
    // This means that we will usually read the entirety of the contents of the file we need for a
    // block in one read request. It is also very small, which means we can parallelize small files
    // efficiently.
    static const uint64_t PARALLEL_BLOCK_SIZE = INITIAL_BUFFER_SIZE / 2;

    static constexpr const char* BOOL_CSV_PARSING_OPTIONS[] = {"HEADER", "PARALLEL"};
    static constexpr bool DEFAULT_CSV_HAS_HEADER = false;
    static constexpr bool DEFAULT_CSV_PARALLEL = true;

    // Default configuration for csv file parsing
    static constexpr const char* STRING_CSV_PARSING_OPTIONS[] = {"ESCAPE", "DELIM", "QUOTE"};
    static constexpr char DEFAULT_CSV_ESCAPE_CHAR = '\\';
    static constexpr char DEFAULT_CSV_DELIMITER = ',';
    static constexpr char DEFAULT_CSV_QUOTE_CHAR = '"';
    static constexpr char DEFAULT_CSV_LIST_BEGIN_CHAR = '[';
    static constexpr char DEFAULT_CSV_LIST_END_CHAR = ']';
    static constexpr char DEFAULT_CSV_LINE_BREAK = '\n';
    static constexpr const char* ROW_IDX_COLUMN_NAME = "ROW_IDX";
    static constexpr uint64_t PANDAS_PARTITION_COUNT = 50 * DEFAULT_VECTOR_CAPACITY;
};

struct RdfConstants {
    static constexpr const char IN_MEMORY_OPTION[] = "IN_MEMORY";
    static constexpr const char STRICT_OPTION[] = "STRICT";
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
    static constexpr uint64_t SIP_RATIO = 5;
};

struct ClientContextConstants {
    // We disable query timeout by default.
    static constexpr uint64_t TIMEOUT_IN_MS = 0;
};

struct OrderByConstants {
    static constexpr uint64_t NUM_BYTES_FOR_PAYLOAD_IDX = 8;
    static constexpr uint64_t MIN_SIZE_TO_REDUCE = common::DEFAULT_VECTOR_CAPACITY * 5;
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
};

struct CopyToCSVConstants {
    static constexpr const char* DEFAULT_CSV_NEWLINE = "\n";
    static constexpr const char* DEFAULT_NULL_STR = "";
    static constexpr const bool DEFAULT_FORCE_QUOTE = false;
    static constexpr const uint64_t DEFAULT_CSV_FLUSH_SIZE = 4096 * 8;
};

} // namespace common
} // namespace kuzu

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
    Object copy() const { return *this; }

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

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

/**
 * Function name is a temporary identifier used for binder because grammar does not parse built in
 * functions. After binding, expression type should replace function name and used as identifier.
 */
// aggregate
const char* const COUNT_STAR_FUNC_NAME = "COUNT_STAR";
const char* const COUNT_FUNC_NAME = "COUNT";
const char* const SUM_FUNC_NAME = "SUM";
const char* const AVG_FUNC_NAME = "AVG";
const char* const MIN_FUNC_NAME = "MIN";
const char* const MAX_FUNC_NAME = "MAX";
const char* const COLLECT_FUNC_NAME = "COLLECT";

// cast
const char* const CAST_FUNC_NAME = "CAST";
const char* const CAST_DATE_FUNC_NAME = "DATE";
const char* const CAST_TO_DATE_FUNC_NAME = "TO_DATE";
const char* const CAST_TO_TIMESTAMP_FUNC_NAME = "TIMESTAMP";
const char* const CAST_INTERVAL_FUNC_NAME = "INTERVAL";
const char* const CAST_TO_INTERVAL_FUNC_NAME = "TO_INTERVAL";
const char* const CAST_STRING_FUNC_NAME = "STRING";
const char* const CAST_TO_STRING_FUNC_NAME = "TO_STRING";
const char* const CAST_TO_DOUBLE_FUNC_NAME = "TO_DOUBLE";
const char* const CAST_TO_FLOAT_FUNC_NAME = "TO_FLOAT";
const char* const CAST_TO_SERIAL_FUNC_NAME = "TO_SERIAL";
const char* const CAST_TO_INT64_FUNC_NAME = "TO_INT64";
const char* const CAST_TO_INT32_FUNC_NAME = "TO_INT32";
const char* const CAST_TO_INT16_FUNC_NAME = "TO_INT16";
const char* const CAST_TO_INT8_FUNC_NAME = "TO_INT8";
const char* const CAST_TO_UINT64_FUNC_NAME = "TO_UINT64";
const char* const CAST_TO_UINT32_FUNC_NAME = "TO_UINT32";
const char* const CAST_TO_UINT16_FUNC_NAME = "TO_UINT16";
const char* const CAST_TO_UINT8_FUNC_NAME = "TO_UINT8";
const char* const CAST_BLOB_FUNC_NAME = "BLOB";
const char* const CAST_TO_BLOB_FUNC_NAME = "TO_BLOB";
const char* const CAST_UUID_FUNC_NAME = "UUID";
const char* const CAST_TO_UUID_FUNC_NAME = "TO_UUID";
const char* const CAST_TO_BOOL_FUNC_NAME = "TO_BOOL";
const char* const CAST_TO_INT128_FUNC_NAME = "TO_INT128";

// list
const char* const LIST_CREATION_FUNC_NAME = "LIST_CREATION";
const char* const LIST_RANGE_FUNC_NAME = "RANGE";
const char* const LIST_EXTRACT_FUNC_NAME = "LIST_EXTRACT";
const char* const LIST_ELEMENT_FUNC_NAME = "LIST_ELEMENT";
const char* const LIST_CONCAT_FUNC_NAME = "LIST_CONCAT";
const char* const LIST_CAT_FUNC_NAME = "LIST_CAT";
const char* const ARRAY_CONCAT_FUNC_NAME = "ARRAY_CONCAT";
const char* const ARRAY_CAT_FUNC_NAME = "ARRAY_CAT";
const char* const LIST_APPEND_FUNC_NAME = "LIST_APPEND";
const char* const ARRAY_APPEND_FUNC_NAME = "ARRAY_APPEND";
const char* const ARRAY_PUSH_BACK_FUNC_NAME = "ARRAY_PUSH_BACK";
const char* const LIST_PREPEND_FUNC_NAME = "LIST_PREPEND";
const char* const ARRAY_PREPEND_FUNC_NAME = "ARRAY_PREPEND";
const char* const ARRAY_PUSH_FRONT_FUNC_NAME = "ARRAY_PUSH_FRONT";
const char* const LIST_POSITION_FUNC_NAME = "LIST_POSITION";
const char* const LIST_INDEXOF_FUNC_NAME = "LIST_INDEXOF";
const char* const ARRAY_POSITION_FUNC_NAME = "ARRAY_POSITION";
const char* const ARRAY_INDEXOF_FUNC_NAME = "ARRAY_INDEXOF";
const char* const LIST_CONTAINS_FUNC_NAME = "LIST_CONTAINS";
const char* const LIST_HAS_FUNC_NAME = "LIST_HAS";
const char* const ARRAY_CONTAINS_FUNC_NAME = "ARRAY_CONTAINS";
const char* const ARRAY_HAS_FUNC_NAME = "ARRAY_HAS";
const char* const LIST_SLICE_FUNC_NAME = "LIST_SLICE";
const char* const ARRAY_SLICE_FUNC_NAME = "ARRAY_SLICE";
const char* const LIST_SUM_FUNC_NAME = "LIST_SUM";
const char* const LIST_PRODUCT_FUNC_NAME = "LIST_PRODUCT";
const char* const LIST_SORT_FUNC_NAME = "LIST_SORT";
const char* const LIST_REVERSE_SORT_FUNC_NAME = "LIST_REVERSE_SORT";
const char* const LIST_DISTINCT_FUNC_NAME = "LIST_DISTINCT";
const char* const LIST_UNIQUE_FUNC_NAME = "LIST_UNIQUE";
const char* const LIST_ANY_VALUE_FUNC_NAME = "LIST_ANY_VALUE";
const char* const LIST_REVERSE_FUNC_NAME = "LIST_REVERSE";

// struct
const char* const STRUCT_PACK_FUNC_NAME = "STRUCT_PACK";
const char* const STRUCT_EXTRACT_FUNC_NAME = "STRUCT_EXTRACT";

// map
const char* const MAP_CREATION_FUNC_NAME = "MAP";
const char* const MAP_EXTRACT_FUNC_NAME = "MAP_EXTRACT";
const char* const ELEMENT_AT_FUNC_NAME = "ELEMENT_AT"; // alias of MAP_EXTRACT
const char* const CARDINALITY_FUNC_NAME = "CARDINALITY";
const char* const MAP_KEYS_FUNC_NAME = "MAP_KEYS";
const char* const MAP_VALUES_FUNC_NAME = "MAP_VALUES";

// union
const char* const UNION_VALUE_FUNC_NAME = "UNION_VALUE";
const char* const UNION_TAG_FUNC_NAME = "UNION_TAG";
const char* const UNION_EXTRACT_FUNC_NAME = "UNION_EXTRACT";

// comparison
const char* const EQUALS_FUNC_NAME = "EQUALS";
const char* const NOT_EQUALS_FUNC_NAME = "NOT_EQUALS";
const char* const GREATER_THAN_FUNC_NAME = "GREATER_THAN";
const char* const GREATER_THAN_EQUALS_FUNC_NAME = "GREATER_THAN_EQUALS";
const char* const LESS_THAN_FUNC_NAME = "LESS_THAN";
const char* const LESS_THAN_EQUALS_FUNC_NAME = "LESS_THAN_EQUALS";

// arithmetics operators
const char* const ADD_FUNC_NAME = "+";
const char* const SUBTRACT_FUNC_NAME = "-";
const char* const MULTIPLY_FUNC_NAME = "*";
const char* const DIVIDE_FUNC_NAME = "/";
const char* const MODULO_FUNC_NAME = "%";
const char* const POWER_FUNC_NAME = "^";

// arithmetics functions
const char* const ABS_FUNC_NAME = "ABS";
const char* const ACOS_FUNC_NAME = "ACOS";
const char* const ASIN_FUNC_NAME = "ASIN";
const char* const ATAN_FUNC_NAME = "ATAN";
const char* const ATAN2_FUNC_NAME = "ATAN2";
const char* const BITWISE_XOR_FUNC_NAME = "BITWISE_XOR";
const char* const BITWISE_AND_FUNC_NAME = "BITWISE_AND";
const char* const BITWISE_OR_FUNC_NAME = "BITWISE_OR";
const char* const BITSHIFT_LEFT_FUNC_NAME = "BITSHIFT_LEFT";
const char* const BITSHIFT_RIGHT_FUNC_NAME = "BITSHIFT_RIGHT";
const char* const CBRT_FUNC_NAME = "CBRT";
const char* const CEIL_FUNC_NAME = "CEIL";
const char* const CEILING_FUNC_NAME = "CEILING";
const char* const COS_FUNC_NAME = "COS";
const char* const COT_FUNC_NAME = "COT";
const char* const DEGREES_FUNC_NAME = "DEGREES";
const char* const EVEN_FUNC_NAME = "EVEN";
const char* const FACTORIAL_FUNC_NAME = "FACTORIAL";
const char* const FLOOR_FUNC_NAME = "FLOOR";
const char* const GAMMA_FUNC_NAME = "GAMMA";
const char* const LGAMMA_FUNC_NAME = "LGAMMA";
const char* const LN_FUNC_NAME = "LN";
const char* const LOG_FUNC_NAME = "LOG";
const char* const LOG2_FUNC_NAME = "LOG2";
const char* const LOG10_FUNC_NAME = "LOG10";
const char* const NEGATE_FUNC_NAME = "NEGATE";
const char* const PI_FUNC_NAME = "PI";
const char* const POW_FUNC_NAME = "POW";
const char* const RADIANS_FUNC_NAME = "RADIANS";
const char* const ROUND_FUNC_NAME = "ROUND";
const char* const SIN_FUNC_NAME = "SIN";
const char* const SIGN_FUNC_NAME = "SIGN";
const char* const SQRT_FUNC_NAME = "SQRT";
const char* const TAN_FUNC_NAME = "TAN";

// string
const char* const ARRAY_EXTRACT_FUNC_NAME = "ARRAY_EXTRACT";
const char* const CONCAT_FUNC_NAME = "CONCAT";
const char* const CONTAINS_FUNC_NAME = "CONTAINS";
const char* const ENDS_WITH_FUNC_NAME = "ENDS_WITH";
const char* const LCASE_FUNC_NAME = "LCASE";
const char* const LEFT_FUNC_NAME = "LEFT";
const char* const LENGTH_FUNC_NAME = "LENGTH";
const char* const LOWER_FUNC_NAME = "LOWER";
const char* const LPAD_FUNC_NAME = "LPAD";
const char* const LTRIM_FUNC_NAME = "LTRIM";
const char* const PREFIX_FUNC_NAME = "PREFIX";
const char* const REPEAT_FUNC_NAME = "REPEAT";
const char* const REVERSE_FUNC_NAME = "REVERSE";
const char* const RIGHT_FUNC_NAME = "RIGHT";
const char* const RPAD_FUNC_NAME = "RPAD";
const char* const RTRIM_FUNC_NAME = "RTRIM";
const char* const STARTS_WITH_FUNC_NAME = "STARTS_WITH";
const char* const SUBSTR_FUNC_NAME = "SUBSTR";
const char* const SUBSTRING_FUNC_NAME = "SUBSTRING";
const char* const SUFFIX_FUNC_NAME = "SUFFIX";
const char* const TRIM_FUNC_NAME = "TRIM";
const char* const UCASE_FUNC_NAME = "UCASE";
const char* const UPPER_FUNC_NAME = "UPPER";
const char* const REGEXP_FULL_MATCH_FUNC_NAME = "REGEXP_FULL_MATCH";
const char* const REGEXP_MATCHES_FUNC_NAME = "REGEXP_MATCHES";
const char* const REGEXP_REPLACE_FUNC_NAME = "REGEXP_REPLACE";
const char* const REGEXP_EXTRACT_FUNC_NAME = "REGEXP_EXTRACT";
const char* const REGEXP_EXTRACT_ALL_FUNC_NAME = "REGEXP_EXTRACT_ALL";
const char* const SIZE_FUNC_NAME = "SIZE";
const char* const LEVENSHTEIN_FUNC_NAME = "LEVENSHTEIN";

// Date functions.
const char* const DATE_PART_FUNC_NAME = "DATE_PART";
const char* const DATEPART_FUNC_NAME = "DATEPART";
const char* const DATE_TRUNC_FUNC_NAME = "DATE_TRUNC";
const char* const DATETRUNC_FUNC_NAME = "DATETRUNC";
const char* const DAYNAME_FUNC_NAME = "DAYNAME";
const char* const GREATEST_FUNC_NAME = "GREATEST";
const char* const LAST_DAY_FUNC_NAME = "LAST_DAY";
const char* const LEAST_FUNC_NAME = "LEAST";
const char* const MAKE_DATE_FUNC_NAME = "MAKE_DATE";
const char* const MONTHNAME_FUNC_NAME = "MONTHNAME";

// Timestamp functions.
const char* const CENTURY_FUNC_NAME = "CENTURY";
const char* const EPOCH_MS_FUNC_NAME = "EPOCH_MS";
const char* const TO_TIMESTAMP_FUNC_NAME = "TO_TIMESTAMP";

// Interval functions.
const char* const TO_YEARS_FUNC_NAME = "TO_YEARS";
const char* const TO_MONTHS_FUNC_NAME = "TO_MONTHS";
const char* const TO_DAYS_FUNC_NAME = "TO_DAYS";
const char* const TO_HOURS_FUNC_NAME = "TO_HOURS";
const char* const TO_MINUTES_FUNC_NAME = "TO_MINUTES";
const char* const TO_SECONDS_FUNC_NAME = "TO_SECONDS";
const char* const TO_MILLISECONDS_FUNC_NAME = "TO_MILLISECONDS";
const char* const TO_MICROSECONDS_FUNC_NAME = "TO_MICROSECONDS";

// Node/Rel functions.
const char* const ID_FUNC_NAME = "ID";
const char* const LABEL_FUNC_NAME = "LABEL";
const char* const OFFSET_FUNC_NAME = "OFFSET";

// Path functions
const char* const NODES_FUNC_NAME = "NODES";
const char* const RELS_FUNC_NAME = "RELS";
const char* const PROPERTIES_FUNC_NAME = "PROPERTIES";
const char* const IS_TRAIL_FUNC_NAME = "IS_TRAIL";
const char* const IS_ACYCLIC_FUNC_NAME = "IS_ACYCLIC";

// Blob functions
const char* const OCTET_LENGTH_FUNC_NAME = "OCTET_LENGTH";
const char* const ENCODE_FUNC_NAME = "ENCODE";
const char* const DECODE_FUNC_NAME = "DECODE";

// UUID functions
const char* const GEN_RANDOM_UUID_FUNC_NAME = "GEN_RANDOM_UUID";

// RDF functions
const char* const TYPE_FUNC_NAME = "TYPE";
const char* const VALIDATE_PREDICATE_FUNC_NAME = "VALIDATE_PREDICATE";

// Table functions
const char* const TABLE_INFO_FUNC_NAME = "TABLE_INFO";
const char* const DB_VERSION_FUNC_NAME = "DB_VERSION";
const char* const CURRENT_SETTING_FUNC_NAME = "CURRENT_SETTING";
const char* const SHOW_TABLES_FUNC_NAME = "SHOW_TABLES";
const char* const SHOW_CONNECTION_FUNC_NAME = "SHOW_CONNECTION";
const char* const STORAGE_INFO_FUNC_NAME = "STORAGE_INFO";
// Table functions - read functions
const char* const READ_PARQUET_FUNC_NAME = "READ_PARQUET";
const char* const READ_NPY_FUNC_NAME = "READ_NPY";
const char* const READ_CSV_SERIAL_FUNC_NAME = "READ_CSV_SERIAL";
const char* const READ_CSV_PARALLEL_FUNC_NAME = "READ_CSV_PARALLEL";
const char* const READ_RDF_RESOURCE_FUNC_NAME = "READ_RDF_RESOURCE";
const char* const READ_RDF_LITERAL_FUNC_NAME = "READ_RDF_LITERAL";
const char* const READ_RDF_RESOURCE_TRIPLE_FUNC_NAME = "READ_RDF_RESOURCE_TRIPLE";
const char* const READ_RDF_LITERAL_TRIPLE_FUNC_NAME = "READ_RDF_LITERAL_TRIPLE";
const char* const READ_RDF_ALL_TRIPLE_FUNC_NAME = "READ_RDF_ALL_TRIPLE";
const char* const IN_MEM_READ_RDF_RESOURCE_FUNC_NAME = "IN_MEM_READ_RDF_RESOURCE";
const char* const IN_MEM_READ_RDF_LITERAL_FUNC_NAME = "IN_MEM_READ_RDF_LITERAL";
const char* const IN_MEM_READ_RDF_RESOURCE_TRIPLE_FUNC_NAME = "IN_MEM_READ_RDF_RESOURCE_TRIPLE";
const char* const IN_MEM_READ_RDF_LITERAL_TRIPLE_FUNC_NAME = "IN_MEM_READ_RDF_LITERAL_TRIPLE";
const char* const READ_PANDAS_FUNC_NAME = "READ_PANDAS";
const char* const READ_FTABLE_FUNC_NAME = "READ_FTABLE";

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

    FUNCTION = 110,

    AGGREGATE_FUNCTION = 130,

    SUBQUERY = 190,

    CASE_ELSE = 200,

    MACRO = 210,
};

bool isExpressionUnary(ExpressionType type);
bool isExpressionBinary(ExpressionType type);
bool isExpressionBoolConnection(ExpressionType type);
bool isExpressionComparison(ExpressionType type);
bool isExpressionNullOperator(ExpressionType type);
bool isExpressionLiteral(ExpressionType type);
bool isExpressionAggregate(ExpressionType type);
bool isExpressionSubquery(ExpressionType type);

std::string expressionTypeToString(ExpressionType type);

} // namespace common
} // namespace kuzu

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_TABLE = 1,
    DROP_TABLE = 2,
    ALTER = 3,
    COPY_TO = 19,
    COPY_FROM = 20,
    STANDALONE_CALL = 21,
    EXPLAIN = 22,
    CREATE_MACRO = 23,
    COMMENT_ON = 24,
    TRANSACTION = 30,
    EXTENSION = 31,
    EXPORT_DATABASE = 32,
};

struct StatementTypeUtils {
    static bool allowActiveTransaction(StatementType statementType) {
        switch (statementType) {
        case StatementType::CREATE_TABLE:
        case StatementType::DROP_TABLE:
        case StatementType::ALTER:
        case StatementType::CREATE_MACRO:
        case StatementType::COPY_FROM:
            return false;
        default:
            return true;
        }
    }
};

} // namespace common
} // namespace kuzu

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

#include <cstdint>
#include <string>


namespace kuzu {
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
    KUZU_API static void normalizeIntervalEntries(
        interval_t input, int64_t& months, int64_t& days, int64_t& micros);
    KUZU_API static void tryGetDatePartSpecifier(std::string specifier, DatePartSpecifier& result);
    KUZU_API static int32_t getIntervalPart(DatePartSpecifier specifier, interval_t& timestamp);
    KUZU_API static int64_t getMicro(const interval_t& val);
    KUZU_API static int64_t getNanoseconds(const interval_t& val);
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
    KUZU_API static bool tryConvertInterval(
        const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);
    KUZU_API static bool tryConvertTime(
        const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);

    // Convert a time object to a string in the format "hh:mm:ss"
    KUZU_API static std::string toString(dtime_t time);

    KUZU_API static dtime_t fromTime(
        int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);

    // Extract the time from a given timestamp object
    KUZU_API static void convert(
        dtime_t time, int32_t& out_hour, int32_t& out_min, int32_t& out_sec, int32_t& out_micros);

    KUZU_API static bool isValid(
        int32_t hour, int32_t minute, int32_t second, int32_t milliseconds);

private:
    static bool tryConvertInternal(const char* buf, uint64_t len, uint64_t& pos, dtime_t& result);
    static dtime_t fromTimeInternal(
        int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);
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
    int128_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
    constexpr int128_t(uint64_t low, int64_t high) : low(low), high(high) {}

    constexpr int128_t(const int128_t&) = default;
    constexpr int128_t(int128_t&&) = default;
    int128_t& operator=(const int128_t&) = default;
    int128_t& operator=(int128_t&&) = default;

    // comparison operators
    bool operator==(const int128_t& rhs) const;
    bool operator!=(const int128_t& rhs) const;
    bool operator>(const int128_t& rhs) const;
    bool operator>=(const int128_t& rhs) const;
    bool operator<(const int128_t& rhs) const;
    bool operator<=(const int128_t& rhs) const;

    // arithmetic operators
    int128_t operator+(const int128_t& rhs) const;
    int128_t operator-(const int128_t& rhs) const;
    int128_t operator*(const int128_t& rhs) const;
    int128_t operator/(const int128_t& rhs) const;
    int128_t operator%(const int128_t& rhs) const;
    int128_t operator-() const;

    // inplace arithmetic operators
    int128_t& operator+=(const int128_t& rhs);
    int128_t& operator*=(const int128_t& rhs);

    // cast operators
    explicit operator int64_t() const;
};

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
        int128_t result;
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

#include <cstdint>
#include <unordered_set>
#include <vector>


namespace kuzu {
namespace common {

struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

using table_id_t = uint64_t;
using table_id_vector_t = std::vector<table_id_t>;
using table_id_set_t = std::unordered_set<table_id_t>;
using offset_t = uint64_t;
constexpr table_id_t INVALID_TABLE_ID = UINT64_MAX;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;

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

} // namespace common
} // namespace kuzu

#include <exception>
#include <string>


namespace kuzu {
namespace common {

class KUZU_API Exception : public std::exception {
public:
    explicit Exception(std::string msg) : exception(), exception_message_(std::move(msg)){};

public:
    const char* what() const noexcept override { return exception_message_.c_str(); }

private:
    std::string exception_message_;
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

namespace kuzu {
namespace main {

struct Version {
public:
    /**
     * @brief Get the version of the Kùzu library.
     * @return const char* The version of the Kùzu library.
     */
    KUZU_API static const char* getVersion();

    /**
     * @brief Get the storage version of the Kùzu library.
     * @return uint64_t The storage version of the Kùzu library.
     */
    KUZU_API static uint64_t getStorageVersion();
};
} // namespace main
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

#include <memory>
#include <string>
#include <vector>


namespace kuzu {
namespace common {
class FileSystem;
enum class LogicalTypeID : uint8_t;
} // namespace common

namespace function {
struct Function;
} // namespace function

namespace extension {
struct ExtensionUtils;
struct ExtensionOptions;
} // namespace extension

namespace main {
struct ExtensionOption;

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
     */
    explicit SystemConfig(uint64_t bufferPoolSize = -1u, uint64_t maxNumThreads = 0,
        bool enableCompression = true, bool readOnly = false, uint64_t maxDBSize = -1u);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    bool readOnly;
    uint64_t maxDBSize;
};

/**
 * @brief Database class is the main class of KùzuDB. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class ClientContext;
    friend class Connection;
    friend class StorageDriver;
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::PrivateGraphTest;
    friend class transaction::TransactionContext;
    friend struct extension::ExtensionUtils;

public:
    /**
     * @brief Creates a database object.
     * @param databasePath Database path.
     * @param systemConfig System configurations (buffer pool size and max num threads).
     */
    KUZU_API explicit Database(
        std::string_view databasePath, SystemConfig systemConfig = SystemConfig());
    /**
     * @brief Destructs the database object.
     */
    KUZU_API ~Database();

    /**
     * @brief Sets the logging level of the database instance.
     * @param loggingLevel New logging level. (Supported logging levels are: "info", "debug",
     * "err").
     */
    KUZU_API static void setLoggingLevel(std::string loggingLevel);

    // TODO(Ziyi): Instead of exposing a dedicated API for adding a new function, we should consider
    // add function through the extension module.
    void addBuiltInFunction(
        std::string name, std::vector<std::unique_ptr<function::Function>> functionSet);

    KUZU_API void registerFileSystem(std::unique_ptr<common::FileSystem> fs);

    KUZU_API void addExtensionOption(
        std::string name, common::LogicalTypeID type, common::Value defaultValue);

    ExtensionOption* getExtensionOption(std::string name);

private:
    void openLockFile();
    void initDBDirAndCoreFilesIfNecessary();
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
    std::unique_ptr<common::VirtualFileSystem> vfs;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<storage::WAL> wal;
    std::shared_ptr<spdlog::logger> logger;
    std::unique_ptr<common::FileInfo> lockFile;
    std::unique_ptr<extension::ExtensionOptions> extensionOptions;
};

} // namespace main
} // namespace kuzu


namespace kuzu {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a query.
 */
struct PreparedSummary {
    double compilingTime = 0;
    common::StatementType statementType;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
class QuerySummary {
    friend class Connection;
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

    void setPreparedSummary(PreparedSummary preparedSummary_);

    /**
     * @return true if the query is executed with EXPLAIN.
     */
    bool isExplain() const;

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
};

} // namespace main
} // namespace kuzu

#include <algorithm>
#include <memory>
#include <utility>


namespace kuzu {
namespace common {

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

class NullMask {

public:
    static constexpr uint64_t NO_NULL_ENTRY = 0;
    static constexpr uint64_t ALL_NULL_ENTRY = ~uint64_t(NO_NULL_ENTRY);
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY_LOG2 = 6;
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY = (uint64_t)1 << NUM_BITS_PER_NULL_ENTRY_LOG2;
    static constexpr uint64_t NUM_BYTES_PER_NULL_ENTRY = NUM_BITS_PER_NULL_ENTRY >> 3;
    static constexpr uint64_t DEFAULT_NUM_NULL_ENTRIES =
        DEFAULT_VECTOR_CAPACITY >> NUM_BITS_PER_NULL_ENTRY_LOG2;

    NullMask() : NullMask{DEFAULT_NUM_NULL_ENTRIES} {}

    explicit NullMask(uint64_t numNullEntries)
        : mayContainNulls{false}, numNullEntries{numNullEntries} {
        buffer = std::make_unique<uint64_t[]>(numNullEntries);
        data = buffer.get();
        std::fill(data, data + numNullEntries, NO_NULL_ENTRY);
    }

    inline void setAllNonNull() {
        if (!mayContainNulls) {
            return;
        }
        std::fill(data, data + numNullEntries, NO_NULL_ENTRY);
        mayContainNulls = false;
    }
    inline void setAllNull() {
        std::fill(data, data + numNullEntries, ALL_NULL_ENTRY);
        mayContainNulls = true;
    }

    inline bool hasNoNullsGuarantee() const { return !mayContainNulls; }

    static void setNull(uint64_t* nullEntries, uint32_t pos, bool isNull);
    inline void setNull(uint32_t pos, bool isNull) {
        setNull(data, pos, isNull);
        if (isNull) {
            mayContainNulls = true;
        }
    }

    static inline bool isNull(const uint64_t* nullEntries, uint32_t pos) {
        auto [entryPos, bitPosInEntry] = getNullEntryAndBitPos(pos);
        return nullEntries[entryPos] & NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
    }

    inline bool isNull(uint32_t pos) const { return isNull(data, pos); }

    // const because updates to the data must set mayContainNulls if any value
    // becomes non-null
    // Modifying the underlying data should be done with setNull or copyFromNullData
    inline const uint64_t* getData() { return data; }

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

    bool copyFromNullBits(const uint64_t* srcNullEntries, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy);

    // Sets the given number of bits to null (if isNull is true) or non-null (if isNull is false),
    // starting at the offset
    static void setNullRange(
        uint64_t* nullEntries, uint64_t offset, uint64_t numBitsToSet, bool isNull);

    void setNullFromRange(uint64_t offset, uint64_t numBitsToSet, bool isNull);

    void resize(uint64_t capacity);

private:
    static inline std::pair<uint64_t, uint64_t> getNullEntryAndBitPos(uint64_t pos) {
        auto nullEntryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
        return std::make_pair(
            nullEntryPos, pos - (nullEntryPos << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2));
    }

private:
    uint64_t* data;
    std::unique_ptr<uint64_t[]> buffer;
    bool mayContainNulls;
    uint64_t numNullEntries;
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType) : statementType{statementType} {}

    virtual ~Statement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

private:
    common::StatementType statementType;
};

} // namespace parser
} // namespace kuzu


namespace kuzu {
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
    KUZU_API static bool tryConvertDate(
        const char* buf, uint64_t len, uint64_t& pos, date_t& result);

    // private:
    // Returns true if (year) is a leap year, and false otherwise
    KUZU_API static bool isLeapYear(int32_t year);
    // Returns true if the specified (year, month, day) combination is a valid
    // date
    KUZU_API static bool isValid(int32_t year, int32_t month, int32_t day);
    // Extract the year, month and day from a given date object
    KUZU_API static void convert(
        date_t date, int32_t& out_year, int32_t& out_month, int32_t& out_day);
    // Create a Date object from a specified (year, month, day) combination
    KUZU_API static date_t fromDate(int32_t year, int32_t month, int32_t day);

    // Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
    KUZU_API static bool parseDoubleDigit(
        const char* buf, uint64_t len, uint64_t& pos, int32_t& result);

    KUZU_API static int32_t monthDays(int32_t year, int32_t month);

    KUZU_API static std::string getDayName(date_t& date);

    KUZU_API static std::string getMonthName(date_t& date);

    KUZU_API static date_t getLastDay(date_t& date);

    KUZU_API static int32_t getDatePart(DatePartSpecifier specifier, date_t& date);

    KUZU_API static date_t trunc(DatePartSpecifier specifier, date_t& date);

    KUZU_API static int64_t getEpochNanoSeconds(const date_t& date);

private:
    static void extractYearOffset(int32_t& n, int32_t& year, int32_t& year_offset);
};

} // namespace common
} // namespace kuzu


namespace kuzu {
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

class KUZU_API RuntimeException : public Exception {
public:
    explicit RuntimeException(const std::string& msg) : Exception("Runtime exception: " + msg){};
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
namespace storage {
class Column;
}

namespace main {

class KUZU_API StorageDriver {
public:
    explicit StorageDriver(Database* database);

    ~StorageDriver() = default;

    void scan(const std::string& nodeName, const std::string& propertyName,
        common::offset_t* offsets, size_t size, uint8_t* result, size_t numThreads);

    uint64_t getNumNodes(const std::string& nodeName);
    uint64_t getNumRels(const std::string& relName);

private:
    void scanColumn(transaction::Transaction* transaction, storage::Column* column,
        common::offset_t* offsets, size_t size, uint8_t* result);

private:
    catalog::Catalog* catalog;
    storage::StorageManager* storageManager;
};

} // namespace main
} // namespace kuzu

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


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

    inline std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return parameterMap;
    }

    KUZU_API ~PreparedStatement();

private:
    bool isProfile();

private:
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

    KUZU_API static int32_t getTimestampPart(DatePartSpecifier specifier, timestamp_t& timestamp);

    KUZU_API static timestamp_t trunc(DatePartSpecifier specifier, timestamp_t& date);

    KUZU_API static int64_t getEpochNanoSeconds(const timestamp_t& timestamp);

    KUZU_API static int64_t getEpochMilliSeconds(const timestamp_t& timestamp);

    KUZU_API static int64_t getEpochSeconds(const timestamp_t& timestamp);

    KUZU_API static bool tryParseUTCOffset(
        const char* str, uint64_t& pos, uint64_t len, int& hour_offset, int& minute_offset);

    static std::string getTimestampConversionExceptionMsg(
        const char* str, uint64_t len, const std::string& typeID = "TIMESTAMP") {
        return "Error occurred during parsing " + typeID + ". Given: \"" + std::string(str, len) +
               "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
    }

    KUZU_API static timestamp_t getCurrentTimestamp();
};

} // namespace common
} // namespace kuzu

#include <string>
#include <string_view>
#include <type_traits>


namespace kuzu {
namespace common {

namespace string_format_detail {
#define MAP_STD_TO_STRING(typ)                                                                     \
    inline std::string map(typ v) { return std::to_string(v); }

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
    inline typ map(typ v) { return v; }
MAP_SELF(const char*);
// Also covers std::string
MAP_SELF(std::string_view)

// chars are mapped to themselves, but signed char and unsigned char (which are used for int8_t and
// uint8_t respectively), need to be cast to be properly output as integers. This is consistent with
// fmt's behaviour.
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
inline void stringFormatHelper(
    std::string& ret, std::string_view format, Arg&& arg, Args&&... args) {
    size_t bracket = format.find('{');
    if (bracket == std::string_view::npos) {
        throw InternalException("Too many values for string_format.");
    }
    ret += format.substr(0, bracket);
    if (format.substr(bracket, 4) == "{{}}") {
        // Escaped {}.
        ret += "{}";
        return stringFormatHelper(
            ret, format.substr(bracket + 4), std::forward<Arg>(arg), std::forward<Args>(args)...);
    } else if (format.substr(bracket, 2) == "{}") {
        // Formatted {}.
        ret += map(arg);
        return stringFormatHelper(ret, format.substr(bracket + 2), std::forward<Args>(args)...);
    }
    // Something else.
    ret.push_back('{');
    return stringFormatHelper(
        ret, format.substr(bracket + 1), std::forward<Arg>(arg), std::forward<Args>(args)...);
}
} // namespace string_format_detail

//! Formats `args` according to `format`. Accepts {} for formatting the argument and {{}} for
//! a literal {}. Formatting is done with std::ostream::operator<<.
template<typename... Args>
inline std::string stringFormat(std::string_view format, Args... args) {
    std::string ret;
    ret.reserve(32); // Optimistic pre-allocation.
    string_format_detail::stringFormatHelper(ret, format, std::forward<Args>(args)...);
    return ret;
}

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(
    const char* condition_name, const char* file, int linenr) {
    // LCOV_EXCL_START
    throw InternalException(stringFormat(
        "Assertion failed in file \"{}\" on line {}: {}", file, linenr, condition_name));
    // LCOV_EXCL_STOP
}

#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define KU_ASSERT(condition)                                                                       \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)
#else
#define KU_ASSERT(condition) void(0)
#endif

#define KU_UNREACHABLE                                                                             \
    [[unlikely]] kuzu::common::kuAssertFailureInternal("KU_UNREACHABLE", __FILE__, __LINE__)

} // namespace common
} // namespace kuzu

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace kuzu {
namespace processor {
class ParquetReader;
};
namespace common {

class Serializer;
class Deserializer;
struct FileInfo;

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
constexpr block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;
using struct_field_idx_t = uint8_t;
using union_field_idx_t = struct_field_idx_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT8_MAX;
using row_idx_t = uint64_t;
constexpr row_idx_t INVALID_ROW_IDX = UINT64_MAX;
constexpr uint32_t UNDEFINED_CAST_COST = UINT32_MAX;
using node_group_idx_t = uint64_t;
constexpr node_group_idx_t INVALID_NODE_GROUP_IDX = UINT64_MAX;
using partition_idx_t = uint64_t;
constexpr partition_idx_t INVALID_PARTITION_IDX = UINT64_MAX;
using length_t = uint64_t;

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

struct map_entry_t {
    list_entry_t entry;
};

struct union_entry_t {
    struct_entry_t entry;
};

struct int128_t;
struct ku_string_t;

template<typename T>
concept HashablePrimitive = ((std::integral<T> && !std::is_same_v<T, bool>) ||
                             std::floating_point<T> || std::is_same_v<T, common::int128_t>);
template<typename T>
concept IndexHashable = ((std::integral<T> && !std::is_same_v<T, bool>) || std::floating_point<T> ||
                         std::is_same_v<T, common::int128_t> ||
                         std::is_same_v<T, common::ku_string_t> ||
                         std::is_same_v<T, std::string_view>);

enum class KUZU_API LogicalTypeID : uint8_t {
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
    FIXED_LIST = 41,

    INTERNAL_ID = 42,

    STRING = 50,
    BLOB = 51,

    VAR_LIST = 52,
    STRUCT = 53,
    MAP = 54,
    UNION = 55,
    RDF_VARIANT = 56,
    POINTER = 57,

    UUID = 58
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

    // Variable size types.
    STRING = 20,
    FIXED_LIST = 21,
    VAR_LIST = 22,
    STRUCT = 23,
    POINTER = 24,
};

class LogicalType;

class ExtraTypeInfo {
public:
    virtual ~ExtraTypeInfo() = default;

    inline void serialize(Serializer& serializer) const { serializeInternal(serializer); }

    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;

protected:
    virtual void serializeInternal(Serializer& serializer) const = 0;
};

class VarListTypeInfo : public ExtraTypeInfo {
public:
    VarListTypeInfo() = default;
    explicit VarListTypeInfo(std::unique_ptr<LogicalType> childType)
        : childType{std::move(childType)} {}
    inline LogicalType* getChildType() const { return childType.get(); }
    bool operator==(const VarListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

protected:
    void serializeInternal(Serializer& serializer) const override;

protected:
    std::unique_ptr<LogicalType> childType;
};

class FixedListTypeInfo : public VarListTypeInfo {
public:
    FixedListTypeInfo() = default;
    explicit FixedListTypeInfo(
        std::unique_ptr<LogicalType> childType, uint64_t fixedNumElementsInList)
        : VarListTypeInfo{std::move(childType)}, fixedNumElementsInList{fixedNumElementsInList} {}
    inline uint64_t getNumValuesInList() const { return fixedNumElementsInList; }
    bool operator==(const FixedListTypeInfo& other) const;
    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    uint64_t fixedNumElementsInList;
};

class StructField {
public:
    StructField() : type{std::make_unique<LogicalType>()} {}
    StructField(std::string name, std::unique_ptr<LogicalType> type)
        : name{std::move(name)}, type{std::move(type)} {};

    inline bool operator!=(const StructField& other) const { return !(*this == other); }
    inline std::string getName() const { return name; }
    inline LogicalType* getType() const { return type.get(); }

    bool operator==(const StructField& other) const;

    void serialize(Serializer& serializer) const;

    static StructField deserialize(Deserializer& deserializer);

    StructField copy() const;

private:
    std::string name;
    std::unique_ptr<LogicalType> type;
};

class StructTypeInfo : public ExtraTypeInfo {
public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<StructField>&& fields);
    StructTypeInfo(const std::vector<std::string>& fieldNames,
        const std::vector<std::unique_ptr<LogicalType>>& fieldTypes);

    bool hasField(const std::string& fieldName) const;
    struct_field_idx_t getStructFieldIdx(std::string fieldName) const;
    const StructField* getStructField(struct_field_idx_t idx) const;
    const StructField* getStructField(const std::string& fieldName) const;
    LogicalType* getChildType(struct_field_idx_t idx) const;
    std::vector<LogicalType*> getChildrenTypes() const;
    std::vector<std::string> getChildrenNames() const;
    std::vector<const StructField*> getStructFields() const;
    bool operator==(const kuzu::common::StructTypeInfo& other) const;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    std::vector<StructField> fields;
    std::unordered_map<std::string, struct_field_idx_t> fieldNameToIdxMap;
};

class LogicalType {
    friend class LogicalTypeUtils;
    friend struct StructType;
    friend struct VarListType;
    friend struct FixedListType;

public:
    KUZU_API LogicalType() : typeID{LogicalTypeID::ANY}, extraTypeInfo{nullptr} {
        physicalType = getPhysicalType(this->typeID);
    };
    explicit KUZU_API LogicalType(LogicalTypeID typeID);
    KUZU_API LogicalType(const LogicalType& other);
    KUZU_API LogicalType(LogicalType&& other) = default;

    KUZU_API LogicalType& operator=(const LogicalType& other);

    KUZU_API bool operator==(const LogicalType& other) const;

    KUZU_API bool operator!=(const LogicalType& other) const;

    KUZU_API LogicalType& operator=(LogicalType&& other) = default;

    KUZU_API std::string toString() const;

    KUZU_API inline LogicalTypeID getLogicalTypeID() const { return typeID; }

    inline PhysicalTypeID getPhysicalType() const { return physicalType; }
    static PhysicalTypeID getPhysicalType(LogicalTypeID logicalType);

    inline bool hasExtraTypeInfo() const { return extraTypeInfo != nullptr; }
    inline void setExtraTypeInfo(std::unique_ptr<ExtraTypeInfo> typeInfo) {
        extraTypeInfo = std::move(typeInfo);
    }

    void serialize(Serializer& serializer) const;

    static std::unique_ptr<LogicalType> deserialize(Deserializer& deserializer);

    std::unique_ptr<LogicalType> copy() const;

    static std::vector<std::unique_ptr<LogicalType>> copy(
        const std::vector<std::unique_ptr<LogicalType>>& types);

    static std::unique_ptr<LogicalType> ANY() {
        return std::make_unique<LogicalType>(LogicalTypeID::ANY);
    }
    static std::unique_ptr<LogicalType> BOOL() {
        return std::make_unique<LogicalType>(LogicalTypeID::BOOL);
    }
    static std::unique_ptr<LogicalType> INT64() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT64);
    }
    static std::unique_ptr<LogicalType> INT32() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT32);
    }
    static std::unique_ptr<LogicalType> INT16() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT16);
    }
    static std::unique_ptr<LogicalType> INT8() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT8);
    }
    static std::unique_ptr<LogicalType> UINT64() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT64);
    }
    static std::unique_ptr<LogicalType> UINT32() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT32);
    }
    static std::unique_ptr<LogicalType> UINT16() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT16);
    }
    static std::unique_ptr<LogicalType> UINT8() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT8);
    }
    static std::unique_ptr<LogicalType> INT128() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT128);
    }
    static std::unique_ptr<LogicalType> DOUBLE() {
        return std::make_unique<LogicalType>(LogicalTypeID::DOUBLE);
    }
    static std::unique_ptr<LogicalType> FLOAT() {
        return std::make_unique<LogicalType>(LogicalTypeID::FLOAT);
    }
    static std::unique_ptr<LogicalType> DATE() {
        return std::make_unique<LogicalType>(LogicalTypeID::DATE);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_NS() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_NS);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_MS() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_MS);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_SEC() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_SEC);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_TZ() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_TZ);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
    }
    static std::unique_ptr<LogicalType> INTERVAL() {
        return std::make_unique<LogicalType>(LogicalTypeID::INTERVAL);
    }
    static std::unique_ptr<LogicalType> INTERNAL_ID() {
        return std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID);
    }
    static std::unique_ptr<LogicalType> SERIAL() {
        return std::make_unique<LogicalType>(LogicalTypeID::SERIAL);
    }
    static std::unique_ptr<LogicalType> STRING() {
        return std::make_unique<LogicalType>(LogicalTypeID::STRING);
    }
    static std::unique_ptr<LogicalType> BLOB() {
        return std::make_unique<LogicalType>(LogicalTypeID::BLOB);
    }
    static std::unique_ptr<LogicalType> UUID() {
        return std::make_unique<LogicalType>(LogicalTypeID::UUID);
    }
    static std::unique_ptr<LogicalType> POINTER() {
        return std::make_unique<LogicalType>(LogicalTypeID::POINTER);
    }
    static KUZU_API std::unique_ptr<LogicalType> STRUCT(std::vector<StructField>&& fields);

    static KUZU_API std::unique_ptr<LogicalType> RECURSIVE_REL(
        std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> NODE(std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> REL(std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> RDF_VARIANT();

    static KUZU_API std::unique_ptr<LogicalType> UNION(std::vector<StructField>&& fields);

    static KUZU_API std::unique_ptr<LogicalType> VAR_LIST(std::unique_ptr<LogicalType> childType);
    template<class T>
    static inline std::unique_ptr<LogicalType> VAR_LIST(T&& childType) {
        return LogicalType::VAR_LIST(std::make_unique<LogicalType>(std::forward<T>(childType)));
    }

    static KUZU_API std::unique_ptr<LogicalType> MAP(
        std::unique_ptr<LogicalType> keyType, std::unique_ptr<LogicalType> valueType);
    template<class T>
    static inline std::unique_ptr<LogicalType> MAP(T&& keyType, T&& valueType) {
        return LogicalType::MAP(std::make_unique<LogicalType>(std::forward<T>(keyType)),
            std::make_unique<LogicalType>(std::forward<T>(valueType)));
    }

    static KUZU_API std::unique_ptr<LogicalType> FIXED_LIST(
        std::unique_ptr<LogicalType> childType, uint64_t fixedNumElementsInList);

private:
    friend struct CAPIHelper;
    friend struct JavaAPIHelper;
    friend class kuzu::processor::ParquetReader;
    explicit LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo);

private:
    LogicalTypeID typeID;
    PhysicalTypeID physicalType;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
};

using logical_types_t = std::vector<std::unique_ptr<LogicalType>>;

struct VarListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(type->extraTypeInfo.get());
        return varListTypeInfo->getChildType();
    }
};

struct FixedListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getChildType();
    }

    static inline uint64_t getNumValuesInList(const LogicalType* type) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getNumValuesInList();
    }
};

struct NodeType {
    static inline void setExtraTypeInfo(
        LogicalType& type, std::unique_ptr<ExtraTypeInfo> extraTypeInfo) {
        KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::NODE);
        type.setExtraTypeInfo(std::move(extraTypeInfo));
    }
};

struct RelType {
    static inline void setExtraTypeInfo(
        LogicalType& type, std::unique_ptr<ExtraTypeInfo> extraTypeInfo) {
        KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::REL);
        type.setExtraTypeInfo(std::move(extraTypeInfo));
    }
};

struct StructType {
    static inline std::vector<LogicalType*> getFieldTypes(const LogicalType* type) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenTypes();
    }

    static inline std::vector<std::string> getFieldNames(const LogicalType* type) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenNames();
    }

    static inline uint64_t getNumFields(const LogicalType* type) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        return getFieldTypes(type).size();
    }

    static inline std::vector<const StructField*> getFields(const LogicalType* type) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFields();
    }

    static inline bool hasField(const LogicalType* type, const std::string& key) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->hasField(key);
    }

    static inline const StructField* getField(const LogicalType* type, struct_field_idx_t idx) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructField(idx);
    }

    static inline const StructField* getField(const LogicalType* type, const std::string& key) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructField(key);
    }

    static inline struct_field_idx_t getFieldIdx(const LogicalType* type, const std::string& key) {
        KU_ASSERT(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFieldIdx(key);
    }
};

struct MapType {
    static inline LogicalType* getKeyType(const LogicalType* type) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::MAP);
        return StructType::getFieldTypes(VarListType::getChildType(type))[0];
    }

    static inline LogicalType* getValueType(const LogicalType* type) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::MAP);
        return StructType::getFieldTypes(VarListType::getChildType(type))[1];
    }
};

struct UnionType {
    static constexpr union_field_idx_t TAG_FIELD_IDX = 0;

    static constexpr LogicalTypeID TAG_FIELD_TYPE = LogicalTypeID::INT8;

    static constexpr char TAG_FIELD_NAME[] = "tag";

    static inline union_field_idx_t getInternalFieldIdx(union_field_idx_t idx) { return idx + 1; }

    static inline std::string getFieldName(const LogicalType* type, union_field_idx_t idx) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::UNION);
        return StructType::getFieldNames(type)[getInternalFieldIdx(idx)];
    }

    static inline LogicalType* getFieldType(const LogicalType* type, union_field_idx_t idx) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::UNION);
        return StructType::getFieldTypes(type)[getInternalFieldIdx(idx)];
    }

    static inline uint64_t getNumFields(const LogicalType* type) {
        KU_ASSERT(type->getLogicalTypeID() == LogicalTypeID::UNION);
        return StructType::getNumFields(type) - 1;
    }
};

struct PhysicalTypeUtils {
    static std::string physicalTypeToString(PhysicalTypeID physicalType);
    static uint32_t getFixedTypeSize(PhysicalTypeID physicalType);
};

class LogicalTypeUtils {
public:
    KUZU_API static std::string toString(LogicalTypeID dataTypeID);
    static std::string toString(const std::vector<LogicalType>& dataTypes);
    KUZU_API static std::string toString(const std::vector<LogicalTypeID>& dataTypeIDs);
    KUZU_API static LogicalType dataTypeFromString(const std::string& dataTypeString);
    static uint32_t getRowLayoutSize(const LogicalType& logicalType);
    static bool isNumerical(const LogicalType& dataType);
    static bool isNested(const LogicalType& dataType);
    static bool isNested(LogicalTypeID logicalTypeID);
    static std::vector<LogicalTypeID> getAllValidComparableLogicalTypes();
    static std::vector<LogicalTypeID> getNumericalLogicalTypeIDs();
    static std::vector<LogicalTypeID> getIntegerLogicalTypeIDs();
    static std::vector<LogicalTypeID> getAllValidLogicTypes();

private:
    static LogicalTypeID dataTypeIDFromString(const std::string& trimmedStr);
    static std::vector<std::string> parseStructFields(const std::string& structTypeStr);
    static std::unique_ptr<LogicalType> parseVarListType(const std::string& trimmedStr);
    static std::unique_ptr<LogicalType> parseFixedListType(const std::string& trimmedStr);
    static std::vector<StructField> parseStructTypeInfo(const std::string& structTypeStr);
    static std::unique_ptr<LogicalType> parseStructType(const std::string& trimmedStr);
    static std::unique_ptr<LogicalType> parseMapType(const std::string& trimmedStr);
    static std::unique_ptr<LogicalType> parseUnionType(const std::string& trimmedStr);
};

enum class FileVersionType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

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

    double getDuration() {
        if (finished) {
            auto duration = stopTime - startTime;
            return (double)std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        }
        throw Exception("Timer is still running.");
    }

    uint64_t getElapsedTimeInMS() {
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


namespace kuzu {
namespace common {

template<typename FROM, typename TO>
TO ku_dynamic_cast(FROM old) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    TO newVal = dynamic_cast<TO>(old);
    if constexpr (std::is_pointer<FROM>()) {
        KU_ASSERT(newVal != nullptr);
    }
    return newVal;
#else
    return reinterpret_cast<TO>(old);
#endif
}

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
    void set(const std::vector<uint8_t*>& parameters, LogicalTypeID childTypeId);

public:
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace kuzu
#include <memory>


namespace kuzu {
namespace common {

class SelectionVector {
public:
    explicit SelectionVector(sel_t capacity) : selectedSize{0} {
        selectedPositionsBuffer = std::make_unique<sel_t[]>(capacity);
        resetSelectorToUnselected();
    }

    inline bool isUnfiltered() const {
        return selectedPositions == (sel_t*)&INCREMENTAL_SELECTED_POS;
    }
    inline void resetSelectorToUnselected() {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
    }
    inline void resetSelectorToUnselectedWithSize(sel_t size) {
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
        selectedSize = size;
    }
    inline void resetSelectorToValuePosBuffer() {
        selectedPositions = selectedPositionsBuffer.get();
    }
    inline void resetSelectorToValuePosBufferWithSize(sel_t size) {
        selectedPositions = selectedPositionsBuffer.get();
        selectedSize = size;
    }
    inline sel_t* getSelectedPositionsBuffer() { return selectedPositionsBuffer.get(); }

    KUZU_API static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

public:
    sel_t* selectedPositions;
    // TODO: type of `selectedSize` was changed from `sel_t` to `uint64_t`, which should be reverted
    // when we removed arrow array in ValueVector. Currently, we need to keep size of arrow array,
    // which could be larger than MAX of `sel_t`.
    uint64_t selectedSize;

private:
    std::unique_ptr<sel_t[]> selectedPositionsBuffer;
};

} // namespace common
} // namespace kuzu

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>


namespace kuzu {
namespace common {
class VirtualFileSystem;
}

namespace storage {

class MemoryAllocator;
class BMFileHandle;
class BufferManager;

class MemoryBuffer {
public:
    MemoryBuffer(MemoryAllocator* allocator, common::page_idx_t blockIdx, uint8_t* buffer);
    ~MemoryBuffer();

public:
    uint8_t* buffer;
    common::page_idx_t pageIdx;
    MemoryAllocator* allocator;
};

class MemoryAllocator {
    friend class MemoryBuffer;

public:
    explicit MemoryAllocator(BufferManager* bm, common::VirtualFileSystem* vfs);
    ~MemoryAllocator();

    std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false);
    inline common::page_offset_t getPageSize() const { return pageSize; }

private:
    void freeBlock(common::page_idx_t pageIdx);

private:
    std::unique_ptr<BMFileHandle> fh;
    BufferManager* bm;
    common::page_offset_t pageSize;
    std::stack<common::page_idx_t> freePages;
    std::mutex allocatorLock;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory blocks.
 * It can allocate a memory buffer of size PAGE_256KB from the buffer manager backed by a
 * BMFileHandle with temp in-mem file.
 *
 * Internally, MM uses a MemoryAllocator. The MemoryAllocator is holding the BMFileHandle backed by
 * a temp in-mem file, and responsible for allocating/reclaiming memory buffers of its size class
 * from the buffer manager. The MemoryAllocator keeps track of free pages in the BMFileHandle, so
 * that it can reuse those freed pages without allocating new pages. The MemoryAllocator is
 * thread-safe, so that multiple threads can allocate/reclaim memory blocks with the same size class
 * at the same time.
 *
 * MM will return a MemoryBuffer to the caller, which is a wrapper of the allocated memory block,
 * and it will automatically call its allocator to reclaim the memory block when it is destroyed.
 */
class MemoryManager {
public:
    explicit MemoryManager(BufferManager* bm, common::VirtualFileSystem* vfs) : bm{bm} {
        allocator = std::make_unique<MemoryAllocator>(bm, vfs);
    }

    inline std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false) {
        return allocator->allocateBuffer(initializeToZero);
    }
    inline BufferManager* getBufferManager() const { return bm; }

private:
    BufferManager* bm;
    std::unique_ptr<MemoryAllocator> allocator;
};
} // namespace storage
} // namespace kuzu

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>


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

class Expression : public std::enable_shared_from_this<Expression> {
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
    Expression(
        common::ExpressionType expressionType, common::LogicalType dataType, std::string uniqueName)
        : Expression{
              expressionType, std::move(dataType), expression_vector{}, std::move(uniqueName)} {}
    DELETE_COPY_DEFAULT_MOVE(Expression);
    virtual ~Expression() = default;

    inline void setAlias(const std::string& name) { alias = name; }

    inline void setUniqueName(const std::string& name) { uniqueName = name; }
    inline std::string getUniqueName() const {
        KU_ASSERT(!uniqueName.empty());
        return uniqueName;
    }

    inline common::LogicalType getDataType() const { return dataType; }
    inline common::LogicalType& getDataTypeReference() { return dataType; }

    inline bool hasAlias() const { return !alias.empty(); }
    inline std::string getAlias() const { return alias; }

    inline uint32_t getNumChildren() const { return children.size(); }
    inline std::shared_ptr<Expression> getChild(common::vector_idx_t idx) const {
        return children[idx];
    }
    inline expression_vector getChildren() const { return children; }
    inline void setChild(common::vector_idx_t idx, std::shared_ptr<Expression> child) {
        children[idx] = std::move(child);
    }

    expression_vector splitOnAND();

    inline bool operator==(const Expression& rhs) const { return uniqueName == rhs.uniqueName; }

    std::string toString() const { return hasAlias() ? alias : toStringInternal(); }

    virtual std::unique_ptr<Expression> copy() const {
        throw common::InternalException("Unimplemented expression copy().");
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
    bool operator()(
        const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right) const {
        return left->getUniqueName() == right->getUniqueName();
    }
};

} // namespace binder
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
    KUZU_API explicit Value(std::unique_ptr<LogicalType> type, std::string val_);
    /**
     * @param dataType the logical type of the value.
     * @param children a vector of children values.
     */
    KUZU_API explicit Value(
        std::unique_ptr<LogicalType> dataType, std::vector<std::unique_ptr<Value>> children);
    /**
     * @param other the value to copy from.
     */
    KUZU_API Value(const Value& other);

    /**
     * @param other the value to move from.
     */
    KUZU_API Value(Value&& other) = default;
    KUZU_API Value& operator=(Value&& other) = default;

    /**
     * @brief Sets the data type of the Value.
     * @param dataType_ the data type to set to.
     */
    KUZU_API void setDataType(const LogicalType& dataType_);
    /**
     * @return the dataType of the value.
     */
    KUZU_API LogicalType* getDataType() const;
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

    void serialize(Serializer& serializer) const;

    static std::unique_ptr<Value> deserialize(Deserializer& deserializer);

private:
    Value();
    explicit Value(const LogicalType& dataType);

    void copyFromFixedList(const uint8_t* fixedList);
    void copyFromVarList(ku_list_t& list, const LogicalType& childType);
    void copyFromStruct(const uint8_t* kuStruct);
    void copyFromUnion(const uint8_t* kuUnion);

    std::string rdfVariantToString() const;
    std::string mapToString() const;
    std::string listToString() const;
    std::string structToString() const;
    std::string nodeToString() const;
    std::string relToString() const;

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
    std::unique_ptr<LogicalType> dataType;
    bool isNull_;

    // Note: ALWAYS use childrenSize over children.size(). We do NOT resize children when iterating
    // with nested value. So children.size() reflects the capacity() rather the actual size.
    std::vector<std::unique_ptr<Value>> children;
    uint32_t childrenSize;
};

/**
 * @return boolean value.
 */
template<>
KUZU_API inline bool Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return int8 value.
 */
template<>
KUZU_API inline int8_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT8);
    return val.int8Val;
}

/**
 * @return int16 value.
 */
template<>
KUZU_API inline int16_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return int32 value.
 */
template<>
KUZU_API inline int32_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return int64 value.
 */
template<>
KUZU_API inline int64_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return uint64 value.
 */
template<>
KUZU_API inline uint64_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT64);
    return val.uint64Val;
}

/**
 * @return uint32 value.
 */
template<>
KUZU_API inline uint32_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT32);
    return val.uint32Val;
}

/**
 * @return uint16 value.
 */
template<>
KUZU_API inline uint16_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT16);
    return val.uint16Val;
}

/**
 * @return uint8 value.
 */
template<>
KUZU_API inline uint8_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT8);
    return val.uint8Val;
}

/**
 * @return int128 value.
 */
template<>
KUZU_API inline int128_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT128 ||
              dataType->getLogicalTypeID() == LogicalTypeID::UUID);
    return val.int128Val;
}

/**
 * @return float value.
 */
template<>
KUZU_API inline float Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return double value.
 */
template<>
KUZU_API inline double Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return date_t value.
 */
template<>
KUZU_API inline date_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::DATE);
    return date_t{val.int32Val};
}

/**
 * @return timestamp_t value.
 */
template<>
KUZU_API inline timestamp_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return timestamp_t{val.int64Val};
}

/**
 * @return timestamp_ns_t value.
 */
template<>
KUZU_API inline timestamp_ns_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_NS);
    return timestamp_ns_t{val.int64Val};
}

/**
 * @return timestamp_ms_t value.
 */
template<>
KUZU_API inline timestamp_ms_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_MS);
    return timestamp_ms_t{val.int64Val};
}

/**
 * @return timestamp_sec_t value.
 */
template<>
KUZU_API inline timestamp_sec_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_SEC);
    return timestamp_sec_t{val.int64Val};
}

/**
 * @return timestamp_tz_t value.
 */
template<>
KUZU_API inline timestamp_tz_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_TZ);
    return timestamp_tz_t{val.int64Val};
}

/**
 * @return interval_t value.
 */
template<>
KUZU_API inline interval_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return internal_t value.
 */
template<>
KUZU_API inline internalID_t Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return string value.
 */
template<>
KUZU_API inline std::string Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::STRING ||
              dataType->getLogicalTypeID() == LogicalTypeID::BLOB ||
              dataType->getLogicalTypeID() == LogicalTypeID::UUID);
    return strVal;
}

/**
 * @return uint8_t* value.
 */
template<>
KUZU_API inline uint8_t* Value::getValue() const {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::POINTER);
    return val.pointer;
}

/**
 * @return the reference to the boolean value.
 */
template<>
KUZU_API inline bool& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return the reference to the int8 value.
 */
template<>
KUZU_API inline int8_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT8);
    return val.int8Val;
}

/**
 * @return the reference to the int16 value.
 */
template<>
KUZU_API inline int16_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return the reference to the int32 value.
 */
template<>
KUZU_API inline int32_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return the reference to the int64 value.
 */
template<>
KUZU_API inline int64_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return the reference to the uint8 value.
 */
template<>
KUZU_API inline uint8_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT8);
    return val.uint8Val;
}

/**
 * @return the reference to the uint16 value.
 */
template<>
KUZU_API inline uint16_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT16);
    return val.uint16Val;
}

/**
 * @return the reference to the uint32 value.
 */
template<>
KUZU_API inline uint32_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT32);
    return val.uint32Val;
}

/**
 * @return the reference to the uint64 value.
 */
template<>
KUZU_API inline uint64_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::UINT64);
    return val.uint64Val;
}

/**
 * @return the reference to the int128 value.
 */
template<>
KUZU_API inline int128_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INT128);
    return val.int128Val;
}

/**
 * @return the reference to the float value.
 */
template<>
KUZU_API inline float& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return the reference to the double value.
 */
template<>
KUZU_API inline double& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return the reference to the date value.
 */
template<>
KUZU_API inline date_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::DATE);
    return *reinterpret_cast<date_t*>(&val.int32Val);
}

/**
 * @return the reference to the timestamp value.
 */
template<>
KUZU_API inline timestamp_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return *reinterpret_cast<timestamp_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_ms value.
 */
template<>
KUZU_API inline timestamp_ms_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_MS);
    return *reinterpret_cast<timestamp_ms_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_ns value.
 */
template<>
KUZU_API inline timestamp_ns_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_NS);
    return *reinterpret_cast<timestamp_ns_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_sec value.
 */
template<>
KUZU_API inline timestamp_sec_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_SEC);
    return *reinterpret_cast<timestamp_sec_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_tz value.
 */
template<>
KUZU_API inline timestamp_tz_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::TIMESTAMP_TZ);
    return *reinterpret_cast<timestamp_tz_t*>(&val.int64Val);
}

/**
 * @return the reference to the interval value.
 */
template<>
KUZU_API inline interval_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return the reference to the internal_id value.
 */
template<>
KUZU_API inline nodeID_t& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return the reference to the string value.
 */
template<>
KUZU_API inline std::string& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return the reference to the uint8_t* value.
 */
template<>
KUZU_API inline uint8_t*& Value::getValueReference() {
    KU_ASSERT(dataType->getLogicalTypeID() == LogicalTypeID::POINTER);
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
namespace common {

// F stands for Factorization
enum class FStateType : uint8_t {
    FLAT = 0,
    UNFLAT = 1,
};

class DataChunkState {
public:
    DataChunkState() : DataChunkState(DEFAULT_VECTOR_CAPACITY) {}
    explicit DataChunkState(uint64_t capacity) : fStateType{FStateType::UNFLAT}, originalSize{0} {
        selVector = std::make_shared<SelectionVector>(capacity);
    }

    // returns a dataChunkState for vectors holding a single value.
    static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

    inline void initOriginalAndSelectedSize(uint64_t size) {
        originalSize = size;
        selVector->selectedSize = size;
    }
    inline void setOriginalSize(uint64_t size) { originalSize = size; }
    inline uint64_t getOriginalSize() const { return originalSize; }
    inline bool isFlat() const { return fStateType == FStateType::FLAT; }
    inline void setToFlat() { fStateType = FStateType::FLAT; }
    inline void setToUnflat() { fStateType = FStateType::UNFLAT; }

    inline uint64_t getNumSelectedValues() const { return selVector->selectedSize; }

    void slice(offset_t offset);

public:
    std::shared_ptr<SelectionVector> selVector;

private:
    FStateType fStateType;
    // We need to keep track of originalSize of DataChunks to perform consistent scans of vectors
    // or lists. This is because all the vectors in a data chunk has to be the same length as they
    // share the same selectedPositions array.Therefore, if there is a scan after a filter on the
    // data chunk, the selectedSize of selVector might decrease, so the scan cannot know how much it
    // has to scan to generate a vector that is consistent with the rest of the vectors in the
    // data chunk.
    uint64_t originalSize;
};

} // namespace common
} // namespace kuzu

#include <iterator>
#include <vector>


namespace kuzu {
namespace common {

struct BufferBlock {
public:
    explicit BufferBlock(std::unique_ptr<storage::MemoryBuffer> block)
        : size{block->allocator->getPageSize()}, currentOffset{0}, block{std::move(block)} {}

public:
    uint64_t size;
    uint64_t currentOffset;
    std::unique_ptr<storage::MemoryBuffer> block;

    inline void resetCurrentOffset() { currentOffset = 0; }
};

class InMemOverflowBuffer {

public:
    explicit InMemOverflowBuffer(storage::MemoryManager* memoryManager)
        : memoryManager{memoryManager}, currentBlock{nullptr} {};

    uint8_t* allocateSpace(uint64_t size);

    inline void merge(InMemOverflowBuffer& other) {
        move(begin(other.blocks), end(other.blocks), back_inserter(blocks));
        // We clear the other InMemOverflowBuffer's block because when it is deconstructed,
        // InMemOverflowBuffer's deconstructed tries to free these pages by calling
        // memoryManager->freeBlock, but it should not because this InMemOverflowBuffer still
        // needs them.
        other.blocks.clear();
        currentBlock = other.currentBlock;
    }

    // Releases all memory accumulated for string overflows so far and re-initializes its state to
    // an empty buffer. If there is a large string that used point to any of these overflow buffers
    // they will error.
    inline void resetBuffer() {
        if (!blocks.empty()) {
            auto firstBlock = std::move(blocks[0]);
            blocks.clear();
            firstBlock->resetCurrentOffset();
            blocks.push_back(std::move(firstBlock));
        }
        if (!blocks.empty()) {
            currentBlock = blocks[0].get();
        }
    }

private:
    inline bool requireNewBlock(uint64_t sizeToAllocate) {
        if (sizeToAllocate > BufferPoolConstants::PAGE_256KB_SIZE) {
            throw RuntimeException("Required size " + std::to_string(sizeToAllocate) +
                                   " is greater than the single block size of " +
                                   std::to_string(BufferPoolConstants::PAGE_256KB_SIZE) + ".");
        }
        return currentBlock == nullptr ||
               (currentBlock->currentOffset + sizeToAllocate) > currentBlock->size;
    }

    void allocateNewBlock();

private:
    std::vector<std::unique_ptr<BufferBlock>> blocks;
    storage::MemoryManager* memoryManager;
    BufferBlock* currentBlock;
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace common {
class Value;
class RdfVariant {
public:
    /**
     * @brief Get the logical type id of the rdf variant.
     * @param rdfVariant the rdf variant.
     * @return the logical type id.
     */
    KUZU_API static LogicalTypeID getLogicalTypeID(const Value* rdfVariant);

    /**
     * @brief Get the value of the rdf variant.
     * @tparam T the type of the value.
     * @param rdfVariant the rdf variant.
     * @return the value.
     */
    template<typename T>
    T static getValue(const Value* rdfVariant) {
        auto blobData = NestedVal::getChildVal(rdfVariant, 1)->strVal.data();
        return Blob::getValue<T>(blobData);
    }
};

/**
 * @brief Specialization for string.
 * @param rdfVariant the rdf variant.
 * @return the string value.
 */
template<>
KUZU_API inline std::string RdfVariant::getValue<std::string>(const Value* rdfVariant) {
    return NestedVal::getChildVal(rdfVariant, 1)->strVal;
}
} // namespace common
} // namespace kuzu

#include <sstream>


namespace kuzu {
namespace common {

struct CSVOption {
    // TODO(Xiyang): Add newline character option and delimiter can be a string.
    char escapeChar;
    char delimiter;
    char quoteChar;
    bool hasHeader;

    CSVOption()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVOption);

    std::string toCypher() const {
        std::stringstream ss;
        ss << " (escape = '\\" << escapeChar << "' , delim = '" << delimiter << "' , quote = '\\"
           << quoteChar << "', header=";
        if (hasHeader) {
            ss << "true);";
        } else {
            ss << "false);";
        }
        return ss.str();
    }

private:
    CSVOption(const CSVOption& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          hasHeader{other.hasHeader} {}
};

struct CSVReaderConfig {
    CSVOption option;
    bool parallel;

    CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig);

    static CSVReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);

private:
    CSVReaderConfig(const CSVReaderConfig& other)
        : option{other.option.copy()}, parallel{other.parallel} {}
};

} // namespace common
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


namespace arrow {
class ChunkedArray;
} // namespace arrow

namespace kuzu {
namespace common {

class ValueVector;

// AuxiliaryBuffer holds data which is only used by the targeting dataType.
class AuxiliaryBuffer {
public:
    virtual ~AuxiliaryBuffer() = default;
};

class StringAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    explicit StringAuxiliaryBuffer(storage::MemoryManager* memoryManager) {
        inMemOverflowBuffer = std::make_unique<InMemOverflowBuffer>(memoryManager);
    }

    inline InMemOverflowBuffer* getOverflowBuffer() const { return inMemOverflowBuffer.get(); }
    inline uint8_t* allocateOverflow(uint64_t size) {
        return inMemOverflowBuffer->allocateSpace(size);
    }
    inline void resetOverflowBuffer() const { inMemOverflowBuffer->resetBuffer(); }

private:
    std::unique_ptr<InMemOverflowBuffer> inMemOverflowBuffer;
};

class StructAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    StructAuxiliaryBuffer(const LogicalType& type, storage::MemoryManager* memoryManager);

    inline void referenceChildVector(
        vector_idx_t idx, std::shared_ptr<ValueVector> vectorToReference) {
        childrenVectors[idx] = std::move(vectorToReference);
    }
    inline const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors() const {
        return childrenVectors;
    }

private:
    std::vector<std::shared_ptr<ValueVector>> childrenVectors;
};

class ArrowColumnAuxiliaryBuffer : public AuxiliaryBuffer {
    friend class ArrowColumnVector;

private:
    std::shared_ptr<arrow::ChunkedArray> column;
};

// ListVector layout:
// To store a list value in the valueVector, we could use two separate vectors.
// 1. A vector(called offset vector) for the list offsets and length(called list_entry_t): This
// vector contains the starting indices and length for each list within the data vector.
// 2. A data vector(called dataVector) to store the actual list elements: This vector holds the
// actual elements of the lists in a flat, continuous storage. Each list would be represented as a
// contiguous subsequence of elements in this vector.
class ListAuxiliaryBuffer : public AuxiliaryBuffer {
    friend class ListVector;

public:
    ListAuxiliaryBuffer(const LogicalType& dataVectorType, storage::MemoryManager* memoryManager);

    inline void setDataVector(std::shared_ptr<ValueVector> vector) {
        dataVector = std::move(vector);
    }
    inline ValueVector* getDataVector() const { return dataVector.get(); }
    inline std::shared_ptr<ValueVector> getSharedDataVector() const { return dataVector; }

    list_entry_t addList(uint64_t listSize);

    inline uint64_t getSize() const { return size; }

    inline void resetSize() { size = 0; }

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
    static std::unique_ptr<AuxiliaryBuffer> getAuxiliaryBuffer(
        LogicalType& type, storage::MemoryManager* memoryManager);
};

} // namespace common
} // namespace kuzu


namespace kuzu {
namespace function {

struct FunctionBindData {
    std::unique_ptr<common::LogicalType> resultType;

    explicit FunctionBindData(std::unique_ptr<common::LogicalType> dataType)
        : resultType{std::move(dataType)} {}

    virtual ~FunctionBindData() = default;
};

struct CastFunctionBindData : public FunctionBindData {
    common::CSVReaderConfig csvConfig;
    uint64_t numOfEntries;

    explicit CastFunctionBindData(std::unique_ptr<common::LogicalType> dataType)
        : FunctionBindData{std::move(dataType)} {}
};

struct Function;
using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const binder::expression_vector&, Function* definition)>;

enum class FunctionType : uint8_t { SCALAR, AGGREGATE, TABLE };

struct Function {
    Function(
        FunctionType type, std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs)
        : type{type}, name{std::move(name)}, parameterTypeIDs{std::move(parameterTypeIDs)} {}

    virtual ~Function() = default;

    virtual std::string signatureToString() const = 0;

    virtual std::unique_ptr<Function> copy() const = 0;

    // TODO(Ziyi): Move to catalog entry once we have implemented the catalog entry.
    FunctionType type;
    std::string name;
    std::vector<common::LogicalTypeID> parameterTypeIDs;
};

struct BaseScalarFunction : public Function {
    BaseScalarFunction(FunctionType type, std::string name,
        std::vector<common::LogicalTypeID> parameterTypeIDs, common::LogicalTypeID returnTypeID,
        scalar_bind_func bindFunc)
        : Function{type, std::move(name), std::move(parameterTypeIDs)},
          returnTypeID{returnTypeID}, bindFunc{std::move(bindFunc)} {}

    inline std::string signatureToString() const override {
        std::string result = common::LogicalTypeUtils::toString(parameterTypeIDs);
        result += " -> " + common::LogicalTypeUtils::toString(returnTypeID);
        return result;
    }

    common::LogicalTypeID returnTypeID;
    // This function is used to bind parameter/return types for functions with nested dataType.
    scalar_bind_func bindFunc;
};

} // namespace function
} // namespace kuzu

#include <string>


namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(common::LogicalTypeID typeID, std::string name)
        : typeID{typeID}, name{std::move(name)}, numValuesPerList{0} {}

    common::LogicalTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;
    // Used by fixedList only.
    uint64_t numValuesPerList;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const common::LogicalType& type, const std::string& name);
};

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
    friend class Connection;
    class QueryResultIterator {
    private:
        QueryResult* currentResult;

    public:
        explicit QueryResultIterator(QueryResult* startResult) : currentResult(startResult) {}

        void operator++() {
            if (currentResult) {
                currentResult = currentResult->nextQueryResult.get();
            }
        }

        bool isEnd() { return currentResult == nullptr; }

        QueryResult* getCurrentResult() { return currentResult; }
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

    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo() const;
    /**
     * @return whether there are more tuples to read.
     */
    KUZU_API bool hasNext() const;
    std::unique_ptr<QueryResult> nextQueryResult;

    std::string toSingleQueryString();
    /**
     * @return next flat tuple in the query result.
     */
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();
    /**
     * @return string of query result.
     */
    KUZU_API std::string toString();

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
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const std::vector<std::shared_ptr<binder::Expression>>& columns);
    void validateQuerySucceed() const;

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

#include <numeric>
#include <utility>


namespace kuzu {
namespace common {

class Value;

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {
    friend class ListVector;
    friend class FixedListVector;
    friend class ListAuxiliaryBuffer;
    friend class StructVector;
    friend class StringVector;
    friend class ArrowColumnVector;

public:
    explicit ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager = nullptr);
    explicit ValueVector(LogicalTypeID dataTypeID, storage::MemoryManager* memoryManager = nullptr)
        : ValueVector(LogicalType(dataTypeID), memoryManager) {
        KU_ASSERT(dataTypeID != LogicalTypeID::VAR_LIST);
    }

    KUZU_API ~ValueVector() = default;

    void setState(const std::shared_ptr<DataChunkState>& state_);

    inline void setAllNull() { nullMask->setAllNull(); }
    inline void setAllNonNull() { nullMask->setAllNonNull(); }
    // On return true, there are no null. On return false, there may or may not be nulls.
    inline bool hasNoNullsGuarantee() const { return nullMask->hasNoNullsGuarantee(); }
    inline void setNullRange(uint32_t startPos, uint32_t len, bool value) {
        nullMask->setNullFromRange(startPos, len, value);
    }
    inline const uint64_t* getNullMaskData() { return nullMask->getData(); }
    KUZU_API void setNull(uint32_t pos, bool isNull);
    inline uint8_t isNull(uint32_t pos) const { return nullMask->isNull(pos); }
    inline void setAsSingleNullEntry() {
        state->selVector->selectedSize = 1;
        setNull(state->selVector->selectedPositions[0], true);
    }

    bool setNullFromBits(const uint64_t* srcNullEntries, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy);

    inline uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

    // TODO(Guodong): Rename this to getValueRef
    template<typename T>
    inline T& getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    KUZU_API void setValue(uint32_t pos, T val);
    // copyFromRowData assumes rowData is non-NULL.
    void copyFromRowData(uint32_t pos, const uint8_t* rowData);
    // copyToRowData assumes srcVectorData is non-NULL.
    void copyToRowData(
        uint32_t pos, uint8_t* rowData, InMemOverflowBuffer* rowOverflowBuffer) const;
    // copyFromVectorData assumes srcVectorData is non-NULL.
    void copyFromVectorData(
        uint8_t* dstData, const ValueVector* srcVector, const uint8_t* srcVectorData);
    void copyFromVectorData(uint64_t dstPos, const ValueVector* srcVector, uint64_t srcPos);
    void copyFromValue(uint64_t pos, const Value& value);

    std::unique_ptr<Value> getAsValue(uint64_t pos);

    inline uint8_t* getData() const { return valueBuffer.get(); }

    inline offset_t readNodeOffset(uint32_t pos) const {
        KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
        return getValue<nodeID_t>(pos).offset;
    }

    inline void setSequential() { _isSequential = true; }
    inline bool isSequential() const { return _isSequential; }

    KUZU_API void resetAuxiliaryBuffer();

    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);

private:
    uint32_t getDataTypeSize(const LogicalType& type);
    void initializeValueBuffer();

public:
    LogicalType dataType;
    std::shared_ptr<DataChunkState> state;

private:
    bool _isSequential = false;
    std::unique_ptr<uint8_t[]> valueBuffer;
    std::unique_ptr<NullMask> nullMask;
    uint32_t numBytesPerValue;
    std::unique_ptr<AuxiliaryBuffer> auxiliaryBuffer;
};

class StringVector {
public:
    static inline InMemOverflowBuffer* getInMemOverflowBuffer(ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
        return ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getOverflowBuffer();
    }

    static void addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, uint32_t vectorPos, const char* srcStr, uint64_t length);
    static void addString(ValueVector* vector, uint32_t vectorPos, const std::string& srcStr);
    // Add empty string with space reserved for the provided size
    // Returned value can be modified to set the string contents
    static ku_string_t& reserveString(ValueVector* vector, uint32_t vectorPos, uint64_t length);
    static void addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, ku_string_t& dstStr, const char* srcStr, uint64_t length);
    static void addString(
        kuzu::common::ValueVector* vector, ku_string_t& dstStr, const std::string& srcStr);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
};

struct BlobVector {
    static void addBlob(ValueVector* vector, uint32_t pos, const char* data, uint32_t length) {
        StringVector::addString(vector, pos, data, length);
    }
    static void addBlob(ValueVector* vector, uint32_t pos, const uint8_t* data, uint64_t length) {
        StringVector::addString(vector, pos, reinterpret_cast<const char*>(data), length);
    }
};

class ListVector {
public:
    static inline void setDataVector(
        const ValueVector* vector, std::shared_ptr<ValueVector> dataVector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto listBuffer =
            ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
        listBuffer->setDataVector(std::move(dataVector));
    }
    static inline ValueVector* getDataVector(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getDataVector();
    }
    static inline std::shared_ptr<ValueVector> getSharedDataVector(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getSharedDataVector();
    }
    static inline uint64_t getDataVectorSize(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getSize();
    }

    static inline uint8_t* getListValues(const ValueVector* vector, const list_entry_t& listEntry) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto dataVector = getDataVector(vector);
        return dataVector->getData() + dataVector->getNumBytesPerValue() * listEntry.offset;
    }
    static inline uint8_t* getListValuesWithOffset(
        const ValueVector* vector, const list_entry_t& listEntry, offset_t elementOffsetInList) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return getListValues(vector, listEntry) +
               elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
    }
    static inline list_entry_t addList(ValueVector* vector, uint64_t listSize) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->addList(listSize);
    }
    static inline void resizeDataVector(ValueVector* vector, uint64_t numValues) {
        ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->resize(numValues);
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
    static void appendDataVector(
        ValueVector* dstVector, ValueVector* srcDataVector, uint64_t numValuesToAppend);
    static void sliceDataVector(ValueVector* vectorToSlice, uint64_t offset, uint64_t numValues);
};

class FixedListVector {
public:
    template<typename T>
    static void getAsValue(ValueVector* vector, std::vector<std::unique_ptr<Value>>& children,
        uint64_t pos, uint64_t numElements);
};

template<>
void FixedListVector::getAsValue<int64_t>(ValueVector* vector,
    std::vector<std::unique_ptr<Value>>& children, uint64_t pos, uint64_t numElements);
template<>
void FixedListVector::getAsValue<int32_t>(ValueVector* vector,
    std::vector<std::unique_ptr<Value>>& children, uint64_t pos, uint64_t numElements);
template<>
void FixedListVector::getAsValue<int16_t>(ValueVector* vector,
    std::vector<std::unique_ptr<Value>>& children, uint64_t pos, uint64_t numElements);
template<>
void FixedListVector::getAsValue<double>(ValueVector* vector,
    std::vector<std::unique_ptr<Value>>& children, uint64_t pos, uint64_t numElements);
template<>
void FixedListVector::getAsValue<float>(ValueVector* vector,
    std::vector<std::unique_ptr<Value>>& children, uint64_t pos, uint64_t numElements);

class StructVector {
public:
    static inline const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors(
        const ValueVector* vector) {
        return ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getFieldVectors();
    }

    static inline std::shared_ptr<ValueVector> getFieldVector(
        const ValueVector* vector, struct_field_idx_t idx) {
        return ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getFieldVectors()[idx];
    }

    static inline void referenceVector(ValueVector* vector, struct_field_idx_t idx,
        std::shared_ptr<ValueVector> vectorToReference) {
        ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->referenceChildVector(idx, std::move(vectorToReference));
    }

    static inline void initializeEntries(ValueVector* vector) {
        std::iota(reinterpret_cast<int64_t*>(vector->getData()),
            reinterpret_cast<int64_t*>(
                vector->getData() + vector->getNumBytesPerValue() * DEFAULT_VECTOR_CAPACITY),
            0);
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
        StructVector::referenceVector(
            vector, UnionType::getInternalFieldIdx(fieldIdx), std::move(vectorToReference));
    }

    static inline void setTagField(ValueVector* vector, union_field_idx_t tag) {
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            vector->setValue<struct_field_idx_t>(
                vector->state->selVector->selectedPositions[i], tag);
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

struct RdfVariantVector {
    static void addString(ValueVector* vector, sel_t pos, ku_string_t str);
    static void addString(ValueVector* vector, sel_t pos, const char* str, uint32_t length);

    template<typename T>
    static void add(ValueVector* vector, sel_t pos, T val);
};

} // namespace common
} // namespace kuzu

#include <map>


namespace kuzu {
namespace storage {
class TableData;

using offset_to_row_idx_t = std::map<common::offset_t, common::row_idx_t>;
using offset_set_t = std::unordered_set<common::offset_t>;
using update_insert_info_t = std::map<common::offset_t, offset_to_row_idx_t>;
using delete_info_t = std::map<common::offset_t, std::unordered_set<common::offset_t>>;

// TODO(Guodong): Instead of using ValueVector, we should switch to ColumnChunk.
// This class is used to store a chunk of local changes to a column in a node group.
// Values are stored inside `vector`.
class LocalVector {
public:
    LocalVector(const common::LogicalType& dataType, MemoryManager* mm) : numValues{0} {
        vector = std::make_unique<common::ValueVector>(dataType, mm);
        vector->setState(std::make_shared<common::DataChunkState>());
        vector->state->selVector->resetSelectorToValuePosBufferWithSize(1);
    }

    void read(common::sel_t offsetInLocalVector, common::ValueVector* resultVector,
        common::sel_t offsetInResultVector);
    void append(common::ValueVector* valueVector);

    inline common::ValueVector* getVector() { return vector.get(); }
    inline bool isFull() const { return numValues == common::DEFAULT_VECTOR_CAPACITY; }

private:
    std::unique_ptr<common::ValueVector> vector;
    common::sel_t numValues;
};

// This class is used to store local changes of a column in a node group.
// It consists of a collection of LocalVector, each of which is a chunk of the local changes.
// By default, the size of each vector (chunk) is DEFAULT_VECTOR_CAPACITY, and the collection
// contains 64 vectors (chunks).
class LocalVectorCollection {
public:
    LocalVectorCollection(std::unique_ptr<common::LogicalType> dataType, MemoryManager* mm)
        : dataType{std::move(dataType)}, mm{mm}, numRows{0} {}

    void read(common::row_idx_t rowIdx, common::ValueVector* outputVector,
        common::sel_t posInOutputVector);
    inline uint64_t getNumRows() const { return numRows; }
    inline LocalVector* getLocalVector(common::row_idx_t rowIdx) {
        auto vectorIdx = rowIdx >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
        KU_ASSERT(vectorIdx < vectors.size());
        return vectors[vectorIdx].get();
    }

    std::unique_ptr<LocalVectorCollection> getStructChildVectorCollection(
        common::struct_field_idx_t idx);

    // TODO(Guodong): Change this interface to take an extra `SelVector` or `DataChunkState`.
    common::row_idx_t append(common::ValueVector* vector);

private:
    void prepareAppend();

private:
    std::unique_ptr<common::LogicalType> dataType;
    MemoryManager* mm;
    std::vector<std::unique_ptr<LocalVector>> vectors;
    common::row_idx_t numRows;
};

class LocalNodeGroup {
    friend class NodeTableData;

public:
    LocalNodeGroup(common::offset_t nodeGroupStartOffset,
        std::vector<common::LogicalType*> dataTypes, MemoryManager* mm);
    virtual ~LocalNodeGroup() = default;

    inline LocalVectorCollection* getLocalColumnChunk(common::column_id_t columnID) {
        return chunks[columnID].get();
    }

protected:
    common::offset_t nodeGroupStartOffset;
    std::vector<std::unique_ptr<LocalVectorCollection>> chunks;
};

class LocalTableData {
    friend class NodeTableData;

public:
    LocalTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm,
        common::ColumnDataFormat dataFormat)
        : dataTypes{std::move(dataTypes)}, mm{mm}, dataFormat{dataFormat} {}
    virtual ~LocalTableData() = default;

    inline void clear() { nodeGroups.clear(); }

protected:
    virtual LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) = 0;

protected:
    std::vector<common::LogicalType*> dataTypes;
    MemoryManager* mm;
    common::ColumnDataFormat dataFormat;
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> nodeGroups;
};

class Column;
class LocalTable {
public:
    explicit LocalTable(common::TableType tableType) : tableType{tableType} {};

    LocalTableData* getOrCreateLocalTableData(const std::vector<std::unique_ptr<Column>>& columns,
        MemoryManager* mm, common::ColumnDataFormat dataFormat = common::ColumnDataFormat::REGULAR,
        common::vector_idx_t dataIdx = 0);
    inline LocalTableData* getLocalTableData(common::vector_idx_t dataIdx) {
        KU_ASSERT(dataIdx < localTableDataCollection.size());
        return localTableDataCollection[dataIdx].get();
    }

private:
    common::TableType tableType;
    // For a node table, it should only contain one LocalTableData, while a rel table should contain
    // two, one for each direction.
    std::vector<std::unique_ptr<LocalTableData>> localTableDataCollection;
};

} // namespace storage
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
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        common::ValueVector* leftValueVector, common::ValueVector* rightValueVector,
        common::ValueVector* resultValueVector, uint64_t /*resultPos*/, void* /*dataPtr*/) {
        OP::operation(left, right, result, *leftValueVector, *rightValueVector, *resultValueVector);
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

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeBothFlat(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        auto lPos = left.state->selVector->selectedPositions[0];
        auto rPos = right.state->selVector->selectedPositions[0];
        auto resPos = result.state->selVector->selectedPositions[0];
        result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
        if (!result.isNull(resPos)) {
            executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, lPos, rPos, resPos, dataPtr);
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        auto lPos = left.state->selVector->selectedPositions[0];
        if (left.isNull(lPos)) {
            result.setAllNull();
        } else if (right.hasNoNullsGuarantee()) {
            if (right.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, rPos, rPos, dataPtr);
                }
            }
        } else {
            if (right.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    result.setNull(i, right.isNull(i)); // left is always not null
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    result.setNull(rPos, right.isNull(rPos)); // left is always not null
                    if (!result.isNull(rPos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, rPos, rPos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnFlatFlat(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        auto rPos = right.state->selVector->selectedPositions[0];
        if (right.isNull(rPos)) {
            result.setAllNull();
        } else if (left.hasNoNullsGuarantee()) {
            if (left.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, i, rPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, lPos, rPos, lPos, dataPtr);
                }
            }
        } else {
            if (left.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    result.setNull(i, left.isNull(i)); // right is always not null
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, i, rPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    result.setNull(lPos, left.isNull(lPos)); // right is always not null
                    if (!result.isNull(lPos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, lPos, rPos, lPos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeBothUnFlat(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(left.state == right.state);
        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
            if (result.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, i, i, i, dataPtr);
                }
            } else {
                for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
                    auto pos = result.state->selVector->selectedPositions[i];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        left, right, result, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (result.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
                    result.setNull(i, left.isNull(i) || right.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, i, i, i, dataPtr);
                    }
                }
            } else {
                for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
                    auto pos = result.state->selVector->selectedPositions[i];
                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            left, right, result, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (left.state->isFlat() && right.state->isFlat()) {
            executeBothFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, dataPtr);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            executeFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, dataPtr);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            executeUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, dataPtr);
        } else if (!left.state->isFlat() && !right.state->isFlat()) {
            executeBothUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                left, right, result, dataPtr);
        } else {
            KU_ASSERT(false);
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryFunctionWrapper>(
            left, right, result, nullptr /* dataPtr */);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeString(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryStringFunctionWrapper>(
            left, right, result, nullptr /* dataPtr */);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeListStruct(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryListStructFunctionWrapper>(
            left, right, result, nullptr /* dataPtr */);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeListExtract(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryListExtractFunctionWrapper>(
            left, right, result, nullptr /* dataPtr */);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeComparison(
        common::ValueVector& left, common::ValueVector& right, common::ValueVector& result) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryComparisonFunctionWrapper>(
            left, right, result, nullptr /* dataPtr */);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeUDF(common::ValueVector& left, common::ValueVector& right,
        common::ValueVector& result, void* dataPtr) {
        executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryUDFFunctionWrapper>(
            left, right, result, dataPtr);
    }

    struct BinarySelectWrapper {
        template<typename LEFT_TYPE, typename RIGHT_TYPE, typename OP>
        static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, uint8_t& result,
            common::ValueVector* /*leftValueVector*/, common::ValueVector* /*rightValueVector*/) {
            OP::operation(left, right, result);
        }
    };

    struct BinaryComparisonSelectWrapper {
        template<typename LEFT_TYPE, typename RIGHT_TYPE, typename OP>
        static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, uint8_t& result,
            common::ValueVector* leftValueVector, common::ValueVector* rightValueVector) {
            OP::operation(left, right, result, leftValueVector, rightValueVector);
        }
    };

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static void selectOnValue(common::ValueVector& left, common::ValueVector& right, uint64_t lPos,
        uint64_t rPos, uint64_t resPos, uint64_t& numSelectedValues,
        common::sel_t* selectedPositionsBuffer) {
        uint8_t resultValue = 0;
        SELECT_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos], resultValue,
            &left, &right);
        selectedPositionsBuffer[numSelectedValues] = resPos;
        numSelectedValues += (resultValue == true);
    }

    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static uint64_t selectBothFlat(common::ValueVector& left, common::ValueVector& right) {
        auto lPos = left.state->selVector->selectedPositions[0];
        auto rPos = right.state->selVector->selectedPositions[0];
        uint8_t resultValue = 0;
        if (!left.isNull(lPos) && !right.isNull(rPos)) {
            SELECT_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, FUNC>(
                ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos],
                resultValue, &left, &right);
        }
        return resultValue == true;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC, typename SELECT_WRAPPER>
    static bool selectFlatUnFlat(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        auto lPos = left.state->selVector->selectedPositions[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (left.isNull(lPos)) {
            return numSelectedValues;
        } else if (right.hasNoNullsGuarantee()) {
            if (right.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, lPos, i, i, numSelectedValues, selectedPositionsBuffer);
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, lPos, rPos, rPos, numSelectedValues, selectedPositionsBuffer);
                }
            }
        } else {
            if (right.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    if (!right.isNull(i)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                            left, right, lPos, i, i, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            } else {
                for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
                    auto rPos = right.state->selVector->selectedPositions[i];
                    if (!right.isNull(rPos)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right,
                            lPos, rPos, rPos, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC, typename SELECT_WRAPPER>
    static bool selectUnFlatFlat(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        auto rPos = right.state->selVector->selectedPositions[0];
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (right.isNull(rPos)) {
            return numSelectedValues;
        } else if (left.hasNoNullsGuarantee()) {
            if (left.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, i, rPos, i, numSelectedValues, selectedPositionsBuffer);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, lPos, rPos, lPos, numSelectedValues, selectedPositionsBuffer);
                }
            }
        } else {
            if (left.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    if (!left.isNull(i)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                            left, right, i, rPos, i, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
                    auto lPos = left.state->selVector->selectedPositions[i];
                    if (!left.isNull(lPos)) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(left, right,
                            lPos, rPos, lPos, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    // Right, left, and result vectors share the same selectedPositions.
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC, typename SELECT_WRAPPER>
    static bool selectBothUnFlat(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        uint64_t numSelectedValues = 0;
        auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
            if (left.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < left.state->selVector->selectedSize; i++) {
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, i, i, i, numSelectedValues, selectedPositionsBuffer);
                }
            } else {
                for (auto i = 0u; i < left.state->selVector->selectedSize; i++) {
                    auto pos = left.state->selVector->selectedPositions[i];
                    selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                        left, right, pos, pos, pos, numSelectedValues, selectedPositionsBuffer);
                }
            }
        } else {
            if (left.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < left.state->selVector->selectedSize; i++) {
                    auto isNull = left.isNull(i) || right.isNull(i);
                    if (!isNull) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                            left, right, i, i, i, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            } else {
                for (uint64_t i = 0; i < left.state->selVector->selectedSize; i++) {
                    auto pos = left.state->selVector->selectedPositions[i];
                    auto isNull = left.isNull(pos) || right.isNull(pos);
                    if (!isNull) {
                        selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC, SELECT_WRAPPER>(
                            left, right, pos, pos, pos, numSelectedValues, selectedPositionsBuffer);
                    }
                }
            }
        }
        selVector.selectedSize = numSelectedValues;
        return numSelectedValues > 0;
    }

    // BOOLEAN (AND, OR, XOR)
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static bool select(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinarySelectWrapper>(left, right);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinarySelectWrapper>(
                left, right, selVector);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinarySelectWrapper>(
                left, right, selVector);
        } else {
            return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinarySelectWrapper>(
                left, right, selVector);
        }
    }

    // COMPARISON (GT, GTE, LT, LTE, EQ, NEQ)
    template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
    static bool selectComparison(
        common::ValueVector& left, common::ValueVector& right, common::SelectionVector& selVector) {
        if (left.state->isFlat() && right.state->isFlat()) {
            return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right);
        } else if (left.state->isFlat() && !right.state->isFlat()) {
            return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector);
        } else if (!left.state->isFlat() && right.state->isFlat()) {
            return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector);
        } else {
            return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC, BinaryComparisonSelectWrapper>(
                left, right, selVector);
        }
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace function {

struct ConstFunctionExecutor {

    template<typename RESULT_TYPE, typename OP>
    static void execute(common::ValueVector& result) {
        KU_ASSERT(result.state->isFlat());
        auto resultValues = (RESULT_TYPE*)result.getData();
        auto idx = result.state->selVector->selectedPositions[0];
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
    static void execute(common::ValueVector& result, void* dataPtr) {
        if (result.state->selVector->isUnfiltered()) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                OP::operation(result.getValue<RESULT_TYPE>(i), dataPtr);
            }
        } else {
            for (auto i = 0u; i < result.state->selVector->selectedSize; i++) {
                auto pos = result.state->selVector->selectedPositions[i];
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
    static void executeAllFlat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        auto aPos = a.state->selVector->selectedPositions[0];
        auto bPos = b.state->selVector->selectedPositions[0];
        auto cPos = c.state->selVector->selectedPositions[0];
        auto resPos = result.state->selVector->selectedPositions[0];
        result.setNull(resPos, a.isNull(aPos) || b.isNull(bPos) || c.isNull(cPos));
        if (!result.isNull(resPos)) {
            executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, aPos, bPos, cPos, resPos, dataPtr);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatFlatUnflat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        auto aPos = a.state->selVector->selectedPositions[0];
        auto bPos = b.state->selVector->selectedPositions[0];
        if (a.isNull(aPos) || b.isNull(bPos)) {
            result.setAllNull();
        } else if (c.hasNoNullsGuarantee()) {
            if (c.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    auto pos = c.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (c.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    result.setNull(i, c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    auto pos = c.state->selVector->selectedPositions[i];
                    result.setNull(pos, c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatUnflat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(b.state == c.state);
        auto aPos = a.state->selVector->selectedPositions[0];
        if (a.isNull(aPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, i, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    result.setNull(i, b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, i, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatFlat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        auto aPos = a.state->selVector->selectedPositions[0];
        auto cPos = c.state->selVector->selectedPositions[0];
        if (a.isNull(aPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee()) {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    result.setNull(i, b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllUnFlat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(a.state == b.state && b.state == c.state);
        if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, i, i, i, dataPtr);
                }
            } else {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    result.setNull(i, a.isNull(i) || b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, i, i, i, dataPtr);
                    }
                }
            } else {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatFlat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        auto bPos = b.state->selVector->selectedPositions[0];
        auto cPos = c.state->selVector->selectedPositions[0];
        if (b.isNull(bPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, bPos, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, bPos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, bPos, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, bPos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatUnflat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(a.state == c.state);
        auto bPos = b.state->selVector->selectedPositions[0];
        if (b.isNull(bPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatUnFlatFlat(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(a.state == b.state);
        auto cPos = c.state->selVector->selectedPositions[0];
        if (c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i) || b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeAllFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeFlatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeFlatUnflatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeFlatUnflatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeAllUnFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeUnflatUnFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeUnflatFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeUnflatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, dataPtr);
        } else {
            KU_ASSERT(false);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& a, common::ValueVector& b, common::ValueVector& c,
        common::ValueVector& result) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, TernaryFunctionWrapper>(
            a, b, c, result, nullptr /* dataPtr */);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeString(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, TernaryStringFunctionWrapper>(
            a, b, c, result, nullptr /* dataPtr */);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeListStruct(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, TernaryListFunctionWrapper>(
            a, b, c, result, nullptr /* dataPtr */);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeUDF(common::ValueVector& a, common::ValueVector& b, common::ValueVector& c,
        common::ValueVector& result, void* dataPtr) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, TernaryUDFFunctionWrapper>(
            a, b, c, result, dataPtr);
    }
};

} // namespace function
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

struct UnaryCastStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto resultVector_ = (common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_->getValue<RESULT_TYPE>(resultPos), resultVector_, inputPos,
            &reinterpret_cast<CastFunctionBindData*>(dataPtr)->csvConfig.option);
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

struct UnaryRdfVariantCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::template operation<OPERAND_TYPE, RESULT_TYPE>(
            inputVector_.getValue<OPERAND_TYPE>(inputPos), inputVector_, inputPos,
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_, resultPos);
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

struct CastFixedListToListFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
        auto numValuesPerList = common::FixedListType::getNumValuesInList(&operand.dataType);

        for (auto i = 0u; i < numOfEntries; i++) {
            if (!operand.isNull(i)) {
                for (auto j = 0u; j < numValuesPerList; j++) {
                    OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                        (void*)(&operand), i * numValuesPerList + j, (void*)(&result),
                        i * numValuesPerList + j, nullptr);
                }
            }
        }
    }
};

struct CastChildFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        auto numOfEntries = reinterpret_cast<CastFunctionBindData*>(dataPtr)->numOfEntries;
        for (auto i = 0u; i < numOfEntries; i++) {
            result.setNull(i, operand.isNull(i));
            if (!result.isNull(i)) {
                OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                    (void*)(&operand), i, (void*)(&result), i, dataPtr);
            }
        }
    }
};

struct UnaryFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& inputVector, uint64_t inputPos,
        common::ValueVector& resultVector, uint64_t resultPos, void* dataPtr) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
            (void*)&inputVector, inputPos, (void*)&resultVector, resultPos, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (operand.state->isFlat()) {
            auto inputPos = operand.state->selVector->selectedPositions[0];
            auto resultPos = result.state->selVector->selectedPositions[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(resultPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                    operand, inputPos, result, resultPos, dataPtr);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, i, result, i, dataPtr);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, pos, result, pos, dataPtr);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, i, result, i, dataPtr);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, pos, result, pos, dataPtr);
                        }
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            operand, result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeUDF(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryUDFFunctionWrapper>(
            operand, result, dataPtr);
    }
};

} // namespace function
} // namespace kuzu

#include <unordered_map>


namespace kuzu {
namespace storage {

class Column;
class MemoryManager;

// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    explicit LocalStorage(storage::MemoryManager* mm);

    // This function will create the local table data if not exists.
    LocalTableData* getOrCreateLocalTableData(common::table_id_t tableID,
        const std::vector<std::unique_ptr<Column>>& columns,
        common::TableType tableType = common::TableType::NODE,
        common::ColumnDataFormat dataFormat = common::ColumnDataFormat::REGULAR,
        common::vector_idx_t dataIdx = 0);
    LocalTable* getLocalTable(common::table_id_t tableID);
    // This function will return nullptr if the local table does not exist.
    LocalTableData* getLocalTableData(common::table_id_t tableID, common::vector_idx_t dataIdx = 0);
    std::unordered_set<common::table_id_t> getTableIDsWithUpdates();

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<LocalTable>> tables;
    storage::MemoryManager* mm;
};

} // namespace storage
} // namespace kuzu


namespace kuzu {
namespace function {

struct ScalarFunction;

using scalar_compile_func =
    std::function<void(FunctionBindData*, const std::vector<std::shared_ptr<common::ValueVector>>&,
        std::shared_ptr<common::ValueVector>&)>;
using scalar_exec_func = std::function<void(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::ValueVector&, void*)>;
using scalar_select_func = std::function<bool(
    const std::vector<std::shared_ptr<common::ValueVector>>&, common::SelectionVector&)>;
using function_set = std::vector<std::unique_ptr<Function>>;

struct ScalarFunction final : public BaseScalarFunction {

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), nullptr, nullptr, nullptr, isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, nullptr, isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_bind_func bindFunc, bool isVarLength = false)
        : ScalarFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(execFunc), std::move(selectFunc), nullptr, std::move(bindFunc),
              isVarLength} {}

    ScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, scalar_exec_func execFunc,
        scalar_select_func selectFunc, scalar_compile_func compileFunc, scalar_bind_func bindFunc,
        bool isVarLength = false)
        : BaseScalarFunction{FunctionType::SCALAR, std::move(name), std::move(parameterTypeIDs),
              returnTypeID, std::move(bindFunc)},
          execFunc{std::move(execFunc)}, selectFunc(std::move(selectFunc)),
          compileFunc{std::move(compileFunc)}, isVarLength{isVarLength} {}

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::execute<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeString<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeString<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
    static bool BinarySelectFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::SelectionVector& selVector) {
        KU_ASSERT(params.size() == 2);
        return BinaryFunctionExecutor::select<LEFT_TYPE, RIGHT_TYPE, FUNC>(
            *params[0], *params[1], selVector);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            *params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryStringFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryCastStringFunctionWrapper>(*params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastFunctionWrapper>(
            *params[0], result, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC,
        typename EXECUTOR = UnaryFunctionExecutor>
    static void UnaryRdfVariantCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        EXECUTOR::template executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryRdfVariantCastFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecNestedTypeFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 1);
        UnaryFunctionExecutor::executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC,
            UnaryNestedTypeFunctionWrapper>(*params[0], result, nullptr /* dataPtr */);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void ConstExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.empty());
        (void)params;
        ConstFunctionExecutor::execute<RESULT_TYPE, FUNC>(result);
    }

    template<typename RESULT_TYPE, typename FUNC>
    static void PoniterExecFunction(const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(params.empty());
        (void)params;
        PointerFunctionExecutor::execute<RESULT_TYPE, FUNC>(result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 3);
        TernaryFunctionExecutor::executeListStruct<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryExecListStructFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr) {
        KU_ASSERT(params.size() == 2);
        BinaryFunctionExecutor::executeListStruct<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<ScalarFunction>(*this);
    }

    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    scalar_compile_func compileFunc;
    // Currently we only one variable-length function which is list creation. The expectation is
    // that all parameters must have the same type as parameterTypes[0].
    bool isVarLength;
};

} // namespace function
} // namespace kuzu

#include <memory>


namespace kuzu {
namespace storage {
class LocalStorage;
class MemoryManager;
} // namespace storage
namespace transaction {
class TransactionManager;

enum class TransactionType : uint8_t { READ_ONLY, WRITE };
constexpr uint64_t INVALID_TRANSACTION_ID = UINT64_MAX;

class Transaction {
    friend class TransactionManager;

public:
    Transaction(TransactionType transactionType, uint64_t transactionID, storage::MemoryManager* mm)
        : type{transactionType}, ID{transactionID} {
        localStorage = std::make_unique<storage::LocalStorage>(mm);
    }

    constexpr explicit Transaction(TransactionType transactionType) noexcept
        : type{transactionType}, ID{INVALID_TRANSACTION_ID} {}

public:
    inline TransactionType getType() const { return type; }
    inline bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    inline bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    inline uint64_t getID() const { return ID; }
    inline storage::LocalStorage* getLocalStorage() { return localStorage.get(); }

    static inline std::unique_ptr<Transaction> getDummyWriteTrx() {
        return std::make_unique<Transaction>(TransactionType::WRITE);
    }
    static inline std::unique_ptr<Transaction> getDummyReadOnlyTrx() {
        return std::make_unique<Transaction>(TransactionType::READ_ONLY);
    }

private:
    TransactionType type;
    // TODO(Guodong): add type transaction_id_t.
    uint64_t ID;
    std::unique_ptr<storage::LocalStorage> localStorage;
};

static Transaction DUMMY_READ_TRANSACTION = Transaction(TransactionType::READ_ONLY);
static Transaction DUMMY_WRITE_TRANSACTION = Transaction(TransactionType::WRITE);

} // namespace transaction
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
    static inline void operation(
        LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*binary_udf_func)(LEFT_TYPE, RIGHT_TYPE);
        auto binaryUDFFunc = (binary_udf_func)udfFunc;
        result = binaryUDFFunc(left, right);
    }
};

struct TernaryUDFExecutor {
    template<class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE>
    static inline void operation(
        A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*ternary_udf_func)(A_TYPE, B_TYPE, C_TYPE);
        auto ternaryUDFFunc = (ternary_udf_func)udfFunc;
        result = ternaryUDFFunc(a, b, c);
    }
};

struct UDF {
    template<typename T>
    static bool templateValidateType(const common::LogicalTypeID& type) {
        switch (type) {
        case common::LogicalTypeID::BOOL:
            return std::is_same<T, bool>();
        case common::LogicalTypeID::INT16:
            return std::is_same<T, int16_t>();
        case common::LogicalTypeID::INT32:
            return std::is_same<T, int32_t>();
        case common::LogicalTypeID::INT64:
            return std::is_same<T, int64_t>();
        case common::LogicalTypeID::FLOAT:
            return std::is_same<T, float>();
        case common::LogicalTypeID::DOUBLE:
            return std::is_same<T, double>();
        case common::LogicalTypeID::DATE:
            return std::is_same<T, int32_t>();
        case common::LogicalTypeID::TIMESTAMP_NS:
        case common::LogicalTypeID::TIMESTAMP_MS:
        case common::LogicalTypeID::TIMESTAMP_SEC:
        case common::LogicalTypeID::TIMESTAMP_TZ:
        case common::LogicalTypeID::TIMESTAMP:
            return std::is_same<T, int64_t>();
        case common::LogicalTypeID::STRING:
            return std::is_same<T, common::ku_string_t>();
        case common::LogicalTypeID::BLOB:
            return std::is_same<T, common::blob_t>();
        default:
            KU_UNREACHABLE;
        }
    }

    template<typename T>
    static void validateType(const common::LogicalTypeID& type) {
        if (!templateValidateType<T>(type)) {
            throw common::CatalogException{
                "Incompatible udf parameter/return type and templated type."};
        }
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_exec_func createUnaryExecFunc(RESULT_TYPE (*/*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename OPERAND_TYPE>
    static function::scalar_exec_func createUnaryExecFunc(RESULT_TYPE (*udfFunc)(OPERAND_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 1) {
            throw common::CatalogException{
                "Expected exactly one parameter type for unary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<OPERAND_TYPE>(parameterTypes[0]);
        function::scalar_exec_func execFunc =
            [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 1);
            UnaryFunctionExecutor::executeUDF<OPERAND_TYPE, RESULT_TYPE, UnaryUDFExecutor>(
                *params[0], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_exec_func createBinaryExecFunc(RESULT_TYPE (*/*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename LEFT_TYPE, typename RIGHT_TYPE>
    static function::scalar_exec_func createBinaryExecFunc(
        RESULT_TYPE (*udfFunc)(LEFT_TYPE, RIGHT_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 2) {
            throw common::CatalogException{
                "Expected exactly two parameter types for binary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<LEFT_TYPE>(parameterTypes[0]);
        validateType<RIGHT_TYPE>(parameterTypes[1]);
        function::scalar_exec_func execFunc =
            [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 2);
            BinaryFunctionExecutor::executeUDF<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE,
                BinaryUDFExecutor>(*params[0], *params[1], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_exec_func createTernaryExecFunc(RESULT_TYPE (*/*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename A_TYPE, typename B_TYPE, typename C_TYPE>
    static function::scalar_exec_func createTernaryExecFunc(
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
        function::scalar_exec_func execFunc =
            [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 3);
            TernaryFunctionExecutor::executeUDF<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE,
                TernaryUDFExecutor>(*params[0], *params[1], *params[2], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename TR, typename... Args>
    static scalar_exec_func getScalarExecFunc(
        TR (*udfFunc)(Args...), std::vector<common::LogicalTypeID> parameterTypes) {
        constexpr auto numArgs = sizeof...(Args);
        switch (numArgs) {
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
        } else if (std::is_same<T, int16_t>()) {
            return common::LogicalTypeID::INT16;
        } else if (std::is_same<T, int32_t>()) {
            return common::LogicalTypeID::INT32;
        } else if (std::is_same<T, int64_t>()) {
            return common::LogicalTypeID::INT64;
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
        getParameterTypesRecursive<Args...>(parameterTypes);
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
        scalar_exec_func scalarExecFunc = getScalarExecFunc<TR, Args...>(udfFunc, parameterTypes);
        definitions.push_back(std::make_unique<function::ScalarFunction>(
            std::move(name), std::move(parameterTypes), returnType, std::move(scalarExecFunc)));
        return definitions;
    }

    template<typename TR, typename... Args>
    static function_set getFunction(std::string name, TR (*udfFunc)(Args...)) {
        return getFunction<TR, Args...>(
            std::move(name), udfFunc, getParameterTypes<Args...>(), getParameterType<TR>());
    }

    template<typename TR, typename... Args>
    static function_set getVectorizedFunction(std::string name, scalar_exec_func execFunc) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            getParameterTypes<Args...>(), getParameterType<TR>(), std::move(execFunc)));
        return definitions;
    }

    static function_set getVectorizedFunction(std::string name, scalar_exec_func execFunc,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(
            std::move(name), std::move(parameterTypes), returnType, std::move(execFunc)));
        return definitions;
    }
};

} // namespace function
} // namespace kuzu


namespace kuzu {
namespace main {
class Database;
}

namespace transaction {

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
enum class TransactionMode : uint8_t { AUTO = 0, MANUAL = 1 };

class TransactionContext {
public:
    explicit TransactionContext(main::Database* database);
    ~TransactionContext();

    inline bool isAutoTransaction() const { return mode == TransactionMode::AUTO; }

    void beginReadTransaction();
    void beginWriteTransaction();
    void beginAutoTransaction(bool readOnlyStatement);
    void validateManualTransaction(bool allowActiveTransaction, bool readOnlyStatement);

    void commit();
    void rollback();
    void commitSkipCheckPointing();
    void rollbackSkipCheckPointing();

    inline TransactionMode getTransactionMode() const { return mode; }
    inline bool hasActiveTransaction() const { return activeTransaction != nullptr; }
    inline Transaction* getActiveTransaction() const { return activeTransaction.get(); }

private:
    void commitInternal(bool skipCheckPointing);
    void rollbackInternal(bool skipCheckPointing);

private:
    void beginTransactionInternal(TransactionType transactionType);

private:
    std::mutex mtx;
    main::Database* database;
    TransactionMode mode;
    std::unique_ptr<Transaction> activeTransaction;
};

} // namespace transaction
} // namespace kuzu

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>


namespace kuzu {

namespace binder {
class Binder;
class ExpressionBinder;
} // namespace binder

namespace common {
class RandomEngine;
}

namespace main {
class Database;

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

using replace_func_t = std::function<std::unique_ptr<common::Value>(common::Value*)>;

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class binder::ExpressionBinder;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;
    friend struct ThreadsSetting;
    friend struct TimeoutSetting;
    friend struct VarLengthExtendMaxDepthSetting;
    friend struct EnableSemiMaskSetting;
    friend struct HomeDirectorySetting;
    friend struct FileSearchPathSetting;

public:
    explicit ClientContext(Database* database);

    inline void interrupt() { activeQuery.interrupted = true; }

    bool isInterrupted() const { return activeQuery.interrupted; }

    inline bool isTimeOutEnabled() const { return timeoutInMS != 0; }

    inline uint64_t getTimeoutRemainingInMS() {
        KU_ASSERT(isTimeOutEnabled());
        auto elapsed = activeQuery.timer.getElapsedTimeInMS();
        return elapsed >= timeoutInMS ? 0 : timeoutInMS - elapsed;
    }

    inline bool isEnableSemiMask() const { return enableSemiMask; }

    void startTimingIfEnabled();

    KUZU_API common::Value getCurrentSetting(const std::string& optionName);

    transaction::Transaction* getTx() const;
    KUZU_API transaction::TransactionContext* getTransactionContext() const;

    inline bool hasReplaceFunc() { return replaceFunc != nullptr; }
    inline void setReplaceFunc(replace_func_t func) { replaceFunc = func; }

    KUZU_API void setExtensionOption(std::string name, common::Value value);

    common::RandomEngine* getRandomEngine() { return randomEngine.get(); }

    common::VirtualFileSystem* getVFSUnsafe() const;

    std::string getExtensionDir() const;

    KUZU_API Database* getDatabase() const { return database; }
    storage::StorageManager* getStorageManager();
    storage::MemoryManager* getMemoryManager();
    catalog::Catalog* getCatalog();

    KUZU_API std::string getEnvVariable(const std::string& name);

private:
    inline void resetActiveQuery() { activeQuery.reset(); }

    uint64_t numThreadsForExecution;
    ActiveQuery activeQuery;
    uint64_t timeoutInMS;
    uint32_t varLengthExtendMaxDepth;
    std::unique_ptr<transaction::TransactionContext> transactionContext;
    bool enableSemiMask;
    replace_func_t replaceFunc;
    std::unordered_map<std::string, common::Value> extensionOptionValues;
    std::unique_ptr<common::RandomEngine> randomEngine;
    std::string homeDirectory;
    std::string fileSearchPath;
    Database* database;
};

} // namespace main
} // namespace kuzu

#include <mutex>


namespace kuzu {
namespace main {

/**
 * @brief Connection is used to interact with a Database instance. Each Connection is thread-safe.
 * Multiple connections can connect to the same Database instance in a multi-threaded environment.
 */
class Connection {
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::PrivateGraphTest;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::testing::TestRunner;
    friend class kuzu::benchmark::Benchmark;
    friend class kuzu::testing::TinySnbDDLTest;

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
     * @brief Executes the given prepared statement with args and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param args The parameter pack where each arg is a std::pair with the first element being
     * parameter name and second element being parameter value.
     * @return the result of the query.
     */
    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, std::pair<std::string, Args>... args) {
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

    /**
     * @brief gets the query timeout value of the current connection. A value of zero (the default)
     * disables the timeout.
     */
    KUZU_API uint64_t getQueryTimeOut();

    template<typename TR, typename... Args>
    void createScalarFunction(std::string name, TR (*udfFunc)(Args...)) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(
            std::move(nameCopy), function::UDF::getFunction<TR, Args...>(std::move(name), udfFunc));
        commitUDFTrx(autoTrx);
    }

    template<typename TR, typename... Args>
    void createScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypes,
        common::LogicalTypeID returnType, TR (*udfFunc)(Args...)) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(
            std::move(nameCopy), function::UDF::getFunction<TR, Args...>(std::move(name), udfFunc,
                                     std::move(parameterTypes), returnType));
        commitUDFTrx(autoTrx);
    }

    template<typename TR, typename... Args>
    void createVectorizedFunction(std::string name, function::scalar_exec_func scalarFunc) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(std::move(nameCopy), function::UDF::getVectorizedFunction<TR, Args...>(
                                                   std::move(name), std::move(scalarFunc)));
        commitUDFTrx(autoTrx);
    }

    void createVectorizedFunction(std::string name,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType,
        function::scalar_exec_func scalarFunc) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(
            std::move(nameCopy), function::UDF::getVectorizedFunction(std::move(name),
                                     std::move(scalarFunc), std::move(parameterTypes), returnType));
        commitUDFTrx(autoTrx);
    }

    inline void setReplaceFunc(replace_func_t replaceFunc) {
        clientContext->setReplaceFunc(std::move(replaceFunc));
    }

    inline ClientContext* getClientContext() { return clientContext.get(); };

private:
    std::unique_ptr<QueryResult> query(
        std::string_view query, std::string_view encodedJoin, bool enumerateAllPlans = true);

    std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);

    std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);

    std::vector<std::unique_ptr<parser::Statement>> parseQuery(std::string_view query);

    std::unique_ptr<PreparedStatement> prepareNoLock(parser::Statement* parsedStatement,
        bool enumerateAllPlans = false, std::string_view joinOrder = std::string_view());

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_unique<common::Value>((T)arg.second);
        params.insert({name, std::move(val)});
        return executeWithParams(preparedStatement, std::move(params), args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    KUZU_API void addScalarFunction(std::string name, function::function_set definitions);

    KUZU_API bool startUDFAutoTrx(transaction::TransactionContext* trx);
    KUZU_API void commitUDFTrx(bool isAutoCommitTrx);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu

