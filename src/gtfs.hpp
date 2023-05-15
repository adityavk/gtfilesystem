#ifndef GTFS
#define GTFS

#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <unordered_map>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>
#include <memory>

/*********** Cross-compiler <filesystem> include taken from https://stackoverflow.com/a/53365539 *********/ 

// We haven't checked which filesystem to include yet
#ifndef INCLUDE_STD_FILESYSTEM_EXPERIMENTAL

// Check for feature test macro for <filesystem>
#   if defined(__cpp_lib_filesystem)
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 0

// Check for feature test macro for <experimental/filesystem>
#   elif defined(__cpp_lib_experimental_filesystem)
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1

// We can't check if headers exist...
// Let's assume experimental to be safe
#   elif !defined(__has_include)
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1

// Check if the header "<filesystem>" exists
#   elif __has_include(<filesystem>)
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 0

// Check if the header "<filesystem>" exists
#   elif __has_include(<experimental/filesystem>)
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1

// Fail if neither header is available with a nice error message
#   else
#       error Could not find system header "<filesystem>" or "<experimental/filesystem>"
#   endif

// We determined that we need the exprimental version
#   if INCLUDE_STD_FILESYSTEM_EXPERIMENTAL
// Include it
#       include <experimental/filesystem>

// We need the alias from std::experimental::filesystem to std::filesystem
namespace std {
    namespace filesystem = experimental::filesystem;
}

// We have a decent compiler and can use the normal version
#   else
// Include it
#       include <filesystem>
#   endif

#endif // #ifndef INCLUDE_STD_FILESYSTEM_EXPERIMENTAL

/***************** End of cross-compiler <filesystem> include ***************/ 


using namespace std;
namespace fs = std::filesystem;

#define PASS "\033[32;1m PASS \033[0m\n"
#define FAIL "\033[31;1m FAIL \033[0m\n"

// GTFileSystem basic data structures 

#define MAX_FILENAME_LEN 255
#define MAX_NUM_FILES_PER_DIR 1024

extern int do_verbose;

class TransactionManager;
using TransactionID = uint32_t;
using VMSizeT = size_t;

typedef struct gtfs {
    string dirname;
} gtfs_t;

typedef struct file {
    string filename;
    int fileLength;
    int fileDescriptor = -1;
    unique_ptr<TransactionManager> transactionManager;
} file_t;

typedef struct write {
    string filename;
    int offset;
    int length;
    file_t* file;
    TransactionID transactionId;
} write_t;

// GTFileSystem basic API calls

gtfs_t* gtfs_init(string directory, int verbose_flag);
int gtfs_clean(gtfs_t *gtfs);

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length);
int gtfs_close_file(gtfs_t* gtfs, file_t* fl);
int gtfs_remove_file(gtfs_t* gtfs, file_t* fl);

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length);
write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data);
int gtfs_sync_write_file(write_t* write_id);
int gtfs_abort_write_file(write_t* write_id);

// BONUS: Implement below API calls to get bonus credits

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes);
int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes);


struct Transaction {
    TransactionID transactionId;
    VMSizeT offset;
    vector<char> oldData;
    vector<char> newData;
};
ostream& operator<<(ostream& os, const Transaction& transaction);
istream& operator>>(istream& is, Transaction& transaction);

class LogManager;
using VMSegment = vector<char>;

/** Transaction manager that manages a virtual memory segment and provides the basic functionality to create, abort and replay transactions */
class BaseTransactionManager {
protected:
    int totalTransactionCount = 0;
    VMSegment vmSegment;
    vector<Transaction> uncommittedTransactions;
public:
    BaseTransactionManager(VMSegment&& vmSegment);
    TransactionID createTransaction(VMSizeT offset, VMSizeT length, const char* newData);
    int abortTransaction(TransactionID transactionId);
    int replayTransactions(const vector<Transaction>& transactions);
    vector<char>& getVMBase();
};

/** Specialization of BaseTransactionManager that manages a disk file and provides additional functionality to commit transactions to a log file */
class TransactionManager: public BaseTransactionManager {
    fs::path logFilePath;
public:
    TransactionManager(const fs::path& originalFilePath, VMSegment&& vmSegment);
    int commitTransaction(TransactionID transactionId, int bytes = -1);
    fs::path getLogFilePath() const;
};

/** Utility class to read and write transactions to/from a given log file */
class LogManager {
public:
    static vector<Transaction> getTransactionsInLog(const fs::path& logFilePath);
    static int writeTransaction(const fs::path& logFilePath, const Transaction& transaction);
};

#endif
