#include "gtfs.hpp"
#include <cstring>
#include <fstream>
#include <algorithm>
#include <sys/file.h>

#define VERBOSE_PRINT(verbose, str...) do { \
    if (verbose) cout << "VERBOSE: "<< __FILE__ << ":" << __LINE__ << " " << __func__ << "(): " << str; \
} while(0)

int do_verbose;

unordered_map<string, gtfs_t*> gtfs_map;

gtfs_t* gtfs_init(string directory, int verbose_flag) {
    do_verbose = verbose_flag;
    gtfs_t *gtfs = nullptr;
    VERBOSE_PRINT(do_verbose, "Initializing GTFileSystem inside directory " << directory << "\n");
    
    if (directory.length() == 0) {
        VERBOSE_PRINT(do_verbose, "Directory name is empty, returning nullptr\n");
        return gtfs;
    }
    auto gtfs_dir = fs::path(directory);
    auto existingIt = gtfs_map.find(gtfs_dir.string());
    if (existingIt != gtfs_map.end()) {
        return existingIt->second;
    }
    if (!fs::exists(gtfs_dir)) {
        VERBOSE_PRINT(do_verbose, "Directory does not exist, creating it\n");
        fs::create_directory(gtfs_dir);
    } else if (!fs::is_directory(gtfs_dir)) {
        VERBOSE_PRINT(do_verbose, "Directory name exists but is not a directory, returning nullptr\n");
        return gtfs;
    }

    gtfs = new gtfs_t;
    gtfs->dirname = gtfs_dir.string();
    gtfs_map[gtfs_dir.string()] = gtfs;

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return gtfs;
}

/**
 * Processes the transactions in given log file, optionally truncating the processing to n bytes.
 * Applies the transactions to the original file and deletes the log file.
 * Called from gtfs_clean() and gtfs_clean_n_bytes().
 */ 
int clean_n_bytes(const fs::path& logFilePath, int bytes = -1) {
    auto transactions = LogManager::getTransactionsInLog(logFilePath);
    if (bytes >= 0) {
        auto it = transactions.begin();
        for (; it != transactions.end(); ++it) {
            if (it->newData.size() > bytes) {
                break;
            }
            bytes -= it->newData.size();
            if (bytes == 0) {
                ++it;
                break;
            }
        }
        // Remove rest of the transactions
        transactions.erase(it, transactions.end());
        if (bytes > 0) {
            VERBOSE_PRINT(do_verbose, "Not enough transactions to clean " << bytes << " bytes in log file " << logFilePath << "\n");
        }
        VERBOSE_PRINT(do_verbose, "Cleaning " << transactions.size() << " transactions in log file " << logFilePath << "\n");
    }
    fs::path originalFilePath = logFilePath.string().substr(0, logFilePath.string().length() - 4);
    
    // Read the current file contents of the actual file on disk
    ifstream originalFile(originalFilePath);
    vector<char> originalFileBuffer((istreambuf_iterator<char>(originalFile)), istreambuf_iterator<char>());
    originalFile.close();

    // Replay all log transactions on the buffer
    BaseTransactionManager transactionManager(move(originalFileBuffer));
    transactionManager.replayTransactions(transactions);

    // Overwrite the updated buffer (transactionManager.getVMBase()) into the original file
    ofstream originalFileWrite(originalFilePath);
    auto updatedBuffer = transactionManager.getVMBase();
    originalFileWrite.write(updatedBuffer.data(), updatedBuffer.size());
    originalFileWrite.close();

    // Delete the log file
    if (!fs::remove(logFilePath)) {
        VERBOSE_PRINT(do_verbose, "Failed to delete log file " << logFilePath << "\n");
        return -1;
    }
    return 0;
}

int gtfs_clean(gtfs_t *gtfs) {
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up GTFileSystem inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return ret;
    }

    // Iterate through each log file in the directory and apply the transactions to the corresponding actual file
    for (auto& p: fs::directory_iterator(gtfs->dirname)) {
        if (fs::is_regular_file(p) && p.path().extension() == ".log") {
            if (clean_n_bytes(p.path(), -1) != 0) {
                ret = -2;
            }
        }
    }

    if (ret != -2) {
        ret = 0;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int fileLength) {
    file_t *fl = NULL;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Opening file " << filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return NULL;
    }
    
    if (filename.length() == 0) {
        VERBOSE_PRINT(do_verbose, "Filename is empty, returning nullptr\n");
        return fl;
    }
    auto file_path = fs::path(gtfs->dirname) / filename;
    if (!fs::exists(file_path)) {
        VERBOSE_PRINT(do_verbose, "File does not exist, creating it\n");
        ofstream file(file_path);
        file.close();
    } else if (!fs::is_regular_file(file_path)) {
        VERBOSE_PRINT(do_verbose, "File name exists but is not a regular file, returning nullptr\n");
        return fl;
    }

    // Get size of file at file_path
    auto file_size = fs::file_size(file_path);
    if (fileLength < file_size) {
        VERBOSE_PRINT(do_verbose, "File length is less than the size of the file, not allowed!\n");
        return fl;
    } else if (fileLength > file_size) {
        VERBOSE_PRINT(do_verbose, "File length is greater than the size of the file, extending file\n");
        // Extend file to fileLength
        fs::resize_file(file_path, fileLength);
    }

    // Lock file at path file_path using flock
    int fileDescriptor = open(file_path.c_str(), O_RDWR);
    if (fileDescriptor == -1) {
        VERBOSE_PRINT(do_verbose, "Failed to open file\n");
        return fl;
    }
    if (flock(fileDescriptor, LOCK_EX | LOCK_NB) == -1) {
        VERBOSE_PRINT(do_verbose, "Failed to lock file\n");
        close(fileDescriptor);
        return fl;
    }

    // Read fd into buffer
    vector<char> buffer(fileLength);
    if (read(fileDescriptor, buffer.data(), fileLength) == -1) {
        VERBOSE_PRINT(do_verbose, "Failed to read file\n");
        close(fileDescriptor);
        return fl;
    }
    
    fl = new file_t;
    fl->filename = filename;
    fl->fileLength = fileLength;
    fl->fileDescriptor = fileDescriptor;
    fl->transactionManager = make_unique<TransactionManager>(file_path, move(buffer));
    fl->transactionManager->replayTransactions(LogManager::getTransactionsInLog(fl->transactionManager->getLogFilePath()));

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return fl;
}

int gtfs_close_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Closing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }

    if (fl->fileDescriptor == -1) {
        VERBOSE_PRINT(do_verbose, "File is not open\n");
        return ret;
    }
    
    // Close the locked file, flock's lock gets dropped automatically on close
    ret = close(fl->fileDescriptor);
    // Also destruct remaining properties to avoid mem leaks and check whether file is open in other ops
    fl->fileDescriptor = -1;
    fl->fileLength = 0;
    fl->transactionManager = nullptr;

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

int gtfs_remove_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Removing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return ret;
    }

    if (fl->fileDescriptor != -1) {
        VERBOSE_PRINT(do_verbose, "File is still open, close it before removing!\n");
        return ret;
    }
    // Delete both the actual file and the log file
    auto file_path = fs::path(gtfs->dirname) / fl->filename;
    auto log_path = fs::path(gtfs->dirname) / (fl->filename + ".log");
    ret = fs::remove(file_path);
    // Log file may not have been created if no writes were synced, so ignore error
    fs::remove(log_path);

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length) {
    char* ret_data = NULL;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Reading " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }

    if (fl->fileDescriptor == -1) {
        VERBOSE_PRINT(do_verbose, "File is not open\n");
        return ret_data;
    }
    
    // Copy data from transaction manager's managed virtual memory segment into a string
    // TransactionManager contains the most up-to-date data: synced writes before file open, and all synced and unsynced writes after file open
    string data;
    const auto vmBaseIt = fl->transactionManager->getVMBase().begin();
    if (offset < fl->transactionManager->getVMBase().size()) {
        auto vmIt = vmBaseIt + offset;
        while (length-- && vmIt != fl->transactionManager->getVMBase().end()) {
            data.push_back(*vmIt++);
        }
    }
    // Duplicate the string into a char* to return, string would be destructed on return. Caller is responsible for freeing the returned char*
    ret_data = strdup(data.c_str());

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns pointer to data read.
    return ret_data;
}

write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data) {
    write_t *write_id = NULL;
    if (gtfs and fl) {
        VERBOSE_PRINT(do_verbose, "Writting " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem or file does not exist\n");
        return NULL;
    }

    if (fl->fileDescriptor == -1) {
        VERBOSE_PRINT(do_verbose, "File is not open\n");
        return write_id;
    }

    // Create a transaction in the transaction manager and attach the transactionId to the returned write_t
    auto transactionId = fl->transactionManager->createTransaction(offset, length, data);
    write_id = new write_t;
    write_id->filename = fl->filename;
    write_id->offset = offset;
    write_id->length = length;
    write_id->file = fl;
    write_id->transactionId = transactionId;
    
    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return write_id;
}

int gtfs_sync_write_file(write_t* write_id) {
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Persisting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    // Use the transactionId and commit the transaction to log file via transaction manager
    if (write_id-> file == nullptr || write_id->file->transactionManager == nullptr) {
        VERBOSE_PRINT(do_verbose, "File is not open\n");
        return ret;
    }
    ret = write_id->file->transactionManager->commitTransaction(write_id->transactionId);

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns number of bytes written.
    return ret;
}

int gtfs_abort_write_file(write_t* write_id) {
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Aborting write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    // Abort the transaction via transaction manager (works if transaction is not committed yet)
    if (write_id-> file == nullptr || write_id->file->transactionManager == nullptr) {
        VERBOSE_PRINT(do_verbose, "File is not open\n");
        return ret;
    }
    ret = write_id->file->transactionManager->abortTransaction(write_id->transactionId);

    VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
    return ret;
}

// BONUS: Implement below API calls to get bonus credits

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes){
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up [ " << bytes << " bytes ] GTFileSystem inside directory " << gtfs->dirname << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "GTFileSystem does not exist\n");
        return ret;
    }

    // Iterate through each log file in the directory and apply the transactions to the corresponding actual file
    // Pass the number of bytes to clean: will clean `bytes` bytes from each log file, not just the first one
    for (auto& p: fs::directory_iterator(gtfs->dirname)) {
        if (fs::is_regular_file(p) && p.path().extension() == ".log") {
            if (clean_n_bytes(p.path(), bytes) != 0) {
                ret = -2;
            }
        }
    }

    if (ret != -2) {
        ret = 0;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

int gtfs_sync_write_file_n_bytes(write_t* write_id, int bytes){
    int ret = -1;
    if (write_id) {
        VERBOSE_PRINT(do_verbose, "Persisting [ " << bytes << " bytes ] write of " << write_id->length << " bytes starting from offset " << write_id->offset << " inside file " << write_id->filename << "\n");
    } else {
        VERBOSE_PRINT(do_verbose, "Write operation does not exist\n");
        return ret;
    }

    // Commit only the first `bytes` bytes of the transaction to the log file
    ret = write_id->file->transactionManager->commitTransaction(write_id->transactionId, bytes);
    if (ret == -1) {
        VERBOSE_PRINT(do_verbose, "Number of bytes to sync was more than the bytes written in write_id\n");
        return ret;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

BaseTransactionManager::BaseTransactionManager(VMSegment&& vmSegment): vmSegment(vmSegment) {}

TransactionID BaseTransactionManager::createTransaction(VMSizeT offset, VMSizeT length, const char* newData) {
    Transaction transaction;
    transaction.transactionId = totalTransactionCount++;
    transaction.offset = offset;
    // Calculate the sizes for undo and redo data for the transaction
    VMSizeT oldSize = max(min(length, vmSegment.size() - offset), size_t{0});
    vector<char>::iterator vmBegin = vmSegment.begin() + offset;
    transaction.oldData.resize(oldSize);
    transaction.newData.resize(length);
    // Copy the managed data from the VM segment offset to the undo data, and from newData to the redo data
    copy(vmBegin, vmBegin + oldSize, transaction.oldData.begin());
    copy(newData, newData + length, transaction.newData.begin());

    // Extend the managed VM segment if required for writing outside bounds of the segment
    // Can be easily done because the VM segment is a vector of chars.
    if (offset + length > vmSegment.size()) {
        vmSegment.resize(offset + length);
    }
    // Also copy the newData to the VM segment at the offset
    copy(newData, newData + length, vmSegment.begin() + offset);

    uncommittedTransactions.push_back(transaction);
    return transaction.transactionId;
}

int BaseTransactionManager::abortTransaction(TransactionID transactionId) {
    for (auto it = uncommittedTransactions.begin(); it != uncommittedTransactions.end(); it++) {
        if (it->transactionId == transactionId) {
            // Apply the undo data from the transaction to the VM segment, and erase the transaction
            copy(it->oldData.begin(), it->oldData.end(), vmSegment.begin() + it->offset);
            uncommittedTransactions.erase(it);
            return 0;
        }
    }
    return -1;
}

int BaseTransactionManager::replayTransactions(const vector<Transaction>& transactions) {
    // First find the max last index in the transactions
    auto maxOffsetElement = max_element(transactions.begin(), transactions.end(), [](const Transaction& t1, const Transaction& t2) {
        return (t1.offset + t1.newData.size()) < (t2.offset + t2.newData.size());
    });
    if (maxOffsetElement == transactions.end()) {
        return 0;
    }
    VMSizeT maxOffset = maxOffsetElement->offset + maxOffsetElement->newData.size();
    // Resize the VM segment to accommodate the max offset
    if (maxOffset > vmSegment.size()) {
        vmSegment.resize(maxOffset);
    }
    for (const auto& transaction: transactions) {
        copy(transaction.newData.begin(), transaction.newData.end(), vmSegment.begin() + transaction.offset);
    }
    return 0;
}

vector<char>& BaseTransactionManager::getVMBase() {
    return vmSegment;
}

TransactionManager::TransactionManager(const fs::path& originalFilePath, VMSegment&& vmSegment)
    : BaseTransactionManager(move(vmSegment)), logFilePath(originalFilePath.string() + ".log") {}

int TransactionManager::commitTransaction(TransactionID transactionId, int bytes) {
    for (auto it = uncommittedTransactions.begin(); it != uncommittedTransactions.end(); it++) {
        if (it->transactionId == transactionId) {
            // If `bytes` is provided, then only commit the first `bytes` bytes of the transaction
            if (bytes != -1) {
                if (bytes > it->newData.size()) {
                    return -1;
                } else {
                    it->newData.resize(bytes);
                }
            }
            LogManager::writeTransaction(logFilePath, *it);
            uncommittedTransactions.erase(it);
            return 0;
        }
    }
    return -1;
}

fs::path TransactionManager::getLogFilePath() const {
    return logFilePath;
}

ostream& operator<<(ostream& os, const Transaction& transaction) {
    os<< transaction.transactionId<< " "<< transaction.offset<< " "<< transaction.newData.size()<< " ";
    for (const auto& c: transaction.newData) {
        os << c;
    }
    return os;
}

istream& operator>>(istream& is, Transaction& transaction) {
    size_t newDataSize;
    // Skip whitespaces (because redo data might have whitespace chars) and read the transaction id, offset and redo data size
    is.unsetf(ios_base::skipws);
    is>> transaction.transactionId;
    is.ignore(1);
    is >> transaction.offset;
    is.ignore(1);
    is >> newDataSize;
    is.ignore(1);
    transaction.oldData.clear();
    transaction.newData.resize(newDataSize);
    // Read the redo data now
    for (size_t i = 0; i < newDataSize; ++i) {
        is >> transaction.newData[i];
    }
    return is;
}

vector<Transaction> LogManager::getTransactionsInLog(const fs::path& logFilePath) {
    ifstream logFile(logFilePath, ios::binary);
    vector<Transaction> transactions;
    if (!logFile.is_open()) {
        return transactions;
    }
    Transaction transaction;
    // Read the transactions one by one from the log file until eof
    while (logFile.peek() != char_traits<char>::eof() && logFile>> transaction) {
        transactions.push_back(transaction);
    }
    logFile.close();
    return transactions;
}

int LogManager::writeTransaction(const fs::path& logFilePath, const Transaction& transaction) {
    ofstream logFile(logFilePath, ios::binary | ios::app);
    if (!logFile.is_open()) {
        return -1;
    }
    logFile<< transaction;
    logFile.close();
    return 0;
}
