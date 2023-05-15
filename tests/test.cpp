#include "../src/gtfs.hpp"
#include <cstring>
#include <sys/wait.h>

// Assumes files are located within the current directory
string directory;
int verbose;

// **Test 1**: Testing that data written by one process is then successfully read by another process.
void writer(bool partial_sync = false) {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    write_t *wrt = gtfs_write_file(gtfs, fl, 10, str.length(), str.c_str());
    if (!partial_sync) {
        gtfs_sync_write_file(wrt);
    } else {
        gtfs_sync_write_file_n_bytes(wrt, str.length() / 2);
    }

    gtfs_close_file(gtfs, fl);
}

void reader(bool partial_sync = false) {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    if (partial_sync) {
        str = str.substr(0, str.length() / 2);
    }
    char *data = gtfs_read_file(gtfs, fl, 10, str.length());
    if (data != NULL) {
        string dataStr(data);
        (dataStr.size() == str.size() && str.compare(dataStr) == 0) ? cout << PASS : cout << FAIL;
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

void test_write_read() {
    int pid;
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        writer();
        exit(0);
    }
    waitpid(pid, NULL, 0);
    reader();
}

// **Test 2**: Testing that aborting a write returns the file to its original contents.

void test_abort_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test2.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_abort_write_file(wrt2);

    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (string(data2).compare("") == 0) {
            cout << PASS;
        }
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

// **Test 3**: Testing that the logs are truncated.

void test_truncate_log() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test3.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");

    gtfs_clean(gtfs);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    cout << "If log is truncated: " << PASS << "If exactly same output:" << FAIL;

    gtfs_close_file(gtfs, fl);

}

// TODO: Implement any additional tests

/** Testing that another process can read partial data after gtfs_sync_write_file_n_bytes() is called */
void test_write_partial_sync_read() {
    int pid;
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        writer(true);
        exit(0);
    }
    waitpid(pid, NULL, 0);
    reader(true);
}

/** Testing that gtfs_clean_n_bytes() only applies first n bytes of the data */
void test_truncate_log_partial() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test4.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    int bytes = 3 * str.length() / 2;
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);
    gtfs_close_file(gtfs, fl);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");

    // Clean 3n/2 bytes, which should apply the first transaction but drop the second half-transcation
    gtfs_clean_n_bytes(gtfs, bytes);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    // Ensure all log files are deleted
    for (auto& p: fs::directory_iterator(gtfs->dirname)) {
        if (fs::is_regular_file(p) && p.path().extension() == ".log") {
            cout<< "Contains log file: " << p.path() << ", "<< FAIL;
            return;
        }
    }

    fl = gtfs_open_file(gtfs, filename, 100);
    // Try reading the two written ranges and verify that only first range is read
    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
    if (data1 && data2 && strlen(data2) == 0) {
        string dataStr(data1);
        (dataStr.size() == str.size() && str.compare(dataStr) == 0) ? cout << PASS : cout << FAIL;
    } else {
        cout << FAIL;
    }
}

/** Testing that files can be removed */
void test_remove_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    // Create a test file and close it
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs, fl);

    cout << "Before gtfs_remove_file() call\n";
    system("ls -l .");
    auto filePath = fs::path(directory) / filename;
    cout << filename << " exists: " << fs::exists(filePath) << endl;

    gtfs_remove_file(gtfs, fl);

    cout << "After gtfs_remove_file() call\n";
    system("ls -l .");
    bool exists = fs::exists(filePath);
    cout << filename << " exists: " << exists << (exists ? FAIL : PASS);
}

/** Testing that files can be removed */
void test_remove_synced_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs, fl);

    cout << "Before gtfs_remove_file() call\n";
    system("ls -l .");
    auto filePath = fs::path(directory) / filename;
    auto logFileName = filename + ".log";
    auto logFilePath = fs::path(directory) / logFileName;
    cout << filename << " exists: " << fs::exists(filePath) << ", " << logFileName << " exists: " << fs::exists(logFilePath) << endl;

    gtfs_remove_file(gtfs, fl);

    cout << "After gtfs_remove_file() call\n";
    system("ls -l .");
    bool exists = fs::exists(filePath);
    bool logExists = fs::exists(logFilePath);
    cout << filename << " exists: " << exists << ", " << logFileName << " exists: " << logExists << (exists || logExists ? FAIL : PASS);
}

/** Testing that removing an open file fails */
void test_remove_open_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    int ret = gtfs_remove_file(gtfs, fl);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "gtfs_remove_file() returns " << ret << ": " << PASS;
    } else {
        cout << "gtfs_remove_file() returns success code: " << FAIL;
    }
}

/** Testing that read returns NULL and write returns error for a closed file */
void test_read_write_closed_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test6.txt";
    // Create a test file and close it
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs, fl);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());

    char *data = gtfs_read_file(gtfs, fl, 0, 10);
    if (!data && !wrt1) {
        cout << "Read data is NULL and gtfs_write_file() returns null pointer: " << PASS;
    } else {
        cout << "Read data is " << data << ": " << FAIL;
    }
}

/** Testing that read returns "" for segment of a file which hasn't been written to */
void test_read_unwritten_data() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test7.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    char *data = gtfs_read_file(gtfs, fl, str.length(), 10);
    gtfs_close_file(gtfs, fl);
    if (data && strlen(data) == 0) {
        cout << "Read data is empty: " << PASS;
    } else {
        cout << "Read data is " << (!data ? "NULL" : data) << ": " << FAIL;
    }
}

/** Testing that opening an already open file returns a null file_t* */
void test_open_already_open_file() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test8.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    int pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        file_t *fl2 = gtfs_open_file(gtfs, filename, 100);
        exit(fl2 == nullptr ? 0 : -1);
    }
    int status;
    waitpid(pid, &status, 0);
    
    gtfs_close_file(gtfs, fl);
    if (fl && WEXITSTATUS(status) == 0) {
        cout << "Second open returns null pointer: " << PASS;
    } else {
        cout << "Second open returns non-null file pointer: " << FAIL;
    }
}

/** Testing that syncing more bytes than written fails */
void test_sync_write_more_bytes_than_written() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    int ret = gtfs_sync_write_file_n_bytes(wrt1, str.length() + 1);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "gtfs_sync_write_file_n_bytes() with more bytes than written fails correctly: " << PASS;
    } else {
        cout << "gtfs_sync_write_file_n_bytes() with more bytes than written succeeds: " << FAIL;
    }
}

/** Testing that syncing a write that doesn't exist fails */
void test_sync_invalid_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    write_t* wrt = new write_t;
    wrt->filename = filename;
    wrt->offset = 0;
    wrt->length = 10;
    wrt->file = fl;
    wrt->transactionId = 0;
    int ret = gtfs_sync_write_file(wrt);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Sync for an invalid write fails correctly: " << PASS;
    } else {
        cout << "Sync for an invalid write returns success status: " << FAIL;
    }
}

/** Testing that syncing a write for which file was closed fails */
void test_sync_closed_write() {
   gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_close_file(gtfs, fl);
    int ret = gtfs_sync_write_file(wrt1);
    if (ret != 0) {
        cout << "Sync for a write for which file was closed fails correctly: " << PASS;
    } else {
        cout << "Sync for a write for which file was closed returns success status: " << FAIL;
    }
}

/** Testing that syncing an aborted write fails */
void test_sync_aborted_write() {
   gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_abort_write_file(wrt1);
    int ret = gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Sync for an aborted write fails correctly: " << PASS;
    } else {
        cout << "Sync for an aborted write returns success status: " << FAIL;
    }
}

/** Testing that syncing a synced write again fails */
void test_sync_synced_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    int ret = gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Syncing a synced write again fails correctly: " << PASS;
    } else {
        cout << "Syncing a synced write again returns success status: " << FAIL;
    }
}

/** Testing that aborting a write that doesn't exist fails */
void test_abort_invalid_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    write_t* wrt = new write_t;
    wrt->filename = filename;
    wrt->offset = 0;
    wrt->length = 10;
    wrt->file = fl;
    wrt->transactionId = 0;
    int ret = gtfs_abort_write_file(wrt);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Aborting an invalid write fails correctly: " << PASS;
    } else {
        cout << "Aborting an invalid write returns success status: " << FAIL;
    }
}

/** Testing that aborting a write for which file was closed fails */
void test_abort_closed_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_close_file(gtfs, fl);
    int ret = gtfs_abort_write_file(wrt1);
    if (ret != 0) {
        cout << "Aborting a write for which file was closed fails correctly: " << PASS;
    } else {
        cout << "Aborting a write for which file was closed returns success status: " << FAIL;
    }
}

/** Testing that aborting an already-aborted write fails */
void test_abort_aborted_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_abort_write_file(wrt1);
    int ret = gtfs_abort_write_file(wrt1);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Aborting an aborted write fails correctly: " << PASS;
    } else {
        cout << "Aborting an aborted write returns success status: " << FAIL;
    }
}

/** Testing that aborting a synced write fails */
void test_abort_synced_write() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);
    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    int ret = gtfs_abort_write_file(wrt1);
    gtfs_close_file(gtfs, fl);
    if (ret != 0) {
        cout << "Aborting a synced write fails correctly: " << PASS;
    } else {
        cout << "Aborting a synced write returns success status: " << FAIL;
    }
}

int main(int argc, char **argv) {
    if (argc < 2)
        printf("Usage: ./test verbose_flag\n");
    else
        verbose = strtol(argv[1], NULL, 10);

    // Get current directory path
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        directory = string(cwd);
    } else {
        cout << "[cwd] Something went wrong.\n";
    }

    // Call existing tests
    cout << "================== Test 1 ==================\n";
    cout << "Testing that data written by one process is then successfully read by another process.\n";
    test_write_read();

    cout << "================== Test 2 ==================\n";
    cout << "Testing that aborting a write returns the file to its original contents.\n";
    test_abort_write();

    cout << "================== Test 3 ==================\n";
    cout << "Testing that the logs are truncated.\n";
    test_truncate_log();

    cout << "================== Test 4 ==================\n";
    cout << "Testing that the second process can only read n bytes if first process only synced first n bytes of the write.\n";
    test_write_partial_sync_read();

    cout << "================== Test 5 ==================\n";
    cout << "Testing that the second process can only read n bytes after clean.\n";
    test_truncate_log_partial();

    cout << "================== Test 6 ==================\n";
    cout << "Testing that a file can be removed.\n";
    test_remove_file();

    cout << "================== Test 7 ==================\n";
    cout << "Testing that removing a file that has synced writes removes the original as well as log file.\n";
    test_remove_synced_file();

    cout << "================== Test 8 ==================\n";
    cout << "Testing that removing an open file fails.\n";
    test_remove_open_file();

    cout << "================== Test 9 ==================\n";
    cout << "Testing that reading from and writing to a closed file returns NULL.\n";
    test_read_write_closed_file();

    cout << "================== Test 10 ==================\n";
    cout << "Testing that read returns \"\" for segment of a file which hasn't been written to.\n";
    test_read_unwritten_data();

    cout << "================== Test 11 ==================\n";
    cout << "Testing that opening an already open file returns a null file_t*.\n";
    test_open_already_open_file();

    cout << "================== Test 12 ==================\n";
    cout << "Testing that syncing more bytes than written fails.\n";
    test_sync_write_more_bytes_than_written();

    cout << "================== Test 13 ==================\n";
    cout << "Testing that syncing a write that doesn't exist fails.\n";
    test_sync_invalid_write();

    cout << "================== Test 14 ==================\n";
    cout << "Testing that syncing a write for which file was closed fails.\n";
    test_sync_closed_write();

    cout << "================== Test 15 ==================\n";
    cout << "Testing that syncing an aborted write fails.\n";
    test_sync_aborted_write();

    cout << "================== Test 16 ==================\n";
    cout << "Testing that aborting a write that doesn't exist fails.\n";
    test_abort_invalid_write();

    cout << "================== Test 17 ==================\n";
    cout << "Testing that aborting a write for which file was closed fails.\n";
    test_abort_closed_write();

    cout << "================== Test 18 ==================\n";
    cout << "Testing that aborting an already-aborted write fails.\n";
    test_abort_aborted_write();

    cout << "================== Test 19 ==================\n";
    cout << "Testing that aborting a synced write fails.\n";
    test_abort_synced_write();

    cout << "================== Test 20 ==================\n";
    cout << "Testing that syncing a synced write again fails.\n";
    test_sync_synced_write();

}
