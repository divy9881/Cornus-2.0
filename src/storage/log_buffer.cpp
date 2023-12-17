#include <queue>
#include <string>
#include <algorithm>
#include <iostream>

#include "log_buffer.h"

extern RedisClient*     redis_client;
extern AzureBlobClient* azure_blob_client;
extern LogBuffer*       LOGGER;

LogBuffer::LogBuffer()
{
    // Initialize prepare buffer
    this->_max_prepare_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_prepare_flush_timestamp = 0;
    this->_prepare_buffer_spilling = false;
    this->_prepare_buffer_lock = new std::mutex;
    this->_prepare_flush_lock = new std::mutex;
    this->_prepare_buffer_signal = new std::condition_variable;
    this->prepare_buffer_condition = new std::condition_variable;
    log_spill_required = false;
    this->_prepare_buf_size = 0;

    // Initialize commit buffer
    this->_max_commit_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_commit_flush_timestamp = 0;
    this->_commit_buffer_spilling = false;
    this->_commit_buffer_lock = new std::mutex;
    this->_commit_flush_lock = new std::mutex;
    this->_commit_buffer_signal = new std::condition_variable;
    this->_commit_buf_size = 0;
    this->commit_buffer_condition = new std::condition_variable;

    this->_max_sync_log_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_sync_log_flush_timestamp = 0;
    this->_sync_log_buffer_spilling = false;
    this->_sync_log_buffer_lock = new std::mutex;
    this->_sync_log_flush_lock = new std::mutex;
    this->_sync_log_buffer_signal = new std::condition_variable;
    this->sync_log_buffer_condition = new std::condition_variable;
    
}

LogBuffer::~LogBuffer()
{
}

// (static LogBuffer*) LogBuffer::getBufferInstance() {
//     if (LogBuffer::logBufferInstance == NULL) {
//         logBufferInstance = new LogBuffer();
//         return logBufferInstance;
//     }
//     return logBufferInstance;
// }

void LogBuffer::print()
{
    printf("The PREPARE buffer has %ld transactions and %ld logs\n", this->_prepare_buffer.size(), this->_prepare_buf_size);
    for (auto a : this->_prepare_buffer)
    {
        std::cout << a.first << " ";
        for (auto iter : a.second)
        {
            std::cout << iter.first << ":" << iter.second << "|";
        }
        std::cout << std::endl;
    }

    printf("The COMMIT buffer has %ld transactions and %ld logs\n", this->_commit_buffer.size(), this->_commit_buf_size);
    for (auto a : this->_commit_buffer)
    {
        std::cout << a.first << " ";
        for (auto iter : a.second)
        {
            std::cout << iter.first << ":" << iter.second << "|";
        }
        std::cout << std::endl;
    }
}

int LogBuffer::add_prepare_log(uint64_t node_id, uint64_t txn_id, int status, std::string data)
{
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_prepare_buffer_lock));
    int ret = 0;

    while (this->_prepare_buffer_spilling.load())
        this->_prepare_buffer_signal->wait(buffer_unique_lock);

    // Check If the log requires flushing
    if (this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
        // If the transaction logs already exist in the buffer, since we still have some
        // space to allow logs, add the log without flushing
        if (this->_prepare_buffer.find(txn_id) == this->_prepare_buffer.end()) {
            // If the txn does not exist in the logs, first flush.
            std::unique_lock <std::mutex> flush_lock(*(this->_prepare_flush_lock));
            // Recheck the flush conditions to avoid race
            if (!this->_prepare_buffer_spilling.load() &&
                this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
#if DEBUG_PRINT
            printf("PREPARE Log buffer has %ld transactions, total logs (%ld); "
                "near High watermark capacity (%d). "
                "Signalled the log spill thread. Time: %lu\n",
                this->_prepare_buffer.size(), this->_prepare_buf_size,
                LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
                    this->_prepare_buffer_spilling.store(true);
                    
                    std::vector<uint64_t> txn_id_cache;
                    std::vector<std::string> txn_log_str; // we store one string per transaction ID

                    uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
                    std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

                    for (auto buffer_iterator : this->_prepare_buffer)
                    {
                        uint64_t log_txn_id = buffer_iterator.first;
                        std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;
                        // Sort on basis of the node id for easier processing of the message later
                        // std::sort(node_id_message_list.begin(), node_id_message_list.end());

                        // Store the largest LSN for a node
                        string current_txn_log;
                        for (auto node_id_msg_iter : node_id_message_list)
                        {
                            if (largest_txn_id < log_txn_id)
                            {
                                largest_txn_id = log_txn_id;
                            }
                            if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id)
                            {
                                node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                            }
                            current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                        }
                        txn_id_cache.push_back(log_txn_id);
                        txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
                    }
                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        this->_prepare_buffer.erase(txn_id_cache[ii]);
                    }
                    this->_prepare_buf_size = 0; // all the logs are popped

                    this->_prepare_buffer_spilling.store(false);
                    this->last_prepare_flush_timestamp = get_server_clock();
                    this->_prepare_buffer_signal->notify_all();
                    buffer_unique_lock.unlock();

                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        uint64_t txn_id = txn_id_cache[ii];
                        std::string txn_logs = txn_log_str[ii];
                        // TODO set the LSN in the txn_table for the transactions
                        // TODO How to read the logs for one transaction, for one node
                        #if LOG_DEVICE == LOG_DVC_REDIS
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                            azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #endif
                    }
                }
                // It also needs to commit its own lock, so try to acquire this lock again
                buffer_unique_lock.lock();
        } // The flush_lock is released as the scope is over. Subsequent threads will see that the buffer_size is less than watermark

    }
    std::string status_data = "E"; // Empty status
    if (status != -1)
    {
        status_data = std::to_string(status);
    }
    status_data += data;
    // We still hold the buffer_unique_lock
    this->_prepare_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_prepare_buf_size++;
    buffer_unique_lock.unlock();
    return ret;
}


int LogBuffer::add_commit_log(uint64_t node_id, uint64_t txn_id, int status, std::string data)
{
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_commit_buffer_lock));
    int ret = 0;

    while (this->_commit_buffer_spilling.load())
        this->_commit_buffer_signal->wait(buffer_unique_lock);

    // Check If the log requires flushing
    if (this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
        // If the transaction logs already exist in the buffer, since we still have some
        // space to allow logs, add the log without flushing
        if (this->_commit_buffer.find(txn_id) == this->_commit_buffer.end()) {
            // If the txn does not exist in the logs, first flush.
            std::unique_lock <std::mutex> flush_lock(*(this->_commit_flush_lock));
            // Recheck the flush conditions to avoid race
            if (!this->_commit_buffer_spilling.load() &&
                this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
#if DEBUG_PRINT
            printf("commit Log buffer has %ld transactions, total logs (%ld); "
                "near High watermark capacity (%d). "
                "Signalled the log spill thread. Time: %lu\n",
                this->_commit_buffer.size(), this->_commit_buf_size,
                LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
                    this->_commit_buffer_spilling.store(true);
                    // we map the transaction ID to txn_log_str[ii]; one-to-one
                    std::vector<uint64_t> txn_id_cache;
                    std::vector<std::string> txn_log_str; // we store one string per transaction ID

                    uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
                    std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

                    for (auto buffer_iterator : this->_commit_buffer)
                    {
                        uint64_t log_txn_id = buffer_iterator.first;
                        std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;
                        // Sort on basis of the node id for easier processing of the message later
                        // std::sort(node_id_message_list.begin(), node_id_message_list.end());

                        // Store the largest LSN for a node
                        string current_txn_log;
                        for (auto node_id_msg_iter : node_id_message_list)
                        {
                            if (largest_txn_id < log_txn_id)
                            {
                                largest_txn_id = log_txn_id;
                            }
                            if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id)
                            {
                                node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                            }
                            current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                        }
                        txn_id_cache.push_back(log_txn_id);
                        txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
                    }
                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        this->_commit_buffer.erase(txn_id_cache[ii]);
                    }
                    this->_commit_buf_size = 0; // all the logs are popped

                    this->_commit_buffer_spilling = false;
                    this->last_commit_flush_timestamp = get_server_clock();

                #if DEBUG_PRINT
                    printf("Logs last flushed timestamp %lu\n", this->last_commit_flush_timestamp);
                #endif
                    // unset the manual spill flag if set
                    this->_commit_buffer_spilling.store(false);
                    this->_commit_buffer_signal->notify_all();
                    buffer_unique_lock.unlock();

                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        uint64_t txn_id = txn_id_cache[ii];
                        std::string txn_logs = txn_log_str[ii];
                        // TODO set the LSN in the txn_table for the transactions
                        // TODO How to read the logs for one transaction, for one node
                        #if LOG_DEVICE == LOG_DVC_REDIS
                                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                                azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                        #endif
                    }
                    // The current thread also needs to flush its own logs
                    buffer_unique_lock.lock();
            }
        } // The flush_lock is released as the scope is over. Subsequent threads will see that the buffer_size is less than watermark
    }
    std::string status_data = "E"; // Empty status
    if (status != -1)
    {
        status_data = std::to_string(status);
    }
    status_data += data;
    // We still hold the buffer_unique_lock
    this->_commit_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_commit_buf_size++;
    buffer_unique_lock.unlock();
    return ret;
}

void LogBuffer::flush_prepare_logs(void) {
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_prepare_buffer_lock), std::defer_lock);
    // Wait until there's something to flush or it's time to wake up
    this->prepare_buffer_condition->wait_for(buffer_unique_lock, std::chrono::milliseconds(EMPTY_LOG_BUFFER_TIMEDELTA), [&] {
        return !this->prepare_flush_thread_running || !this->_prepare_buffer.empty();
    });

    if (!this->_prepare_buffer.empty()) {
        buffer_unique_lock.lock();
        // Process and flush the prepare_buffer
        // std::cout << "Flushing " << prepare_buffer.size() << " items\n";
        std::unique_lock <std::mutex> flush_lock(*(this->_prepare_flush_lock));
            // Recheck the flush conditions to avoid race
        if (!this->_prepare_buffer_spilling.load() &&
            this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
#if DEBUG_PRINT
        printf("prepare Log buffer has %ld transactions, total logs (%ld); "
            "near High watermark capacity (%d). "
            "Signalled the log spill thread. Time: %lu\n",
            this->_prepare_buffer.size(), this->_prepare_buf_size,
            LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
                this->_prepare_buffer_spilling.store(true);
                // we map the transaction ID to txn_log_str[ii]; one-to-one
                std::vector<uint64_t> txn_id_cache;
                std::vector<std::string> txn_log_str; // we store one string per transaction ID

                uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
                std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

                for (auto buffer_iterator : this->_prepare_buffer)
                {
                    uint64_t log_txn_id = buffer_iterator.first;
                    std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;
                    // Sort on basis of the node id for easier processing of the message later
                    // std::sort(node_id_message_list.begin(), node_id_message_list.end());

                    // Store the largest LSN for a node
                    string current_txn_log;
                    for (auto node_id_msg_iter : node_id_message_list)
                    {
                        if (largest_txn_id < log_txn_id)
                        {
                            largest_txn_id = log_txn_id;
                        }
                        if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id)
                        {
                            node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                        }
                        current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                    }
                    txn_id_cache.push_back(log_txn_id);
                    txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
                }
                for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                {
                    this->_prepare_buffer.erase(txn_id_cache[ii]);
                }
                this->_prepare_buf_size = 0; // all the logs are popped

                this->_prepare_buffer_spilling.store(false);
                this->last_prepare_flush_timestamp = get_server_clock();

            #if DEBUG_PRINT
                printf("Logs last flushed timestamp %lu\n", this->last_prepare_flush_timestamp);
            #endif
                // unset the manual spill flag if set
                this->_prepare_buffer_signal->notify_all();
                buffer_unique_lock.unlock();

                for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                {
                    uint64_t txn_id = txn_id_cache[ii];
                    std::string txn_logs = txn_log_str[ii];
                    // TODO set the LSN in the txn_table for the transactions
                    // TODO How to read the logs for one transaction, for one node
                    #if LOG_DEVICE == LOG_DVC_REDIS
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                            azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #endif
                }
        }
    }
}

void LogBuffer::flush_commit_logs() {
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_commit_buffer_lock), std::defer_lock);
    // Wait until there's something to flush or it's time to wake up
    this->commit_buffer_condition->wait_for(buffer_unique_lock, std::chrono::milliseconds(EMPTY_LOG_BUFFER_TIMEDELTA), [&] {
        return !this->commit_flush_thread_running | !this->_commit_buffer.empty();
    });

    if (!this->_commit_buffer.empty()) {
        buffer_unique_lock.lock();
        // Process and flush the commit_buffer
        // std::cout << "Flushing " << commit_buffer.size() << " items\n";
        std::unique_lock <std::mutex> flush_lock(*(this->_commit_flush_lock));
            // Recheck the flush conditions to avoid race
        if (!this->_commit_buffer_spilling.load() &&
            this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
#if DEBUG_PRINT
        printf("commit Log buffer has %ld transactions, total logs (%ld); "
            "near High watermark capacity (%d). "
            "Signalled the log spill thread. Time: %lu\n",
            this->_commit_buffer.size(), this->_commit_buf_size,
            LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
                this->_commit_buffer_spilling.store(true);
                // we map the transaction ID to txn_log_str[ii]; one-to-one
                std::vector<uint64_t> txn_id_cache;
                std::vector<std::string> txn_log_str; // we store one string per transaction ID

                uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
                std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

                for (auto buffer_iterator : this->_commit_buffer)
                {
                    uint64_t log_txn_id = buffer_iterator.first;
                    std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;
                    // Sort on basis of the node id for easier processing of the message later
                    // std::sort(node_id_message_list.begin(), node_id_message_list.end());

                    // Store the largest LSN for a node
                    string current_txn_log;
                    for (auto node_id_msg_iter : node_id_message_list)
                    {
                        if (largest_txn_id < log_txn_id)
                        {
                            largest_txn_id = log_txn_id;
                        }
                        if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id)
                        {
                            node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                        }
                        current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                    }
                    txn_id_cache.push_back(log_txn_id);
                    txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
                }
                for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                {
                    this->_commit_buffer.erase(txn_id_cache[ii]);
                }
                this->_commit_buf_size = 0; // all the logs are popped

                this->_commit_buffer_spilling.store(false);
                this->last_commit_flush_timestamp = get_server_clock();

            #if DEBUG_PRINT
                printf("Logs last flushed timestamp %lu\n", this->last_commit_flush_timestamp);
            #endif
                // unset the manual spill flag if set
                this->_commit_buffer_signal->notify_all();
                buffer_unique_lock.unlock();

                for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                {
                    uint64_t txn_id = txn_id_cache[ii];
                    std::string txn_logs = txn_log_str[ii];
                    // TODO set the LSN in the txn_table for the transactions
                    // TODO How to read the logs for one transaction, for one node
                    #if LOG_DEVICE == LOG_DVC_REDIS
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                            azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
                    #endif
                }
        }
    }
}

void LogBuffer::start_prepare_flush_thread() {
    if (!this->prepare_flush_thread_running) {
        this->prepare_flush_thread_running = true;
        std::thread(&LogBuffer::flush_prepare_logs, this).detach();
    }
}

void LogBuffer::stop_prepare_flush_thread() {
    this->prepare_flush_thread_running = false;
    // Notify the flush thread in case it's waiting for new logs
    this->prepare_buffer_condition->notify_one();
}

void LogBuffer::stop_commit_flush_thread() {
    this->commit_flush_thread_running = false;
    this->commit_buffer_condition->notify_one();
}

void LogBuffer::start_commit_flush_thread() {
    if (!this->commit_flush_thread_running) {
        this->commit_flush_thread_running = true;
        std::thread(&LogBuffer::flush_commit_logs, this).detach();
    }
}
/*
int add_sync_log(uint64_t node_id, uint64_t txn_id, int status) {
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_sync_log_buffer_lock));
    int ret = 0;

    while (this->_sync_log_buffer_spilling.load())
        this->_sync_log_buffer_signal->wait(buffer_unique_lock);

    // Check If the log requires flushing
    if (this->_sync_log_buf_size >= LOG_BUFFER_HW_CAPACITY) {
        // If the transaction logs already exist in the buffer, since we still have some
        // space to allow logs, add the log without flushing
        if (this->_sync_log_buffer.find(txn_id) == this->_sync_log_buffer.end()) {
            // If the txn does not exist in the logs, first flush.
            std::unique_lock <std::mutex> flush_lock(*(this->_sync_log_flush_lock));
            // Recheck the flush conditions to avoid race
            if (!this->_sync_log_buffer_spilling.load() &&
                this->_sync_log_buf_size >= LOG_BUFFER_HW_CAPACITY) {
#if DEBUG_PRINT
            printf("sync_log Log buffer has %ld transactions, total logs (%ld); "
                "near High watermark capacity (%d). "
                "Signalled the log spill thread. Time: %lu\n",
                this->_sync_log_buffer.size(), this->_sync_log_buf_size,
                LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
                    this->_sync_log_buffer_spilling.store(true);
                    // we map the transaction ID to txn_log_str[ii]; one-to-one
                    std::vector<uint64_t> txn_id_cache;
                    std::vector<std::pair<uint64_t, int>> node_status_str; // we store one string per transaction ID

                    uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
                    std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

                    for (auto buffer_iterator : this->_sync_log_buffer)
                    {
                        uint64_t log_txn_id = buffer_iterator.first;
                        std::vector<std::pair<uint64_t, int>> node_id_status_list = buffer_iterator.second;

                        for (auto node_id_msg_iter : node_id_status_list)
                        {
                            if (largest_txn_id < log_txn_id)
                            {
                                largest_txn_id = log_txn_id;
                            }
                            if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id)
                            {
                                node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                            }
                        }
                        txn_id_cache.push_back(log_txn_id);
                        node_status_str.push_back(node_id_msg_iter);
                    }
                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        this->_sync_log_buffer.erase(txn_id_cache[ii]);
                    }
                    this->_sync_log_buf_size = 0; // all the logs are popped

                    this->_sync_log_buffer_spilling = false;
                    this->last_sync_log_flush_timestamp = get_server_clock();

                #if DEBUG_PRINT
                    printf("Logs last flushed timestamp %lu\n", this->last_sync_log_flush_timestamp);
                #endif
                    // unset the manual spill flag if set
                    this->_sync_log_buffer_spilling.store(false);
                    this->_sync_log_buffer_signal->notify_all();
                    buffer_unique_lock.unlock();

                    for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++)
                    {
                        uint64_t txn_id = txn_id_cache[ii];
                        std::string txn_logs = txn_log_str[ii];
                        // TODO set the LSN in the txn_table for the transactions
                        // TODO How to read the logs for one transaction, for one node
                        #if LOG_DEVICE == LOG_DVC_REDIS
                                ret = redis_client->log_sync(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                                ret = azure_blob_client->log_sync(txn_id, largest_txn_id, txn_logs);
                        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                                ret = redis_client->log_sync(txn_id, largest_txn_id, txn_logs);
                        #endif
                    }
                    // The current thread also needs to flush its own logs
                    buffer_unique_lock.lock();
            }
        } // The flush_lock is released as the scope is over. Subsequent threads will see that the buffer_size is less than watermark
    }

    // We still hold the buffer_unique_lock
    this->_sync_log_buffer[txn_id].push_back(std::make_pair(node_id, status));
    this->_sync_log_buf_size++;
    buffer_unique_lock.unlock();
    return ret;
}

void LogBuffer::start_sync_log_flush_thread() {
    if (!this->sync_log_flush_thread_running) {
        this->sync_log_flush_thread_running = true;
        std::thread(&LogBuffer::flush_sync_logs, this).detach();
    }
}
*/