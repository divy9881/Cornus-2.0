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
    this->_prepare_buffer_signal = new std::condition_variable;
    log_spill_required = false;
    this->_prepare_buf_size = 0;

    // Initialize commit buffer
    this->_max_commit_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_commit_flush_timestamp = 0;
    this->_prepare_buffer_lock = new std::mutex;
    this->_prepare_buffer_signal = new std::condition_variable;
    this->_commit_buf_size = 0;
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


int LogBuffer::add_prepare_log(uint64_t node_id, uint64_t txn_id, int status, std::string data)
{
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_prepare_buffer_lock));
    int ret = 0;
    buffer_unique_lock.lock();
    while (this->_prepare_buffer_spilling)
        this->_prepare_buffer_signal->wait(buffer_unique_lock);
    if (this->_prep_buf_size >= LOG_BUFFER_HW_CAPACITY)
    {
#if DEBUG_PRINT
        printf("Log buffer has %ld transactions, total logs (%ld); "
               "near High watermark capacity (%d). "
               "Signalled the log spill thread. Time: %lu\n",
               this->_prepare_buffer.size(), this->_prep_buf_size,
               LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
        // Schedule log writer thread to empty out the log buffer
        this->flush_prepare_logs();
        this->_prepare_buffer_signal->notify_all();
        buffer_unique_lock.unlock();
        return 0;
    }
    std::string status_data = "E"; // Empty status
    if (status != -1)
    {
        status_data = std::to_string(status);
    }
    status_data += data;
    this->_prepare_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_prep_buf_size++;
//     if (this->_prep_buf_size == DEFAULT_BUFFER_SIZE)
//     {
// #if DEBUG_PRINT
//         printf("Log buffer has (%ld) transactions, total logs (%d), FULL. "
//                "Spilling the logs to the persistent storage. Time: %lu\n",
//                this->_prepare_buffer.size(), this->_prep_buf_size, get_sys_clock());
// #endif
//     }
    buffer_unique_lock.unlock();
    return ret;
}


int LogBuffer::add_commit_log(uint64_t node_id, uint64_t txn_id, int status, std::string data)
{
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_commit_buffer_lock));
    int ret = 0;
    buffer_unique_lock.lock();
    while (this->_commit_buffer_spilling)
        this->_commit_buffer_signal->wait(buffer_unique_lock);
    if (this->_prep_buf_size >= LOG_BUFFER_HW_CAPACITY)
    {
#if DEBUG_PRINT
        printf("Log buffer has %ld transactions, total logs (%ld); "
               "near High watermark capacity (%d). "
               "Signalled the log spill thread. Time: %lu\n",
               this->_commit_buffer.size(), this->_prep_buf_size,
               LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
        // Schedule log writer thread to empty out the log buffer
        this->flush_commit_logs();
        this->_commit_buffer_signal->notify_all();
        buffer_unique_lock.unlock();
        return 0;
    }
    std::string status_data = "E"; // Empty status
    if (status != -1)
    {
        status_data = std::to_string(status);
    }
    status_data += data;
    this->_commit_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_prep_buf_size++;
//     if (this->_prep_buf_size == DEFAULT_BUFFER_SIZE)
//     {
// #if DEBUG_PRINT
//         printf("Log buffer has (%ld) transactions, total logs (%d), FULL. "
//                "Spilling the logs to the persistent storage. Time: %lu\n",
//                this->_commit_buffer.size(), this->_prep_buf_size, get_sys_clock());
// #endif
//     }
    buffer_unique_lock.unlock();
    return ret;
}

void LogBuffer::print()
{
    printf("The buffer has %ld transactions and %ld logs\n", this->_prepare_buffer.size(), this->_prep_buf_size);
    for (auto a : this->_prepare_buffer)
    {
        std::cout << a.first << " ";
        for (auto iter : a.second)
        {
            std::cout << iter.first << ":" << iter.second << "|";
        }
        std::cout << std::endl;
    }
}

// The logs will be of the format txn_id1-nodeid1:msg1|nodeid2:msg2;txn_id2-nodeid1:msg1|nodeid2:msg2;
// TODO 1. Different types of logs for both phases (?)
void LogBuffer::flush_prepare_logs(void)
{
    std::unique_lock<std::mutex> unique_buffer_lock(*(this->_prepare_buffer_lock));

    // Lock the mutex and set the spilling flag
    unique_buffer_lock.lock();
    this->_prepare_buffer_spilling = true;

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
    this->_prep_buf_size = 0; // all the logs are popped

    this->_prepare_buffer_spilling = false;
    this->last_prepare_flush_timestamp = get_server_clock();

#if DEBUG_PRINT
    printf("Logs last flushed timestamp %lu\n", this->last_prepare_flush_timestamp);
#endif
    // unset the manual spill flag if set
    if (log_spill_required)
    {
        log_spill_required = false;
    }
    // Unlock the buffer after reading and emptying it. We will keep on spilling async.
    this->_prepare_buffer_signal->notify_all();
    unique_buffer_lock.unlock();

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
        // If this was a force spill, exit after spilling
        // if (force_empty)
        // {
        //     return NULL;
        // }
    // }
    // TODO Need to update the txn table based on the txn IDs which are committed
    return NULL;
}

void LogBuffer::flush_commit_logs(void)
{
    std::unique_lock<std::mutex> unique_buffer_lock(*(this->_commit_buffer_lock));

    // Lock the mutex and set the spilling flag
    unique_buffer_lock.lock();
    this->_commit_buffer_spilling = true;

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
    this->_prep_buf_size = 0; // all the logs are popped

    this->_commit_buffer_spilling = false;
    this->last_commit_flush_timestamp = get_server_clock();

#if DEBUG_PRINT
    printf("Logs last flushed timestamp %lu\n", this->last_commit_flush_timestamp);
#endif
    // unset the manual spill flag if set
    if (log_spill_required)
    {
        log_spill_required = false;
    }
    // Unlock the buffer after reading and emptying it. We will keep on spilling async.
    this->_commit_buffer_signal->notify_all();
    unique_buffer_lock.unlock();

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
        // If this was a force spill, exit after spilling
        // if (force_empty)
        // {
        //     return NULL;
        // }
    // }
    // TODO Need to update the txn table based on the txn IDs which are committed
    return NULL;
}

