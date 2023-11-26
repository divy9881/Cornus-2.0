#include <queue>
#include <string>
#include <algorithm>

#include "log_buffer.h"
#include "redis_client.h"
#include "azure_blob_client.h"

int LogBuffer::add_log(uint64_t node_id, uint64_t txn_id, int status, std::string data)
{
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_buffer_lock));
    int ret = 0;
    buffer_unique_lock.lock();
    while (this->is_spilling)
        this->_buffer_signal->wait(buffer_unique_lock);
    if (this->size >= LOG_BUFFER_HW_CAPACITY)
    {
#if DEBUG_PRINT
        printf("Log buffer has %ld transactions, total logs (%d); "
               "near High watermark capacity (%d). "
               "Signalled the log spill thread. Time: %lu\n",
               this->_buffer.size(), this->size,
               LOG_BUFFER_HW_CAPACITY, get_sys_clock());
#endif
        // Schedule log writer thread to empty out the log buffer
        log_spill_required = true;
    }
    string status_data = "E"; // Empty status
    if (status != -1)
    {
        status_data = std::to_string(status);
    }
    status_data += data;
    this->_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->size++;
    if (this->size == DEFAULT_BUFFER_SIZE)
    {
#if DEBUG_PRINT
        printf("Log buffer has (%ld) transactions, total logs (%d), FULL. "
               "Spilling the logs to the persistent storage. Time: %lu\n",
               this->_buffer.size(), this->size, get_sys_clock());
#endif
        struct spiller_args force_arguments;
        force_arguments.logger_instance = LOGGER;
        force_arguments.force = true;
        spill_buffered_logs_to_storage((void *)&force_arguments);
    }
    buffer_unique_lock.unlock();
    return ret;
}

void LogBuffer::print()
{
    printf("The buffer has %ld transactions and %d logs\n", this->_buffer.size(), this->size);
    for (auto a : this->_buffer)
    {
        cout << a.first << " ";
        for (auto iter : a.second)
        {
            cout << iter.first << ":" << iter.second << "|";
        }
        cout << endl;
    }
}

// The logs will be of the format txn_id1-nodeid1:msg1|nodeid2:msg2;txn_id2-nodeid1:msg1|nodeid2:msg2;
// TODO 1. Different types of logs for both phases (?)
void *spill_buffered_logs_to_storage(void *args)
{
    struct spiller_args *arguments = (struct spiller_args *) args;
    bool force_empty = arguments->force;
    LogBuffer *buf = arguments->logger_instance;
    std::unique_lock<std::mutex> unique_buffer_lock(*(buf->_buffer_lock));

    while (true)
    {

        // If this is not an explicit call, wait for standard sleep and wakeup
        if (!force_empty)
        {
            // Wait for either the sleep to complete, or if the log spill
            // is being forced (used in spurious wakeups)
            buf->_buffer_signal->wait_for(unique_buffer_lock,
                                          std::chrono::seconds(EMPTY_LOG_BUFFER_TIMEDELTA),
                                          [&]
                                          { return !buf->_buffer.empty() || log_spill_required; });
        }
        else
        { // If it is an explicit call, do not sleep
#if DEBUG_PRINT
            printf("Force spilling logs to the persistent storage at time: %lu\n", get_server_clock());
#endif
        }

        // Lock the mutex and set the spilling flag
        unique_buffer_lock.lock();
        buf->is_spilling = true;

        // we map the transaction ID to txn_log_str[ii]; one-to-one
        std::vector<uint64_t> txn_id_cache;
        std::vector<std::string> txn_log_str; // we store one string per transaction ID

        uint64_t largest_txn_id = 0;                   // Overall largest TXN ID
        std::map<uint64_t, uint64_t> node_largest_lsn; // Largest LSN for one particular node

        for (auto buffer_iterator : buf->_buffer)
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
            buf->_buffer.erase(txn_id_cache[ii]);
        }
        buf->size = 0; // all the logs are popped

        buf->is_spilling = false;
        buf->last_flush_timestamp = get_server_clock();

#if DEBUG_PRINT
        printf("Logs last flushed timestamp %lu\n", buf->last_flush_timestamp);
#endif
        // unset the manual spill flag if set
        if (log_spill_required)
        {
            log_spill_required = false;
        }
        // Unlock the buffer after reading and emptying it. We will keep on spilling async.
        buf->_buffer_signal->notify_one();
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
            azure_client->log_async_data(txn_id, largest_txn_id, txn_logs);
#elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
            redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
#endif
        }
        // If this was a force spill, exit after spilling
        if (force_empty)
        {
            return NULL;
        }
    }
    // TODO Need to update the txn table based on the txn IDs which are committed
    return NULL;
}