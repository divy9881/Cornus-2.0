#pragma once

#include <queue>
#include <string>
#include <algorithm>

#include "LogBuffer.h"
#include "redis_client.h"
#include "azure_blob_client.h"


int LogBuffer::add_log(uint64_t node_id, uint64_t txn_id, int status, std::string &data) {
    std::unique_lock<std::mutex> buffer_unique_lock(*(this->_buffer_lock));
    int ret = 0;
    buffer_unique_lock.lock();
    while (this->is_spilling)
        this->_buffer_signal->wait(buffer_unique_lock);
    if (this->_buffer.size() >= LOG_BUFFER_HW_CAPACITY) {
        #if DEBUG_PRINT
            printf("Log buffer size (%d) near High watermark capacity (%d). "
               "Signalled the log spill thread. Time: %lu\n",
               this->_buffer.size(), LOG_BUFFER_HW_CAPACITY, get_sys_clock());
        #endif
        // Schedule log writer thread to empty out the log buffer
        log_spill_required = true;
    }
    string status_data = "E"; // Empty status
    if (status != -1) {
        status_data = to_string(status);
    }
    status_data += data;
    this->_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    if (this->_buffer.size() == DEFAULT_BUFFER_SIZE) {
        #if DEBUG_PRINT
            printf("Log buffer size (%d) FULL. "
               "Spilling the logs to the persistent storage. Time: %lu\n",
               this->_buffer.size(), get_sys_clock());
        #endif
        struct spiller_args force_arguments;
        force_arguments.logger_instance = LOGGER;
        force_arguments.force = true;
        spill_buffered_logs_to_storage((void*) &force_arguments);
    }
    buffer_unique_lock.unlock();
    return ret;
}

void LogBuffer::print() {
    cout<<"Size of the map is "<<this->_buffer.size()<<endl;
    for (auto a: this->_buffer) {
        cout << a.first << " ";
        for (auto iter: a.second) {
            cout << iter.first << ":" << iter.second << "|";
        }
        cout << endl;
    }
}

void * spill_buffered_logs_to_storage(void* args) {
    struct spiller_args *arguments = (struct spiller_args) args;
    bool force_empty = arguments->force;
    LogBuffer *buf = arguments->logger_instance;
    std::unique_lock<std::mutex> unique_buffer_lock(*(buf->_buffer_lock));
    while (true) {

        // If this is not an explicit call, wait for standard sleep and wakeup
        if (!force_empty) {
            // Wait for either the sleep to complete, or if the log spill
            // is being forced (used in spurious wakeups) 
            buf->_buffer_signal->wait_for(unique_buffer_lock,
                    std::chrono::seconds(EMPTY_LOG_BUFFER_TIMEDELTA),
                    [&] { return !buf->_buffer.empty() || log_spill_required; });
        } else { // If it is an explicit call, do not sleep
            #if DEBUG_PRINT
            printf("Force spilling logs to the persistent storage at time: %lu\n", get_server_clock());
            #endif
        }

        // Lock the mutex and set the spilling flag
        unique_buffer_lock.lock();
        buf->is_spilling = true;

        uint32_t buffer_size = buf->_buffer.size();
        // we map the transaction ID to log_str one-to-one
        uint64_t txn_id_cache[buffer_size]; // We store the transaction ID
        std::string log_str[buffer_size]; // we store one string per transaction ID
        int cache_iterator = 0;
        for (auto buffer_iterator: buf->_buffer) {
            uint64_t log_txn_id = buffer_iterator.first;
            txn_id_cache[cache_iterator] = log_txn_id;
            std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;
            // Sort on basis of the node id for easier processing of the message later
            std::sort(node_id_message_list.begin(), node_id_message_list.end());
            // Store the largest LSN for a node
            string current_txn_log;
            for (auto node_id_msg_iter: node_id_message_list) {
                // if (node_largest_txn_id_cache[node_id_msg_iter.first] < log_txn_id) {
                //     node_largest_txn_id_cache[node_id_msg_iter.first] = log_txn_id;
                // }
                current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';    
            }
            log_str[cache_iterator] = current_txn_log;
            cache_iterator++;
        }
        for (int ii = 0 ; ii < buffer_size; ii++) {
            buf->_buffer.erase(txn_id_cache[ii]);
        }

        buf->is_spilling = false;
        buf->last_flush_timestamp = get_server_clock();

        #if DEBUG_PRINT
            printf("Logs last flushed timestamp %lu\n", buf->last_flush_timestamp);
        #endif
        // unset the manual spill flag if set
        if (log_spill_required) {
            log_spill_required = false;
        }
        // Unlock the buffer after reading and emptying it. We will keep on spilling async.
        buf->_buffer_signal->notify_one();
        unique_buffer_lock.unlock();

        #if LOG_DEVICE == LOG_DVC_REDIS
        // Log to REDIS and set the LSN in the txn_table for the transactions
        // redis_client->log_sync_data(g_node_id, )
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        // Log to AZURE
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        #endif

        // If this was a force spill, exit after spilling
        if (force_empty) {
            return NULL;
        }
    }
    // TODO Need to update the txn table based on the txn IDs which are committed
    return NULL;
}