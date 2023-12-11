#include <queue>
#include <string>
#include <algorithm>
#include <iostream>

#include "log_buffer.h"

extern RedisClient*     redis_client;
extern AzureBlobClient* azure_blob_client;
extern LogBuffer*       LOGGER;

LogBuffer::LogBuffer() {
    // Initialize prepare buffer
    this->_max_prepare_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_prepare_flush_timestamp = 0;
    this->_prepare_buffer_spilling = false;
    pthread_mutex_init(&this->_prepare_buffer_lock, nullptr);
    pthread_mutex_init(&this->_prepare_flush_lock, nullptr);
    pthread_cond_init(&this->_prepare_buffer_signal, nullptr);
    pthread_cond_init(&this->prepare_buffer_condition, nullptr);
    log_spill_required = false;
    this->_prepare_buf_size = 0;

    // Initialize commit buffer
    this->_max_commit_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_commit_flush_timestamp = 0;
    this->_commit_buffer_spilling = false;
    pthread_mutex_init(&this->_commit_buffer_lock, nullptr);
    pthread_mutex_init(&this->_commit_flush_lock, nullptr);
    pthread_cond_init(&this->_commit_buffer_signal, nullptr);
    pthread_cond_init(&this->commit_buffer_condition, nullptr);
    this->_commit_buf_size = 0;
}

LogBuffer::~LogBuffer() {
    pthread_mutex_destroy(&this->_prepare_buffer_lock);
    pthread_mutex_destroy(&this->_prepare_flush_lock);
    pthread_cond_destroy(&this->_prepare_buffer_signal);
    pthread_cond_destroy(&this->prepare_buffer_condition);

    pthread_mutex_destroy(&this->_commit_buffer_lock);
    pthread_mutex_destroy(&this->_commit_flush_lock);
    pthread_cond_destroy(&this->_commit_buffer_signal);
    pthread_cond_destroy(&this->commit_buffer_condition);
}

void LogBuffer::print() {
    printf("The PREPARE buffer has %ld transactions and %ld logs\n", this->_prepare_buffer.size(), this->_prepare_buf_size);
    for (auto a : this->_prepare_buffer) {
        std::cout << a.first << " ";
        for (auto iter : a.second) {
            std::cout << iter.first << ":" << iter.second << "|";
        }
        std::cout << std::endl;
    }

    printf("The COMMIT buffer has %ld transactions and %ld logs\n", this->_commit_buffer.size(), this->_commit_buf_size);
    for (auto a : this->_commit_buffer) {
        std::cout << a.first << " ";
        for (auto iter : a.second) {
            std::cout << iter.first << ":" << iter.second << "|";
        }
        std::cout << std::endl;
    }
}

int LogBuffer::add_prepare_log(uint64_t node_id, uint64_t txn_id, int status, std::string data) {
    pthread_mutex_lock(&_prepare_buffer_lock);
    int ret = 0;

    while (_prepare_buffer_spilling.load()) {
        pthread_cond_wait(&_prepare_buffer_signal, &_prepare_buffer_lock);
    }

    if (this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
        if (this->_prepare_buffer.find(txn_id) == this->_prepare_buffer.end()) {
            pthread_mutex_lock(&_prepare_flush_lock);
            if (!this->_prepare_buffer_spilling.load() && this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
                this->_prepare_buffer_spilling.store(true);
                this->start_prepare_flush_thread();
            }
            pthread_mutex_unlock(&this->_prepare_flush_lock);
        }
    }

    std::string status_data = "E"; // Empty status
    if (status != -1) {
        status_data = std::to_string(status);
    }
    status_data += data;

    this->_prepare_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_prepare_buf_size++;
    pthread_mutex_unlock(&this->_prepare_buffer_lock);
    return ret;
}

int LogBuffer::add_commit_log(uint64_t node_id, uint64_t txn_id, int status, std::string data) {
    pthread_mutex_lock(&_commit_buffer_lock);
    int ret = 0;

    while (this->_commit_buffer_spilling.load()) {
        pthread_cond_wait(&_commit_buffer_signal, &_commit_buffer_lock);
    }

    if (this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
        if (this->_commit_buffer.find(txn_id) == this->_commit_buffer.end()) {
            pthread_mutex_lock(&_commit_flush_lock);
            if (!this->_commit_buffer_spilling.load() && this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
                this->_commit_buffer_spilling.store(true);
                this->start_commit_flush_thread();
            }
            pthread_mutex_unlock(&this->_commit_flush_lock);
        }
    }

    std::string status_data = "E"; // Empty status
    if (status != -1) {
        status_data = std::to_string(status);
    }
    status_data += data;

    this->_commit_buffer[txn_id].push_back(std::make_pair(node_id, status_data));
    this->_commit_buf_size++;
    pthread_mutex_unlock(&_commit_buffer_lock);
    return ret;
}

void LogBuffer::flush_prepare_logs(void) {
    pthread_mutex_lock(&_prepare_buffer_lock);

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += EMPTY_LOG_BUFFER_TIMEDELTA / 1000;  // Convert milliseconds to seconds
    ts.tv_nsec += (EMPTY_LOG_BUFFER_TIMEDELTA % 1000) * 1000000;  // Convert remainder to nanoseconds

    if (pthread_cond_timedwait(&_prepare_buffer_signal, &_prepare_buffer_lock, &ts) == ETIMEDOUT) {
        return !this->prepare_flush_thread_running || !this->_prepare_buffer.empty();
    }

    if (!this->_prepare_buffer.empty()) {
        pthread_mutex_lock(&_prepare_flush_lock);
        std::vector<uint64_t> txn_id_cache;
        std::vector<std::string> txn_log_str;
        uint64_t largest_txn_id = 0;
        std::map<uint64_t, uint64_t> node_largest_lsn;

        if (!this->_prepare_buffer_spilling.load() && this->_prepare_buf_size >= LOG_BUFFER_HW_CAPACITY) {
            this->_prepare_buffer_spilling.store(true);

            for (auto buffer_iterator : this->_prepare_buffer) {
                uint64_t log_txn_id = buffer_iterator.first;
                std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;

                std::string current_txn_log;
                for (auto node_id_msg_iter : node_id_message_list) {
                    if (largest_txn_id < log_txn_id) {
                        largest_txn_id = log_txn_id;
                    }
                    if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id) {
                        node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                    }
                    current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                }
                txn_id_cache.push_back(log_txn_id);
                txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
            }

            for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++) {
                this->_prepare_buffer.erase(txn_id_cache[ii]);
            }

            this->_prepare_buf_size = 0;
            this->_prepare_buffer_spilling.store(false);
            this->last_prepare_flush_timestamp = get_server_clock();
            this->_prepare_buffer_signal.notify_all();
        }
        pthread_mutex_unlock(&this->_prepare_flush_lock);

        for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++) {
            uint64_t txn_id = txn_id_cache[ii];
            std::string txn_logs = txn_log_str[ii];

            #if LOG_DEVICE == LOG_DVC_REDIS
                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #endif
        }
    }
    pthread_mutex_unlock(&_prepare_buffer_lock);
}

void LogBuffer::flush_commit_logs() {
    pthread_mutex_lock(&_commit_buffer_lock);

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += EMPTY_LOG_BUFFER_TIMEDELTA / 1000;  // Convert milliseconds to seconds
    ts.tv_nsec += (EMPTY_LOG_BUFFER_TIMEDELTA % 1000) * 1000000;  // Convert remainder to nanoseconds

    if (pthread_cond_timedwait(&_commit_buffer_signal, &_commit_buffer_lock, &ts) == ETIMEDOUT) {
        return !this->commit_flush_thread_running || !this->_commit_buffer.empty();
    }
    if (!this->_commit_buffer.empty()) {
        pthread_mutex_lock(&_commit_flush_lock);
        std::vector<uint64_t> txn_id_cache;
        std::vector<std::string> txn_log_str;
        uint64_t largest_txn_id = 0;
        std::map<uint64_t, uint64_t> node_largest_lsn;
        if (!this->_commit_buffer_spilling.load() && this->_commit_buf_size >= LOG_BUFFER_HW_CAPACITY) {
            this->_commit_buffer_spilling.store(true);

            for (auto buffer_iterator : this->_commit_buffer) {
                uint64_t log_txn_id = buffer_iterator.first;
                std::vector<std::pair<uint64_t, std::string>> node_id_message_list = buffer_iterator.second;

                std::string current_txn_log;
                for (auto node_id_msg_iter : node_id_message_list) {
                    if (largest_txn_id < log_txn_id) {
                        largest_txn_id = log_txn_id;
                    }
                    if (node_largest_lsn[node_id_msg_iter.first] < log_txn_id) {
                        node_largest_lsn[node_id_msg_iter.first] = log_txn_id;
                    }
                    current_txn_log += std::to_string(node_id_msg_iter.first) + ':' + node_id_msg_iter.second + '|';
                }
                txn_id_cache.push_back(log_txn_id);
                txn_log_str.push_back(current_txn_log.substr(0, current_txn_log.size() - 1) + ";");
            }

            for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++) {
                this->_commit_buffer.erase(txn_id_cache[ii]);
            }

            this->_commit_buf_size = 0;
            this->_commit_buffer_spilling.store(false);
            this->last_commit_flush_timestamp = get_server_clock();
            this->_commit_buffer_signal.notify_all();
        }
        pthread_mutex_unlock(&this->_commit_flush_lock);

        for (uint32_t ii = 0; ii < txn_id_cache.size(); ii++) {
            uint64_t txn_id = txn_id_cache[ii];
            std::string txn_logs = txn_log_str[ii];

            #if LOG_DEVICE == LOG_DVC_REDIS
                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
                azure_blob_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
                redis_client->log_async_data(txn_id, largest_txn_id, txn_logs);
            #endif
        }
    }
    pthread_mutex_unlock(&_commit_buffer_lock);
}

void LogBuffer::start_prepare_flush_thread() {
    if (!this->prepare_flush_thread_running) {
        this->prepare_flush_thread_running = true;
        pthread_t thread;
        pthread_create(&thread, nullptr, &LogBuffer::flush_prepare_logs, this);
    }
}

void LogBuffer::stop_prepare_flush_thread() {
    this->prepare_flush_thread_running = false;
    pthread_cond_signal(&this->prepare_buffer_condition);
}

void LogBuffer::stop_commit_flush_thread() {
    this->commit_flush_thread_running = false;
    pthread_cond_signal(&this->commit_buffer_condition);
    // this->commit_buffer_condition.notify_one();
}

void LogBuffer::start_commit_flush_thread() {
    if (!this->commit_flush_thread_running) {
        this->commit_flush_thread_running = true;
        pthread_t thread;
        pthread_create(&thread, nullptr, &LogBuffer::flush_commit_logs, this);
    }
}