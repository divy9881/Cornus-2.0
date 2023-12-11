#pragma once

#ifndef LOG_BUFFER_H_
#define LOG_BUFFER_H_

#include "config.h"
#include <queue>
#include "pthread.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <map>
#include "azure_blob_client.h"
#include "redis_client.h"

extern std::atomic_bool log_spill_required;

struct spiller_args {
    LogBuffer* logger_instance;
    bool force;
};


class LogBuffer
{
public:
    // Buffer - (txn_id1: [<node_id_1>:status:data|<node_id_2>:status:data|<node_id_3>:status:data|..]),
    //          (txn_id2: [<node_id_x>:status:data|<node_id_y>:status:data|<node_id_z>:status:data|,,])
    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> _prepare_buffer;
    std::mutex *_prepare_buffer_lock, *_prepare_flush_lock;
    std::condition_variable *_prepare_buffer_signal, *prepare_buffer_condition;
    std::atomic_bool _prepare_buffer_spilling;
    uint64_t _max_prepare_buffer_size; // Maximum size of records that can be in the buffer at once
    uint64_t _prepare_buf_size; // Current count of total logs added

    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> _commit_buffer;
    std::mutex *_commit_buffer_lock, *_commit_flush_lock;
    std::condition_variable *_commit_buffer_signal, *commit_buffer_condition;
    std::atomic_bool _commit_buffer_spilling;
    uint64_t _max_commit_buffer_size; // Maximum size of records that can be in the buffer at once
    uint64_t _commit_buf_size; // CUrrent count of total logs added
    
    static LogBuffer *logBufferInstance;
    bool prepare_flush_thread_running = false;
    bool commit_flush_thread_running = false;

public:
    uint64_t last_prepare_flush_timestamp;
    uint64_t last_commit_flush_timestamp;

    LogBuffer();
    ~LogBuffer();

    // LogBuffer(const LogBuffer& obj) = delete;
    static LogBuffer *getBufferInstance();
    int add_prepare_log(uint64_t node_id, uint64_t txn_id, int status, std::string data);
    int add_commit_log(uint64_t node_id, uint64_t txn_id, int status, std::string data);
    void print();
    void flush_prepare_logs();
    void flush_commit_logs();
    void start_prepare_flush_thread();
    void start_commit_flush_thread();
    void stop_prepare_flush_thread();
    void stop_commit_flush_thread();
};

// void * flush_prepare_logs(void* args);


#endif // LOG_BUFFER_H_
