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
    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> _buffer;
    std::mutex *_buffer_lock;
    std::condition_variable *_buffer_signal;
    std::atomic_bool is_spilling;
    int _max_buffer_size; // Maximum size of records that can be in the buffer at once
    int current_buffer_size;
    uint64_t size; // Count of total logs added
    static LogBuffer *logBufferInstance;

public:
    uint64_t last_flush_timestamp;

    LogBuffer();
    ~LogBuffer();

    // LogBuffer(const LogBuffer& obj) = delete;
    static LogBuffer *getBufferInstance();
    int add_log(uint64_t node_id, uint64_t txn_id, int status, std::string data);
    void print();
};

void * spill_buffered_logs_to_storage(void* args);


#endif // LOG_BUFFER_H_
