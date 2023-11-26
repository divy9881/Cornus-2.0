#pragma once

#include "config.h"
#include <queue>
#include "pthread.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <map>

std::atomic_bool log_spill_required;

// Make a singleton class for the log buffer
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

LogBuffer::LogBuffer()
{
    current_buffer_size = 0;
    this->_max_buffer_size = DEFAULT_BUFFER_SIZE;
    this->last_flush_timestamp = 0;
    this->is_spilling = false;
    _buffer_lock = new std::mutex;
    _buffer_signal = new std::condition_variable;
    log_spill_required = false;
    this->size = 0;
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