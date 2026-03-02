
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>
#include <algorithm>
#include <cstdio>
#include <thread>
#include <mutex>

#define MAIN_THREAD_ID 0

#define RAISE_ERR(action, msg) \
    try { \
        action; \
    } catch (...) { \
        fprintf(stdout, "system error: %s\n", msg); \
        exit(1); \
    }

// Bit manipulation for state atomic
#define STAGE_BITS 2
#define COUNTER_BITS 31

#define STAGE_MASK (((uint64_t)0x3) << 62)
#define PROCESSED_MASK (((uint64_t)0x7FFFFFFF) << 31)
#define TOTAL_MASK ((uint64_t)0x7FFFFFFF)

#define GET_STAGE(state) ((stage_t)((state & STAGE_MASK) >> 62))
#define GET_PROCESSED(state) ((state & PROCESSED_MASK) >> 31)
#define GET_TOTAL(state) (state & TOTAL_MASK)

#define SET_STAGE(state, stage) ((state & ~STAGE_MASK) | ((uint64_t)(stage) << 62))
#define SET_PROCESSED(state, processed) ((state & ~PROCESSED_MASK) | ((uint64_t)(processed) << 31))
#define SET_TOTAL(state, total) ((state & ~TOTAL_MASK) | (total))

typedef struct JobContext JobContext;
void *start_routine(void *arg);


struct ThreadContext {
    int _id;
    std::thread _thread;
    JobContext* _job_context;
    IntermediateVec _intermediatePairs;

    ThreadContext(int id, JobContext* jobContext) :
        _id(id), _job_context(jobContext) {}
};

struct JobContext {
    const MapReduceClient& _client;
    const InputVec& _inputVector;
    OutputVec& _outputVector;
    int _multiThreadLevel;
    std::vector<ThreadContext> _threads;
    std::vector<IntermediateVec> _shuffled;
    std::mutex _emit3_mutex;
    std::mutex _shuffle_mutex;
    std::mutex _state_mutex;
    Barrier _barrier;
    
    // Single 64-bit atomic state variable:
    // [2 bits for stage][31 bits for processed items][31 bits for total items]
    std::atomic<uint64_t> _state_atomic;
    
    std::atomic<size_t> _input_counter;
    std::atomic<size_t> _reduce_counter;
    std::atomic_flag _joined = ATOMIC_FLAG_INIT;

    JobContext(const MapReduceClient& client, const InputVec& inputVec, 
        OutputVec& outputVec, int multiThreadLevel): 
        _client(client), _inputVector(inputVec), _outputVector(outputVec), 
        _multiThreadLevel(multiThreadLevel), _threads(), _shuffled(), _barrier(multiThreadLevel), 
        _state_atomic(0), _input_counter(0), _reduce_counter(0) {
              // Initialize state with UNDEFINED stage and input vector size as total
              uint64_t initial_state = 0;
              initial_state = SET_STAGE(initial_state, UNDEFINED_STAGE);
              initial_state = SET_PROCESSED(initial_state, 0);
              initial_state = SET_TOTAL(initial_state, inputVec.size());
              _state_atomic.store(initial_state);
    }
};

bool compare_pairs(const IntermediatePair& a, const IntermediatePair& b) {
    return *a.first < *b.first;
}

void emit2(K2* key, V2* value, void* context) {
    auto* tc = static_cast<ThreadContext*>(context);
    RAISE_ERR(tc->_intermediatePairs.emplace_back(key, value), "emplace_back");
}

void emit3(K3* key, V3* value, void* context) {
    auto* tc = static_cast<ThreadContext*>(context);
    auto* jc = tc->_job_context;
    
    std::lock_guard<std::mutex> lock(jc->_emit3_mutex);
    RAISE_ERR(jc->_outputVector.emplace_back(key, value), "emplace_back");
}

// Atomically update the processed counter in the state
void update_processed_counter(JobContext* jc, uint64_t add_value) {
    uint64_t old_state, new_state;
    do {
        old_state = jc->_state_atomic.load();
        uint64_t processed = GET_PROCESSED(old_state);
        uint64_t new_processed = processed + add_value;
        
        // Ensure we don't exceed the total
        uint64_t total = GET_TOTAL(old_state);
        if (new_processed > total) {
            new_processed = total;
        }
        
        new_state = SET_PROCESSED(old_state, new_processed);
    } while (!jc->_state_atomic.compare_exchange_weak(old_state, new_state));
}

// Update stage in the state atomic
void update_stage(JobContext* jc, uint64_t new_stage) {
    uint64_t old_state, new_state;
    do {
        old_state = jc->_state_atomic.load();
        new_state = SET_STAGE(old_state, new_stage);
        
        // When changing stage, reset processed counter
        new_state = SET_PROCESSED(new_state, 0);
    } while (!jc->_state_atomic.compare_exchange_weak(old_state, new_state));
}

// Update total in the state atomic
void update_total(JobContext* jc, uint64_t new_total) {
    uint64_t old_state, new_state;
    do {
        old_state = jc->_state_atomic.load();
        new_state = SET_TOTAL(old_state, new_total);
    } while (!jc->_state_atomic.compare_exchange_weak(old_state, new_state));
}

void map_phase(ThreadContext* tc) {
    JobContext* jc = tc->_job_context;
    size_t index;
    while ((index = jc->_input_counter++) < jc->_inputVector.size()) {
        InputPair input = jc->_inputVector[index];
        jc->_client.map(input.first, input.second, tc);
        update_processed_counter(jc, 1);
    }
    std::sort(tc->_intermediatePairs.begin(), tc->_intermediatePairs.end(), compare_pairs);
}

void shuffle_phase(JobContext* jc) {
    update_stage(jc, SHUFFLE_STAGE);
    
    // Count total intermediate pairs
    size_t total = 0;
    for (auto& tc : jc->_threads) {
        total += tc._intermediatePairs.size();
    }
    
    // Update total in atomic state
    update_total(jc, total);

    size_t processed = 0;
    while (processed < total) {
        K2* max_key = nullptr;
        for (auto& tc : jc->_threads) {
            if (!tc._intermediatePairs.empty()) {
                K2* current = tc._intermediatePairs.back().first;
                if (!max_key || *max_key < *current) {
                    max_key = current;
                }
            }
        }
        IntermediateVec group;
        for (auto& tc : jc->_threads) {
            while (!tc._intermediatePairs.empty() && !(*tc._intermediatePairs.back().first < *max_key) 
                && !(*max_key < *tc._intermediatePairs.back().first)) {
                group.push_back(tc._intermediatePairs.back());
                tc._intermediatePairs.pop_back();
                processed++;
                update_processed_counter(jc, 1);
            }
        }
        jc->_shuffled.push_back(group);
    }
}

void reduce_phase(ThreadContext* tc) {
    JobContext* jc = tc->_job_context;
    while (true) {
        size_t index;
        {
            std::lock_guard<std::mutex> lock(jc->_shuffle_mutex);
            if (jc->_reduce_counter >= jc->_shuffled.size()) {
                break;
            }
            index = jc->_reduce_counter++;
        }
        
        IntermediateVec vec = jc->_shuffled[index];
        jc->_client.reduce(&vec, tc);
        update_processed_counter(jc, vec.size());
    }
}

void* start_routine(void* arg) {
    auto* tc = static_cast<ThreadContext*>(arg);
    JobContext* jc = tc->_job_context;
    
    // Set stage to MAP
    if (tc->_id == MAIN_THREAD_ID) {
        update_stage(jc, MAP_STAGE);
    }
    
    map_phase(tc);
    jc->_barrier.barrier();
    
    if (tc->_id == MAIN_THREAD_ID) {
        shuffle_phase(jc);
        update_stage(jc, REDUCE_STAGE);
    }
    
    jc->_barrier.barrier();
    reduce_phase(tc);
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, 
    OutputVec& outputVec, int multiThreadLevel) {
    auto* jc = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        RAISE_ERR(jc->_threads.emplace_back(i, jc), "emplace_back");
    }
    for (auto& tc : jc->_threads) {
        RAISE_ERR(tc._thread = std::thread(start_routine, &tc), "thread creation failed");
    }
    return jc;
}

void waitForJob(JobHandle job) {
    auto* jc = static_cast<JobContext*>(job);
    if (jc->_joined.test_and_set()) return;
    for (auto& tc : jc->_threads) {
        if (tc._thread.joinable()) {
            RAISE_ERR(tc._thread.join(), "thread join failed");
        }
    }
}

void getJobState(JobHandle job, JobState* state) {
    auto* jc = static_cast<JobContext*>(job);
    
    std::lock_guard<std::mutex> lock(jc->_state_mutex);
    
    // Extract state information from atomic variable
    uint64_t atomic_state = jc->_state_atomic.load();
    
    // Convert internal bit stage to JobState stage enum
    uint64_t bit_stage = GET_STAGE(atomic_state);
    switch (bit_stage) {
        case UNDEFINED_STAGE: state->stage = UNDEFINED_STAGE; break;
        case MAP_STAGE: state->stage = MAP_STAGE; break;
        case SHUFFLE_STAGE: state->stage = SHUFFLE_STAGE; break;
        case REDUCE_STAGE: state->stage = REDUCE_STAGE; break;
    }
    
    // Calculate percentage
    uint64_t processed = GET_PROCESSED(atomic_state);
    uint64_t total = GET_TOTAL(atomic_state);
    
    if (total == 0) {
        state->percentage = 0.0;
    } else {
        state->percentage = 100.0 * static_cast<float>(processed) / static_cast<float>(total);
    }
    
    // Ensure percentage doesn't exceed 100%
    if (state->percentage > 100.0) {
        state->percentage = 100.0;
    }
}

void closeJobHandle(JobHandle job) {
    auto* jc = static_cast<JobContext*>(job);
    waitForJob(job);
    delete jc;
}