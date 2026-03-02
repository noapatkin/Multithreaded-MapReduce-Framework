Multithreaded MapReduce Framework (C++)

Overview

This project implements a multithreaded MapReduce framework in C++, inspired by the MapReduce programming model used in distributed systems.

The framework executes the Map → Shuffle → Reduce pipeline using multiple threads, while providing:

-Thread synchronization

-Barrier coordination

-Atomic state tracking

-Real-time job progress reporting

The implementation focuses on concurrency control, synchronization primitives, and efficient shared-state management.

Architecture

Core Components

1. JobContext

Central structure managing the entire job lifecycle:

-Input vector

-Output vector

-Worker threads

-Intermediate shuffled data

-Atomic job state (stage + progress)

-Synchronization primitives

2. ThreadContext

Represents a worker thread and contains:

-Thread ID

-Private intermediate vector

-Pointer to shared JobContext


Each thread:

-Executes map

-Sorts its intermediate pairs

-Participates in shuffle (via synchronization)

-Executes reduce

3. Barrier (Reusable)
A reusable synchronization barrier implemented using:

-std::mutex

-std::condition_variable

Generation counter (to support multiple barrier cycles) 
Ensures:

-All threads complete Map before Shuffle begins

-All threads wait until Shuffle finishes before Reduce

Execution Flow

Stage 1 – Map

-Threads fetch input items using an atomic counter.

-Each thread emits intermediate key-value pairs.

-Intermediate pairs are locally sorted per thread.

Stage 2 – Shuffle (Single-threaded by main thread)

-Groups identical keys across all intermediate vectors.

-Moves grouped data into shuffled storage.

-Updates progress atomically.

Stage 3 – Reduce

-Threads pull grouped keys using an atomic index.

-Each group is reduced in parallel.

-Final results are emitted into the shared output vector (mutex protected).

Concurrency Design

Atomic 64-bit State Encoding

A single atomic variable encodes:

This allows:

-Lock-free progress tracking

-Accurate percentage reporting

-Atomic stage transitions

-Synchronization Mechanisms

-std::atomic for counters and job state

std::mutex for:

-Output emission

-Shuffle access

-State consistency

Barrier for stage coordination

std::atomic_flag to prevent double join

API Overview

JobHandle startMapReduceJob(
    const MapReduceClient& client,
    const InputVec& inputVec,
    OutputVec& outputVec,
    int multiThreadLevel);

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);

Purpose

Developed as part of a os course to gain practical experience with:

-Parallel algorithm design

-Synchronization patterns

-Atomic operations

-Thread lifecycle management

-Lock-free programming technique
