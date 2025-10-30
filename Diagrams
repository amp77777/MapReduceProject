# MapReduceProject

## Diagram of models used
                 Multithreading Model 

                ┌──────────────────┐
                │    Main Thread   │
                └────────┬─────────┘
                         │
     ┌───────────────────┼────────────────────┐
     │                   │                    │
┌───────────┐       ┌───────────┐        ┌───────────┐
│  Thread 1 │       │  Thread 2 │  ...   │  Thread N │
└──────┬────┘       └──────┬────┘        └──────┬────┘
       │                   │                    │
       └─────────────── Shared Memory ───────────┘
                         (Local IPC)
                                │
                         ┌────────────┐
                         │  Reducer   │
                         └────────────┘


               Multiprocessing Model 

                ┌──────────────────┐
                │   Main Process   │
                └────────┬─────────┘
                         │
     ┌───────────────────┼────────────────────┐
     │                   │                    │
┌────────────┐      ┌────────────┐        ┌────────────┐
│ Process 1  │      │ Process 2  │  ...   │ Process N  │
└──────┬─────┘      └──────┬─────┘        └──────┬─────┘
       │                   │                    │
       └─────────────── IPC Channel ─────────────┘
                    (e.g., Pipes or Queues)
                                │
                         ┌────────────┐
                         │  Reducer   │
                         └────────────┘

My code supports the MapReduce framework by following its core principles of parallel mapping and centralized reduction within a single system. In the Map phase, each thread or process independently handles a portion of the data, either sorting its assigned chunk or finding a local maximum. This mirrors how MapReduce divides tasks across distributed workers. In the Reduce phase, the main thread or reducer process collects and merges all partial outputs, producing a single final result (a fully sorted list or the global maximum). The use of shared memory and synchronization mechanisms (like locks) allows safe communication between workers and the reducer, similar to how MapReduce systems coordinate data exchange and consistency across nodes. 
