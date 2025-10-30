# MapReduce Systems Project
# Parallel Sorting and Max-Value Aggregation

# This project compares multi-threading and multi-processing approaches
# for sorting large datasets and finding the maximum value in a list.
# The goal is to understand performance differences and resource usage.

import time, os, random, heapq, psutil
import threading
import multiprocessing as mp

# Utility helper functions

def current_rss():
    """Returns the current process's memory usage (RSS) in bytes."""
    p = psutil.Process(os.getpid())
    return p.memory_info().rss

def human_bytes(n):
    # Converts bytes into human-readable format like KB, MB, or GB
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024.0:
            return f"{n:.2f}{unit}"
        n /= 1024.0
    return f"{n:.2f}TB"

# Helper: evenly split array indices for workers
def chunk_indices(n_total, n_chunks):
    # This divides a list into roughly equal chunks so each thread or process
    # gets about the same number of elements to work on.
    base = n_total // n_chunks
    remainder = n_total % n_chunks
    pairs, start = [], 0
    for i in range(n_chunks):
        extra = 1 if i < remainder else 0
        end = start + base + extra
        pairs.append((start, end))
        start = end
    return pairs

# PART 1: Parallel Sorting (MapReduce Style)
# The sorting section uses both threading and multiprocessing to sort
# parts of a dataset in parallel and then merges the results together.

def threaded_sort_map(data, n_workers):
    n = len(data)
    pairs = chunk_indices(n, n_workers)
    sorted_chunks = [None] * n_workers

    # Inner worker function for each thread — sorts a portion of the list
    def worker(i, s, e):
        arr = data[s:e]
        arr.sort()
        sorted_chunks[i] = (i, arr)

    threads = []
    t0 = time.perf_counter()
    rss_before = current_rss()

    # Start all threads
    for i, (s, e) in enumerate(pairs):
        t = threading.Thread(target=worker, args=(i, s, e))
        t.start()
        threads.append(t)
    # Wait for all threads to finish
    for t in threads:
        t.join()

    t1 = time.perf_counter()
    rss_after = current_rss()

    # Merge the sorted sublists into one big sorted list
    sorted_chunks.sort(key=lambda x: x[0])
    merged = list(heapq.merge(*[c[1] for c in sorted_chunks]))
    return merged, t1 - t0, rss_before, rss_after


def process_sort_worker(args):
    # Helper function for multiprocessing — sorts a given slice of the data
    i, arr = args
    arr.sort()
    return (i, arr)


def multiprocessing_sort_map(data, n_workers):
    # This version uses multiple processes instead of threads.
    # Each process sorts a chunk independently and sends it back to the main process.
    n = len(data)
    pairs = chunk_indices(n, n_workers)
    args = [(i, data[s:e]) for i, (s, e) in enumerate(pairs)]

    t0 = time.perf_counter()
    rss_before = current_rss()

    # Pool automatically handles distributing work among processes
    with mp.Pool(n_workers) as pool:
        results = pool.map(process_sort_worker, args)

    t1 = time.perf_counter()
    rss_after = current_rss()

    # Merge results once all processes are done
    results.sort(key=lambda x: x[0])
    merged = list(heapq.merge(*[r[1] for r in results]))
    return merged, t1 - t0, rss_before, rss_after

# PART 2: Max-Value Aggregation with Constrained Shared Memory
# This section tests how threads and processes find the max value in parallel.
# Each worker finds a local maximum and then updates a shared global max.

def threaded_max_map_reduce(data, n_workers):
    pairs = chunk_indices(len(data), n_workers)
    shared = {'value': float('-inf')}
    lock = threading.Lock()

    # Thread worker: finds the local max and updates the shared value
    def worker(s, e):
        local_max = max(data[s:e]) if e > s else float('-inf')
        with lock:
            if local_max > shared['value']:
                shared['value'] = local_max

    threads = []
    t0 = time.perf_counter()
    rss_before = current_rss()

    # Launch all threads to find local maximums
    for (s, e) in pairs:
        t = threading.Thread(target=worker, args=(s, e))
        t.start()
        threads.append(t)
    # Wait for all to finish
    for t in threads:
        t.join()

    t1 = time.perf_counter()
    rss_after = current_rss()
    return shared['value'], t1 - t0, rss_before, rss_after

# Top-level worker for multiprocessing (must be global for Windows)
def max_worker(local, shared_val, lock):
    # Each process finds its local max and updates the shared value if higher
    local_max = max(local) if local else -2 ** 31
    with lock:
        if local_max > shared_val.value:
            shared_val.value = int(local_max)


def multiprocessing_max_map_reduce(data, n_workers):
    # Same concept as threading but uses separate processes instead
    pairs = chunk_indices(len(data), n_workers)
    shared_val = mp.Value('i', -2 ** 31)
    lock = mp.Lock()

    t0 = time.perf_counter()
    rss_before = current_rss()

    procs = []
    # Start a separate process for each chunk
    for (s, e) in pairs:
        local = data[s:e]
        p = mp.Process(target=max_worker, args=(local, shared_val, lock))
        p.start()
        procs.append(p)
    # Wait for all processes to complete
    for p in procs:
        p.join()

    t1 = time.perf_counter()
    rss_after = current_rss()
    return shared_val.value, t1 - t0, rss_before, rss_after

# EXPERIMENTS
# These functions run the actual tests to compare threading vs. multiprocessing.
# They print results like execution time and memory usage changes.

def run_sort_experiment(sizes=(32, 131072), workers=(1, 2, 4, 8)):
    random.seed(0)
    for n in sizes:
        print(f"\n=== Sorting Experiment | Input Size: {n} ===")
        # Create a list of random numbers to sort
        data = [random.randint(0, 100000) for _ in range(n)]
        for w in workers:
            # Run both threading and multiprocessing versions
            sorted_thread, t_thread, b1, a1 = threaded_sort_map(data, w)
            sorted_proc, t_proc, b2, a2 = multiprocessing_sort_map(data, w)

            # Verify correctness for small datasets
            if n == 32:
                correct = sorted_thread == sorted(data) and sorted_proc == sorted(data)
                print(f"\nWorkers={w} | Correctness: {'PASS' if correct else 'FAIL'}")

            # Print timing and memory results
            print(f"Threads  | Time={t_thread:.4f}s | RSS Δ={human_bytes(a1 - b1)}")
            print(f"Processes| Time={t_proc:.4f}s | RSS Δ={human_bytes(a2 - b2)}")


def run_max_experiment(sizes=(32, 131072), workers=(1, 2, 4, 8)):
    random.seed(1)
    for n in sizes:
        print(f"\n=== Max Aggregation | Input Size: {n} ===")
        # Create random integers (can include negatives)
        data = [random.randint(-1000000, 1000000) for _ in range(n)]
        truth = max(data)  # The correct maximum (for checking accuracy)
        for w in workers:
            # Run both threading and multiprocessing versions
            val_thread, t_thread, b1, a1 = threaded_max_map_reduce(data, w)
            val_proc, t_proc, b2, a2 = multiprocessing_max_map_reduce(data, w)

            # Display results and check correctness
            print(f"\nWorkers={w}:")
            print(f"Threads  | Value={val_thread}, OK={val_thread == truth}, Time={t_thread:.6f}s")
            print(f"Processes| Value={val_proc}, OK={val_proc == truth}, Time={t_proc:.6f}s")



# Main program entry point
if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)  # Required for Windows compatibility
    print("Running MapReduce-style experiments...\n")
    run_sort_experiment()
    run_max_experiment()
    print("\nAll experiments completed.")

