#ifndef __minispark_h__
#define __minispark_h__

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>

#define MAXDEPS (2)
#define TIME_DIFF_MICROS(start, end)          \
  (((end.tv_sec - start.tv_sec) * 1000000L) + \
   ((end.tv_nsec - start.tv_nsec) / 1000L))

#define QUEUE_CAPACITY 100
struct RDD;
struct List;

typedef struct RDD RDD;    // forward decl. of struct RDD
typedef struct List List;  // forward decl. of List.
// Minimally, we assume "list_add_elem(List *l, void*)"

// Different function pointer types used by minispark
typedef void* (*Mapper)(void* arg);
typedef int (*Filter)(void* arg, void* pred);
typedef void* (*Joiner)(void* arg1, void* arg2, void* arg);
typedef unsigned long (*Partitioner)(void* arg, int numpartitions, void* ctx);
typedef void (*Printer)(void* arg);

typedef enum { MAP, FILTER, JOIN, PARTITIONBY, FILE_BACKED } Transform;

struct RDD {
  Transform trans;   // transform type, see enum
  void* fn;          // transformation function
  void* ctx;         // used by minispark lib functions
  List* partitions;  // list of partitions

  RDD* dependencies[MAXDEPS];
  int numdependencies;  // 0, 1, or 2
  int numpartitions;    // number of partitions in this RDD

  // you may want extra data members here
  RDD* parent;  // parent RDD, if any

  // changeable state
  pthread_mutex_t lock;         // lock for the state variables
  int* materializedPartitions;  // 0 if not materialized, 1 if materialized
  int dependencies_met;
  int submitted;  // 0 if not submitted, 1 if submitted
  pthread_cond_t* done;  // only used for root rdd
};
struct List {
  void** items;
  int size;
  int capacity;
  int index;
  pthread_mutex_t lock;
};

List* list_init(int capacity);
void list_append(List* list, void* elem);
void* list_get(List* list, int index);
void list_free(List* list);
void* list_next(List* list);
void list_seek_to_start(List* list);
void free_dag(RDD* rdd);
void free_rdd(RDD* rdd);

typedef struct {
  struct timespec created;
  struct timespec scheduled;
  size_t duration;  // in usec
  RDD* rdd;
  int pnum;
} TaskMetric;

typedef struct {
  RDD* rdd;
  int pnum;
  TaskMetric* metric;
} Task;

struct taskNode {
  Task task;
  struct taskNode* next;
};
typedef struct taskNode TaskNode;

typedef struct {
  TaskNode* head;  // dequeue index
  TaskNode* tail;  // enqueue index
  int size;      // number of tasks in the queue
} WorkQueue;

typedef struct {
  TaskMetric queue[QUEUE_CAPACITY];
  int head;                  // dequeue index
  int tail;                  // enqueue index
  int size;                  // number of tasks in the queue
  pthread_mutex_t lock;      // mutex for the queue
  pthread_cond_t not_empty;  // condition variable for not empty
  FILE* logfile;             // log file for metrics
} MetricQueue;

typedef struct {
  pthread_t* threads;  // array of worker threads
  int numthreads;      // number of worker threads
  pthread_t monitor;   // monitoring thread
  WorkQueue queue;     // work queue for tasks
  int active_tasks;
  int shutdown;               // flag to indicate shutdown
  pthread_mutex_t lock;       // mutex for thread pool
  pthread_cond_t not_empty;   // available tasks
} ThreadPool;

extern ThreadPool pool;

void thread_pool_init(int num_threads);
void thread_pool_destroy();
void* work_loop(void*);
void* monitor_loop(void*);
void submit_task(Task task);
//////// actions ////////

// Return the total number of elements in "dataset"
int count(RDD* dataset);

// Print each element in "dataset" using "p".
// For example, p(element) for all elements.
void print(RDD* dataset, Printer p);

//////// transformations ////////

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation.
RDD* map(RDD* rdd, Mapper fn);

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation. "ctx" should be passed to "fn"
// when it is called as a Filter
RDD* filter(RDD* rdd, Filter fn, void* ctx);

// Create an RDD with two dependencies, "rdd1" and "rdd2"
// "ctx" should be passed to "fn" when it is called as a
// Joiner.
RDD* join(RDD* rdd1, RDD* rdd2, Joiner fn, void* ctx);

// Create an RDD with "rdd" as a dependency. The new RDD
// will have "numpartitions" number of partitions, which
// may be different than its dependency. "ctx" should be
// passed to "fn" when it is called as a Partitioner.
RDD* partitionBy(RDD* rdd, Partitioner fn, int numpartitions, void* ctx);

// Create an RDD which opens a list of files, one per
// partition. The number of partitions in the RDD will be
// equivalent to "numfiles."
RDD* RDDFromFiles(char* filenames[], int numfiles);

//////// MiniSpark ////////
// Submits work to the thread pool to materialize "rdd".
void execute(RDD* rdd);

// Creates the thread pool and monitoring thread.
void MS_Run();

// Waits for work to be complete, destroys the thread pool, and frees
// all RDDs allocated during runtime.
void MS_TearDown();


#endif // __minispark_h__
