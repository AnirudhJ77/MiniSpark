#define _GNU_SOURCE

#include "minispark.h"

#include <assert.h>
#include <sched.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile. 
void print_formatted_metric(TaskMetric* metric, FILE* fp) {
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
	  metric->rdd, metric->pnum, metric->rdd->trans,
	  metric->created.tv_sec, metric->created.tv_nsec / 1000,
	  metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
	  metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

// Thread pool
ThreadPool pool;

/// List functions
List *list_init(int capacity) {
  if (capacity <= 0) {
    printf("error creating list with capacity %d\n", capacity);
    exit(1);
  }
  List *list = malloc(sizeof(List));
  if (list == NULL) {
    printf("error mallocing new list\n");
    exit(1);
  }
  list->items = malloc(sizeof(void *) * capacity);
  if (list->items == NULL) {
    printf("error mallocing new list items\n");
    exit(1);
  }
  list->size = 0;
  list->capacity = capacity;
  list->index = 0;
  pthread_mutex_init(&list->lock, NULL);
  return list;
}
void list_append(List *list, void *elem) {
  pthread_mutex_lock(&list->lock);
  if (list->size == list->capacity) {
    list->capacity *= 2;
    list->items = realloc(list->items, sizeof(void *) * list->capacity);
    if (list->items == NULL) {
      printf("error reallocing new list items\n");
      exit(1);
    }
  }
  list->items[list->size++] = elem;
  pthread_mutex_unlock(&list->lock);
}

void *list_get(List *list, int index) {
  if (index < 0 || index >= list->size) {
    printf("error getting item %d from list of size %d\n", index, list->size);
    exit(1);
  }
  return list->items[index];
}

void list_free(List *list) {
  for (int i = 0; i < list->size; i++) {
    free(list->items[i]);
  }
  free(list->items);
  free(list);
}
void *list_next(List *list) {
  if (list->index >= list->size) {
    return NULL;
  }
  void *item = list->items[list->index++];
  return item;
}
void list_seek_to_start(List *list) { list->index = 0; }

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  assert(numdeps > 0);
  assert(numdeps <= MAXDEPS);
  assert(t != FILE_BACKED);
  assert(fn != NULL);
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  if (t != PARTITIONBY) {
    rdd->numpartitions = rdd->dependencies[0]->numpartitions;
    rdd->partitions = list_init(rdd->numpartitions);
    rdd->materializedPartitions = calloc(rdd->numpartitions, sizeof(int));
  }
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions);
  rdd->numpartitions = numpartitions;
  rdd->materializedPartitions = calloc(numpartitions, sizeof(int));
  rdd->ctx = ctx;
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);
  rdd->materializedPartitions = malloc(numfiles * sizeof(int));
  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(1);
    }
    list_append(rdd->partitions, fp);
    rdd->materializedPartitions[i] = 1;
  }

  rdd->numdependencies = 0;
  rdd->trans = FILE_BACKED;
  rdd->fn = (void *)identity;
  return rdd;
}

void execute(RDD* rdd) {
  
  return;
}

WorkQueue init_queue(int size) {
  if (size <= 0) {
    printf("error creating queue with size %d\n", size);
    exit(1);
  }
  WorkQueue queue;
  queue.capacity = size;
  queue.head = 0;
  queue.tail = 0;
  queue.size = 0;
  queue.buffer = malloc(sizeof(Task) * size);
  if (queue.buffer == NULL) {
    printf("error mallocing new queue buffer\n");
    exit(1);
  }
  return queue;
}
void free_queue(WorkQueue *queue) {
  assert(queue->head == queue->tail);
  free(queue->buffer);
}

void thread_pool_init(int num_threads) {
  pool.numthreads = num_threads;
  pool.threads = malloc(sizeof(pthread_t) * num_threads);
  // pool->shutting_down = false;
  pool.active_tasks = 0;
  pool.queue = init_queue(QUEUE_CAPACITY);
  pool.shutdown = 0;
  sem_init(&pool.queue_empty, 0, pool.queue.capacity);
  pthread_cond_init(&pool.queue_full, NULL);
  pthread_mutex_init(&pool.lock, NULL);
  for (int i = 0; i < num_threads; i++) {
    pthread_create(&pool.threads[i], NULL, work_loop, NULL);
  }
}

void thread_pool_destroy() {
  pthread_mutex_lock(&pool.lock);
  pool.shutdown = 1;
  pthread_cond_broadcast(&pool.queue_full);
  pthread_mutex_unlock(&pool.lock);
  printf("Broadcasted shutdown signal to threads\n");
  for (int i = 0; i < pool.numthreads; i++) {
    pthread_join(pool.threads[i], NULL);
  }
  assert(pool.active_tasks == 0);
  assert(pool.queue.size == 0);
  free(pool.threads);
  free_queue(&pool.queue);
  pthread_mutex_destroy(&pool.lock);
  sem_destroy(&pool.queue_empty);
  pthread_cond_destroy(&pool.queue_full);
}
void submit_task(Task task) {
  sem_wait(&pool.queue_empty);
  pthread_mutex_lock(&pool.lock);
  pool.queue.buffer[pool.queue.tail] = task;
  pool.queue.tail = (pool.queue.tail + 1) % pool.queue.capacity;
  pool.queue.size++;
  pool.active_tasks++;
  pthread_cond_signal(&pool.queue_full);
  pthread_mutex_unlock(&pool.lock);
}
void *work_loop(void *arg) {
  (void)arg;  // unused
  while (1) {
    pthread_mutex_lock(&pool.lock);
    while (pool.queue.size == 0 && !pool.shutdown) {
      pthread_cond_wait(&pool.queue_full, &pool.lock);
    }
    if (pool.shutdown && pool.queue.size == 0) {
      pthread_mutex_unlock(&pool.lock);
      break;
    }
    Task task = pool.queue.buffer[pool.queue.head];
    pool.queue.head = (pool.queue.head + 1) % pool.queue.capacity;
    pool.queue.size--;
    pthread_mutex_unlock(&pool.lock);
    sem_post(&pool.queue_empty);
    // Execute the task
    execute(task.rdd);
    pthread_mutex_lock(&pool.lock);
    pool.active_tasks--;
    pthread_mutex_unlock(&pool.lock);
  }
  pthread_exit(NULL);
  return NULL;
}

void MS_Run() {
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    return -1;
  }
  return;
}

void MS_TearDown() {
  return;
}

int count(RDD *rdd) {
  execute(rdd);

  int count = 0;
  // count all the items in rdd
  return count;
}

void print(RDD *rdd, Printer p) {
  execute(rdd);

  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
}
