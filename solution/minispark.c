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
    pthread_mutex_lock(&dep->lock);
    dep->parent = rdd;
    pthread_mutex_unlock(&dep->lock);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->dependencies_met = 0;
  pthread_mutex_init(&rdd->lock, NULL);
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  rdd->parent = NULL;
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
  pthread_mutex_init(&rdd->lock, NULL);
  rdd->dependencies_met = 0;
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
  rdd->numpartitions = numfiles;
  rdd->parent = NULL;
  rdd->ctx = NULL;
  return rdd;
}

WorkQueue init_queue() {
  WorkQueue queue;
  queue.head = NULL;
  queue.tail = NULL;
  queue.size = 0;
  return queue;
}
void free_queue(WorkQueue *queue) {
  assert(queue->head == NULL);
  assert(queue->tail == NULL);
}

void thread_pool_init(int num_threads) {
  pool.numthreads = num_threads;
  pool.threads = malloc(sizeof(pthread_t) * num_threads);
  // pool->shutting_down = false;
  pool.active_tasks = 0;
  pool.queue = init_queue();
  pool.shutdown = 0;
  pthread_cond_init(&pool.not_empty, NULL);
  pthread_mutex_init(&pool.lock, NULL);
  for (int i = 0; i < num_threads; i++) {
    pthread_create(&pool.threads[i], NULL, work_loop, NULL);
  }
}

void thread_pool_destroy() {
  pthread_mutex_lock(&pool.lock);
  pool.shutdown = 1;
  pthread_cond_broadcast(&pool.not_empty);
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
  pthread_cond_destroy(&pool.not_empty);
}

Task pop_task(WorkQueue *queue) {
  if (queue->head == NULL) {
    printf("error popping from empty queue\n");
    exit(1);
  }
  TaskNode *node = queue->head;
  Task task = node->task;
  queue->head = node->next;
  if (queue->head == NULL) {
    queue->tail = NULL;
  }
  queue->size--;
  free(node);
  return task;
}

void insert_node(WorkQueue *queue, TaskNode *node) {
  if (queue->head == NULL) {
    queue->head = node;
    queue->tail = node;
  } else {
    queue->tail->next = node;
    queue->tail = node;
  }
  queue->size++;
}
void submit_task(Task task) {
  TaskNode *node = malloc(sizeof(TaskNode));
  if (node == NULL) {
    printf("error mallocing new task node\n");
    exit(1);
  }
  node->task = task;
  node->next = NULL;
  pthread_mutex_lock(&pool.lock);
  insert_node(&pool.queue, node);
  pthread_cond_signal(&pool.not_empty);
  pthread_mutex_unlock(&pool.lock);
}

void MS_Run() {
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(1);
  }
  int num_cpus = CPU_COUNT(&set);
  thread_pool_init(max(num_cpus - 1, 1));
  return;
}

void MS_TearDown() {
  thread_pool_destroy();
  return;
}
// Caller must hold rdd lock before calling this function
void submit_rdd(RDD *rdd) {
  for (int i = 0; i < rdd->numpartitions; i++) {
    assert(rdd->materializedPartitions[i] == 0);
    assert(rdd->submitted == 0);
    assert(rdd->trans == FILE_BACKED);
    assert(rdd->parent != NULL);  // need parent to traverse up the tree
    Task task;
    task.rdd = rdd;
    task.pnum = i;
    task.metric = NULL;  // TODO
    rdd->submitted = 1;
    submit_task(task);
  }
}
// main thread func
void execute(RDD *rdd) {
  for (int i = 0; i < rdd->numdependencies; i++) {
    RDD *dep = rdd->dependencies[i];
    execute(dep);
  }
  if (rdd->numdependencies == 0) {
    submit_rdd(rdd);
  }
  return;
}
int is_rdd_materialized(RDD *rdd) {
  for (int i = 0; i < rdd->numpartitions; i++) {
    if (rdd->materializedPartitions[i] == 0) {
      return 0;
    }
  }
  return 1;
}
// worker thread func
void execute_task(Task task) {
  materialize_partition(task.rdd, task.pnum);
  RDD *parent = task.rdd->parent;
  if (parent != NULL) {
    if (is_rdd_materialized(task.rdd)) {
      pthread_mutex_lock(&parent->lock);
      parent->dependencies_met++;
      if (parent->dependencies_met == parent->numdependencies) {
        submit_rdd(parent);
      }
      pthread_mutex_unlock(&parent->lock);
    }
  }
}
void *work_loop(void *arg) {
  (void)arg;  // unused
  while (1) {
    pthread_mutex_lock(&pool.lock);
    while (pool.queue.size == 0 && !pool.shutdown) {
      pthread_cond_wait(&pool.not_empty, &pool.lock);
    }
    if (pool.shutdown && pool.queue.size == 0) {
      pthread_mutex_unlock(&pool.lock);
      break;
    }
    Task task = pop_task(&pool.queue);
    pool.active_tasks++;
    pthread_mutex_unlock(&pool.lock);
    // Execute the task
    execute_task(task);
    pthread_mutex_lock(&pool.lock);
    pool.active_tasks--;
    pthread_mutex_unlock(&pool.lock);
  }
  pthread_exit(NULL);
  return NULL;
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
