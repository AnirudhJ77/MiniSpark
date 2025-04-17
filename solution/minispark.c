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
void print_enum(Transform t) {
  switch (t) {
    case FILE_BACKED:
      printf("FILE_BACKED\n");
      break;
    case MAP:
      printf("MAP\n");
      break;
    case FILTER:
      printf("FILTER\n");
      break;
    case JOIN:
      printf("JOIN\n");
      break;
    default:
      printf("UNKNOWN\n");
  }
}
int max(int a, int b)
{
  return a > b ? a : b;
}

// Thread pool
ThreadPool pool;
MetricQueue metrics;
RDD *root = NULL;

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
  // we assume individual items are freed externally
  free(list->items);
  pthread_mutex_destroy(&list->lock);
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

  rdd->numdependencies = numdeps;

  rdd->dependencies_met = 0;
  rdd->submitted = 0;
  pthread_mutex_init(&rdd->lock, NULL);

  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    if (dep->trans == FILE_BACKED) {
      rdd->dependencies_met++;
    }
    pthread_mutex_lock(&dep->lock);
    dep->parent = rdd;
    pthread_mutex_unlock(&dep->lock);
  }
  va_end(args);

  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  rdd->parent = NULL;
  rdd->ctx = NULL;
  rdd->done = NULL;
  if (t != PARTITIONBY) {
    rdd->numpartitions = rdd->dependencies[0]->numpartitions;
    rdd->partitions = list_init(rdd->numpartitions);
    rdd->materializedPartitions = calloc(rdd->numpartitions, sizeof(int));
    for (int i = 0; i < rdd->numpartitions; i++) {
      List *partition = list_init(10);
      list_append(rdd->partitions, partition);
    }
  }
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn) { return create_rdd(1, MAP, fn, dep); }

RDD *filter(RDD *dep, Filter fn, void *ctx) {
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx) {
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->partitions = list_init(numpartitions);
  for (int i = 0; i < numpartitions; i++) {
    List *partition = list_init(10);
    list_append(rdd->partitions, partition);
  }
  rdd->numpartitions = numpartitions;
  rdd->materializedPartitions = calloc(numpartitions, sizeof(int));
  rdd->ctx = ctx;
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx) {
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg) { return arg; }

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles) {
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);
  rdd->materializedPartitions = malloc(numfiles * sizeof(int));
  pthread_mutex_init(&rdd->lock, NULL);
  rdd->dependencies_met = 0;
  for (int i = 0; i < numfiles; i++) {
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
  rdd->submitted = 1;
  rdd->fn = (void *)identity;
  rdd->numpartitions = numfiles;
  rdd->parent = NULL;
  rdd->ctx = NULL;
  rdd->done = NULL;
  return rdd;
}

void free_rdd(RDD *rdd) {
  if (rdd->trans == FILE_BACKED) {
    for (int i = 0; i < rdd->numpartitions; i++) {
      FILE *fp = (FILE *)list_get(rdd->partitions, i);
      fclose(fp);
    }
  } else {
    for (int i = 0; i < rdd->numpartitions; i++) {
      List *partition = list_get(rdd->partitions, i);
      list_free(partition);
    }
  }
  list_free(rdd->partitions);
  free(rdd->materializedPartitions);
  pthread_mutex_destroy(&rdd->lock);

  free(rdd);
  return;
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
  pthread_create(&pool.monitor, NULL, monitor_loop, NULL);
}

void thread_pool_destroy() {
  pthread_mutex_lock(&pool.lock);
  pool.shutdown = 1;
  pthread_cond_broadcast(&pool.not_empty);
  pthread_mutex_unlock(&pool.lock);
  pthread_mutex_lock(&metrics.lock);
  pthread_cond_broadcast(&metrics.not_empty);
  pthread_mutex_unlock(&metrics.lock);
  // printf("Broadcasted shutdown signal to threads\n");
  for (int i = 0; i < pool.numthreads; i++) {
    pthread_join(pool.threads[i], NULL);
  }
  pthread_join(pool.monitor, NULL);
  assert(pool.active_tasks == 0);
  assert(pool.queue.size == 0);
  assert(metrics.size == 0);
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
  task.metric = malloc(sizeof(TaskMetric));
  if (task.metric == NULL) {
    printf("error mallocing new task metric\n");
    exit(1);
  }
  clock_gettime(CLOCK_MONOTONIC, &task.metric->created);
  task.metric->rdd = task.rdd;
  task.metric->pnum = task.pnum;

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

  // metric queue
  metrics.head = 0;
  metrics.tail = 0;
  metrics.size = 0;
  pthread_mutex_init(&metrics.lock, NULL);
  pthread_cond_init(&metrics.not_empty, NULL);
  metrics.logfile = fopen("metrics.log", "w");
  if (!metrics.logfile) {
    perror("fopen metrics.log");
    exit(1);
  }
  thread_pool_init(max(num_cpus - 1, 1));
  return;
}

void MS_TearDown() {
  thread_pool_destroy();
  if (metrics.logfile) {
    fclose(metrics.logfile);
    metrics.logfile = NULL;
  }
  free_dag(root);

  return;
}
// Caller must hold rdd lock before calling this function in worker threads
void submit_rdd(RDD *rdd) {
  assert(rdd->submitted == 0);
  assert(rdd->numdependencies > 0);
  // create one task for the whole rdd
  if (rdd->trans == PARTITIONBY) {
    Task task;
    task.rdd = rdd;
    task.pnum = -1;
    submit_task(task);
  } else {
    for (int i = 0; i < rdd->numpartitions; i++) {
      assert(rdd->materializedPartitions[i] == 0);
      Task task;
      task.rdd = rdd;
      task.pnum = i;
      submit_task(task);
    }
  }
  rdd->submitted = 1;
}
// main thread func
void execute(RDD *rdd) {
  int ready = 1;
  for (int i = 0; i < rdd->numdependencies; i++) {
    if (rdd->dependencies[i]->trans == FILE_BACKED) {
      continue;
    }
    ready = 0;
    RDD *dep = rdd->dependencies[i];
    execute(dep);
  }
  if (ready) {
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

void apply_map(RDD *rdd, int pnum) {
  assert(rdd->numdependencies == 1);
  RDD *dep = rdd->dependencies[0];
  assert(dep->materializedPartitions[pnum] == 1);
  void *(*fn)(void *) = rdd->fn;
  // printf("getting partition");
  List *partition = list_get(rdd->partitions, pnum);
  assert(partition != NULL);
  if (dep->trans == FILE_BACKED) {
    // printf("getting file ");
    FILE *fp = (FILE *)list_get(dep->partitions, pnum);
    void *item;
    while ((item = fn(fp)) != NULL) {
      list_append(partition, item);
    }
    return;
  }
  List *dep_partition = list_get(dep->partitions, pnum);
  assert(dep_partition != NULL);
  list_seek_to_start(dep_partition);
  void *next;
  while ((next = list_next(dep_partition)) != NULL) {
    void *item = fn(next);
    if (item != NULL) {
      list_append(partition, item);
    }
  }
  return;
}

void apply_filter(RDD *rdd, int pnum) {
  assert(rdd->numdependencies == 1);
  RDD *dep = rdd->dependencies[0];
  assert(dep->materializedPartitions[pnum] == 1);
  int (*fn)(void *item, void *ctx) = (int (*)(void *, void *))rdd->fn;
  void *ctx = rdd->ctx;

  List *partition = list_get(rdd->partitions, pnum);
  List *dep_partition = list_get(dep->partitions, pnum);
  assert(partition != NULL);
  assert(dep_partition != NULL);
  list_seek_to_start(dep_partition);
  void *next;
  while ((next = list_next(dep_partition)) != NULL) {
    if (fn(next, ctx)) {
      list_append(partition, next);
    }
  }
  return;
}
void apply_join(RDD *rdd, int pnum) {
  assert(rdd->numdependencies == 2);
  RDD *dep1 = rdd->dependencies[0];
  RDD *dep2 = rdd->dependencies[1];
  assert(dep1->materializedPartitions[pnum] == 1);
  assert(dep2->materializedPartitions[pnum] == 1);
  void *(*fn)(void *, void *, void *) = rdd->fn;
  List *partition = list_get(rdd->partitions, pnum);
  List *dep1_partition = list_get(dep1->partitions, pnum);
  List *dep2_partition = list_get(dep2->partitions, pnum);

  list_seek_to_start(dep1_partition);
  void *next1;
  void *next2;
  while ((next1 = list_next(dep1_partition)) != NULL) {
    list_seek_to_start(dep2_partition);
    while ((next2 = list_next(dep2_partition)) != NULL) {
      void *item = fn(next1, next2, rdd->ctx);
      if (item != NULL) {
        list_append(partition, item);
      }
    }
    free(next1);
  }
  list_seek_to_start(dep2_partition);
  while ((next2 = list_next(dep2_partition)) != NULL) {
    free(next2);
  }
  return;
}

void apply_partition(RDD *rdd, int pnum) {
  assert(rdd->numdependencies == 1);
  assert(pnum == -1);
  RDD *dep = rdd->dependencies[0];
  assert(is_rdd_materialized(dep));
  unsigned long (*fn)(void *, int, void *) = rdd->fn;
  for (int i = 0; i < dep->numpartitions; i++) {
    List *dep_partition = list_get(dep->partitions, i);
    list_seek_to_start(dep_partition);
    void *next;
    while ((next = list_next(dep_partition)) != NULL) {
      unsigned long partition_num = fn(next, rdd->numpartitions, rdd->ctx);
      list_append(list_get(rdd->partitions, partition_num), next);
    }
  }
  for (int i = 0; i < rdd->numpartitions; i++) {
    rdd->materializedPartitions[i] = 1;
  }
}

// materialize the partition
void materialize_partition(RDD *rdd, int pnum) {
  assert(rdd->trans != FILE_BACKED);
  if (rdd->trans == MAP) {
    apply_map(rdd, pnum);
  } else if (rdd->trans == FILTER) {
    apply_filter(rdd, pnum);
  } else if (rdd->trans == PARTITIONBY) {
    apply_partition(rdd, pnum);
  } else if (rdd->trans == JOIN) {
    apply_join(rdd, pnum);
  }
}
void push_metric(TaskMetric *metric) {
  pthread_mutex_lock(&metrics.lock);
  metrics.queue[metrics.tail] = *metric;
  metrics.tail = (metrics.tail + 1) % QUEUE_CAPACITY;
  metrics.size++;
  pthread_cond_signal(&metrics.not_empty);
  pthread_mutex_unlock(&metrics.lock);
  free(metric);
}
// worker thread func
void execute_task(Task task) {
  // printf("executing task %d  ", task.pnum);
  // print_enum(task.rdd->trans);
  int rdd_done = 0;
  materialize_partition(task.rdd, task.pnum);
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  task.metric->duration = TIME_DIFF_MICROS(task.metric->scheduled, end);
  push_metric(task.metric);
  pthread_mutex_lock(&task.rdd->lock);
  if (task.pnum != -1) {
    task.rdd->materializedPartitions[task.pnum] = 1;
  }
  rdd_done = is_rdd_materialized(task.rdd);
  // printf("rdd done %d\n", rdd_done);
  pthread_mutex_unlock(&task.rdd->lock);
  RDD *parent = task.rdd->parent;

  if (rdd_done) {
    if (task.rdd->done != NULL) {
      pthread_mutex_lock(&task.rdd->lock);
      // printf("Braodcasting done \n");
      pthread_cond_broadcast(task.rdd->done);
      pthread_mutex_unlock(&task.rdd->lock);
      return;
    }
    if (parent != NULL) {
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
    clock_gettime(CLOCK_MONOTONIC, &task.metric->scheduled);
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
void execute_and_wait(RDD *rdd) {
  assert(rdd->trans != FILE_BACKED);
  rdd->done = malloc(sizeof(pthread_cond_t));
  pthread_cond_init(rdd->done, NULL);
  execute(rdd);
  pthread_mutex_lock(&rdd->lock);
  while (is_rdd_materialized(rdd) == 0) {
    pthread_cond_wait(rdd->done, &rdd->lock);
  }
  // printf("woken up\n");
  pthread_cond_destroy(rdd->done);
  free(rdd->done);
  pthread_mutex_unlock(&rdd->lock);
}

void *monitor_loop(void *arg) {
  (void)arg;  // unused
  while (1) {
    pthread_mutex_lock(&metrics.lock);
    while (metrics.size == 0) {
      pthread_mutex_lock(&pool.lock);
      if (pool.shutdown && pool.active_tasks == 0 && pool.queue.size == 0) {
        pthread_mutex_unlock(&pool.lock);
        pthread_mutex_unlock(&metrics.lock);
        pthread_exit(NULL);
      }
      pthread_mutex_unlock(&pool.lock);
      pthread_cond_wait(&metrics.not_empty, &metrics.lock);
    }
    TaskMetric metric = metrics.queue[metrics.head];
    metrics.head = (metrics.head + 1) % QUEUE_CAPACITY;
    metrics.size--;
    pthread_mutex_unlock(&metrics.lock);
    print_formatted_metric(&metric, metrics.logfile);
  }
  pthread_exit(NULL);
  return NULL;
}
void free_dag(RDD *rdd) {
  for (int i = 0; i < rdd->numdependencies; i++) {
    free_dag(rdd->dependencies[i]);
  }
  free_rdd(rdd);
  return;
}
int count(RDD *rdd) {
  root = rdd;
  execute_and_wait(rdd);
  assert(is_rdd_materialized(rdd));
  int count = 0;
  // count all the items in rdd
  for (int i = 0; i < rdd->numpartitions; i++) {
    List *partition = list_get(rdd->partitions, i);
    list_seek_to_start(partition);
    void *item;
    while ((item = list_next(partition)) != NULL) {
      count++;
    }
  }
  return count;
}

void print(RDD *rdd, Printer p) {
  root = rdd;
  execute_and_wait(rdd);
  assert(is_rdd_materialized(rdd));
  // print all the items in rdd
  for (int i = 0; i < rdd->numpartitions; i++) {
    List *partition = list_get(rdd->partitions, i);
    list_seek_to_start(partition);
    void *item;
    while ((item = list_next(partition)) != NULL) {
      p(item);
    }
  }
  // aka... `p(item)` for all items in rdd
}
