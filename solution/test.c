#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "minispark.h"
#include <unistd.h>
void list_test() {
    List* l = list_init(10);
    char* str = malloc(10);
    strcpy(str, "hello");
    list_append(l, str);
    str = malloc(10);
    strcpy(str, "world");
    list_append(l, str);
    pthread_mutex_lock(&l->lock);
    for (int i = 0; i < l->size; i++) {
        char* item = (char*)list_get(l, i);
        printf("%s\n", item);
    }
    list_seek_to_start(l);
    printf("Next item: %s\n", (char*)list_next(l));
    printf("Next item: %s\n", (char*)list_next(l));
    printf("Next item: %s\n", (char*)list_next(l));
    pthread_mutex_unlock(&l->lock);
    list_free(l);

}
void rdd_files_test() {
    char *filenames[] = {"one.txt", "two.txt"};
    int numfiles = 2;
    RDD *rdd = RDDFromFiles(filenames, numfiles);
    if (rdd == NULL) {
        printf("RDDFromFiles failed\n");
        return;
    }
    if (rdd->partitions == NULL) {
        printf("RDDFromFiles partitions is NULL\n");
        return;
    }
    if (rdd->numdependencies != 0) {
        printf("RDDFromFiles numdependencies is not 0\n");
        return;
    }
    pthread_mutex_lock(&rdd->partitions->lock);
    list_seek_to_start(rdd->partitions);
    for (int i = 0; i < rdd->partitions->size; i++) {
        FILE *fp = (FILE *)list_next(rdd->partitions);
        if (fp == NULL) {
            printf("RDDFromFiles partition %d is NULL\n", i);
            return;
        }
        char line[256];
        printf("RDDFromFiles partition %d:\n", i);
        while (fgets(line, sizeof(line), fp) != NULL) {
            printf("%s", line);
        }
    }

}
void test_thread_pool() {
    thread_pool_init(2);
    printf("Submitting tasks...\n");

    for (int i = 0; i < 10; i++) {
        Task t;
        t.rdd = (RDD*)(0x1 + i);  // dummy non-null pointers (for logging only)
        t.pnum = i;
        t.metric = NULL;
        submit_task(t);
    }

    printf("Submitted 10 tasks. Tearing down...\n");
    thread_pool_destroy();

    printf("All tasks completed. Shutdown clean.\n");
}
int main(int argc, char** argv) {
    test_thread_pool();
}