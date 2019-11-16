//
// Created by zengx on 2019/11/12.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

typedef struct _Pair {
    char *key;
    char *value;
}Pair;

Partitioner realPartitioner = NULL;
Pair **partitionTable = NULL;
int real_num_partitions = 0;
int *capacity = NULL;
int *size = NULL;
int *indexForGet = NULL;

int comparator(const void *p, const void *q) {
    Pair x = *(const Pair *)p;
    Pair y = *(const Pair *)q;

    /* Avoid return x - y, which can cause undefined behaviour
       because of signed integer overflow. */
    if (strcmp(x.key, y.key) < 0)
        return -1;  // Return -1 if you want ascending, 1 if you want descending order.
    else if (strcmp(x.key, y.key) > 0)
        return 1;   // Return 1 if you want ascending, -1 if you want descending order.

    return 0;
}

char *get_next(char *key, int partition_number) {
    int index = indexForGet[partition_number];
    if (index >= size[partition_number])
        return NULL;
    if (strcmp(key, partitionTable[partition_number][index].key) != 0)
        return NULL;
    ++indexForGet[partition_number];
    return partitionTable[partition_number][index].value;
}

void mapper(Mapper map, char *files[], int cnt){
    for (int i = 1; i <= cnt; ++i) {
        map(files[i]);
    }
}

void reducer(Reducer reduce, int partition_number) {
    while (indexForGet[partition_number] < size[partition_number]) {
        reduce(partitionTable[partition_number][indexForGet[partition_number]].key, get_next, partition_number);
    }
}

void MR_Emit(char *key, char *value) {
    unsigned long hash = realPartitioner(key, real_num_partitions);
    // alloc space dynamically
    if (capacity[hash] == 0) {
        capacity[hash] = 16;
        partitionTable[hash] = (Pair *)malloc(sizeof(Pair) * 16);
    } else if (size[hash] == capacity[hash]) {
        partitionTable[hash] = (Pair *)realloc(partitionTable[hash],
                                               2 * capacity[hash] * sizeof(Pair));
        capacity[hash] = capacity[hash] * 2;
    }
    // insert into table
    char *key_copy = (char *)malloc(sizeof(*key));
    char *value_copy = (char *)malloc(sizeof(*value));
    strcpy(key_copy, key);
    strcpy(value_copy, value);
    partitionTable[hash][size[hash]].key = key_copy;
    partitionTable[hash][size[hash]].value = value_copy;
    ++size[hash];
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition, int num_partitions) {
    // init globals
    int num_files = argc - 1;
    realPartitioner = partition;
    real_num_partitions = num_partitions;

    // create num_partitions of partitions
    partitionTable = (Pair **)malloc(sizeof(Pair *) * num_partitions);
    size = (int *)calloc(num_partitions, sizeof(int));
    capacity = (int *)calloc(num_partitions, sizeof(int));
    indexForGet = (int *)calloc(num_partitions, sizeof(int));

    // divide num_files files to num_mappers mappers.

    // create num_mappers threads
    mapper(map, argv, num_files);

    // main thread join

    // sort every partition
    qsort(partitionTable[0], size[0], sizeof(Pair), comparator);

    // divide num_partitions of partitions to num_reducers of reducers

    // create num_reducers reducers
    reducer(reduce, 0);

    // TODO: free
    for (int i = 0; i < real_num_partitions; ++i) {
        for (int j = 0; j < size[i]; ++j) {
            free(partitionTable[i][j].key);
            free(partitionTable[i][j].value);
            partitionTable[i][j].key = NULL;
            partitionTable[i][j].value = NULL;
        }
        free(partitionTable[i]);
        partitionTable[i] = NULL;
    }
    free(partitionTable);
    partitionTable = NULL;
    free(size);
    free(capacity);
    free(indexForGet);
    capacity = NULL;
    size = NULL;
    indexForGet = NULL;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    int high_bits = 0;
    while (num_partitions != 1) {
        high_bits++;
        num_partitions /= 2;
    }
    unsigned long hash = (unsigned long)*key >> (32 - high_bits);
    return hash;
}