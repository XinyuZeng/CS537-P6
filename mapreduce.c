//
// Created by zengx on 2019/11/12.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "./mapreduce.h"

typedef struct _Pair {
    char *key;
    char *value;
} Pair;

typedef struct _arg_mapper {
    Mapper map;
    int cnt;
    char **files;
} arg_mapper;

typedef struct _arg_reducer {
    Reducer reduce;
    int start_num;
    int cnt;
} arg_reducer;

// lock for each partition
pthread_mutex_t *lock;

// partition function, init after MR_RUN
Partitioner realPartitioner = NULL;

// 2d dynamic array to save key-value pairs
Pair **partitionTable = NULL;

// real number of partitions, renew when MR_Run calls
int real_num_partitions = 0;

// capacity of each partition
int *capacity = NULL;

// current size of each partition
int *size = NULL;

// current index for each partition, using for get_next
int *indexForGet = NULL;

int comparator(const void *p, const void *q) {
    Pair x = *(const Pair *) p;
    Pair y = *(const Pair *) q;

    /* Avoid return x - y, which can cause undefined behaviour
       because of signed integer overflow. */
    if (strcmp(x.key, y.key) < 0)
        return -1;
    else if (strcmp(x.key, y.key) > 0)
        return 1;

    return 0;
}

void Pthread_mutex_init(pthread_mutex_t *mutex) {
    if (pthread_mutex_init(mutex, NULL) != 0) {
        printf("Error when creating lock!\n");
    }
}

void Pthread_mutex_lock(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex) != 0) {
        printf("Error when getting lock!\n");
    }
}

void Pthread_mutex_unlock(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex) != 0) {
        printf("Error when unlocking!\n");
    }
}

void round_robin_dispatch(int *num_loads_per_worker,
        int num_worker, int num_file) {
    for (int i = 0; i < num_worker; ++i) {
        num_loads_per_worker[i] = num_file / num_worker;
        if (i < num_file % num_worker) {
            num_loads_per_worker[i]++;
        }
    }
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

void *mapper(void *arg) {
    arg_mapper *args = (arg_mapper *) arg;
    for (int i = 1; i <= args->cnt; ++i) {
        args->map(args->files[i]);
    }
    return NULL;
}

void reducer_helper(Reducer reduce, int p_num) {
    while (indexForGet[p_num] < size[p_num]) {
        reduce(partitionTable[p_num][indexForGet[p_num]].key, get_next, p_num);
    }
}

void *reducer(void *arg) {
    arg_reducer *args = (arg_reducer *) arg;
    for (int i = args->start_num; i < args->start_num + args->cnt; ++i) {
        reducer_helper(args->reduce, i);
    }
    return NULL;
}

void MR_Emit(char *key, char *value) {
    char *key_copy = (char *) malloc(sizeof(*key));
    char *value_copy = (char *) malloc(sizeof(*value));
    strcpy(key_copy, key);
    strcpy(value_copy, value);
    u_int32_t hash = realPartitioner(key, real_num_partitions);
//    printf("hash: %lu key: %lx key: %s\n", hash, (unsigned long)*key, key);
    // alloc space dynamically
    Pthread_mutex_lock(&lock[hash]);
    if (capacity[hash] == 0) {
        capacity[hash] = 16;
        partitionTable[hash] = (Pair *) malloc(sizeof(Pair) * 16);
    } else if (size[hash] == capacity[hash]) {
        partitionTable[hash] = (Pair *) realloc(partitionTable[hash],
                2 * capacity[hash] * sizeof(Pair));
        capacity[hash] = capacity[hash] * 2;
    }
    // insert into table
    partitionTable[hash][size[hash]].key = key_copy;
    partitionTable[hash][size[hash]].value = value_copy;
    ++size[hash];
    Pthread_mutex_unlock(&lock[hash]);
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
    partitionTable = (Pair **) malloc(sizeof(Pair *) * num_partitions);
    size = (int *) calloc(num_partitions, sizeof(int));
    capacity = (int *) calloc(num_partitions, sizeof(int));
    indexForGet = (int *) calloc(num_partitions, sizeof(int));

    // init lock
    lock = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t) * num_partitions);
    for (int i = 0; i < num_partitions; ++i) {
        Pthread_mutex_init(&lock[i]);
    }

    // divide num_files files to num_mappers mappers.
    // ?Shortest File First
    int num_files_per_mapper[num_mappers];
    round_robin_dispatch(num_files_per_mapper, num_mappers, num_files);

    // create num_mappers threads
    pthread_t p_mapper[num_mappers];
    arg_mapper argsMapper[num_mappers];
    int mapper_cnt = 0;
    for (int i = 0; i < num_mappers; ++i) {
        argsMapper[i].map = map;
        argsMapper[i].cnt = num_files_per_mapper[i];
        argsMapper[i].files = (char **) (argv + mapper_cnt);
//        mapper(map, num_files_per_mapper[i], argv + mapper_cnt);
        pthread_create(&p_mapper[i], NULL, mapper, &argsMapper[i]);
        mapper_cnt += num_files_per_mapper[i];
    }

    // main thread join
    for (int i = 0; i < num_mappers; ++i) {
        pthread_join(p_mapper[i], NULL);
    }

    // sort every partition
    for (int i = 0; i < real_num_partitions; ++i) {
        qsort(partitionTable[i], size[i], sizeof(Pair), comparator);
    }

    // divide num_partitions of partitions to num_reducers of reducers
    int num_partitions_per_reducer[num_reducers];
    round_robin_dispatch(num_partitions_per_reducer,
            num_reducers, num_partitions);

    // create num_reducers reducers threads
    pthread_t p_reducer[num_reducers];
    arg_reducer argsReducer[num_reducers];
    int reducer_cnt = 0;
    for (int i = 0; i < num_reducers; ++i) {
        argsReducer[i].reduce = reduce;
        argsReducer[i].start_num = reducer_cnt;
        argsReducer[i].cnt = num_partitions_per_reducer[i];
        pthread_create(&p_reducer[i], NULL, reducer, &argsReducer[i]);
        reducer_cnt += num_partitions_per_reducer[i];
    }

    // main thread join for reducers
    for (int i = 0; i < num_reducers; ++i) {
        pthread_join(p_reducer[i], NULL);
    }

    // free
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
    u_int32_t hash = 5381;
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
    char *endptr;
    u_int32_t hash = strtoul(key, &endptr, 10) >> (32 - high_bits);
    return hash;
}
