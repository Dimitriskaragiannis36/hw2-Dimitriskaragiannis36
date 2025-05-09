#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <time.h>

#define MAX_LINE_LENGTH 1024
#define MAX_HOST_LENGTH 64
#define MAX_DIR_LENGTH 256

typedef struct {
    char source_host[MAX_HOST_LENGTH];
    int source_port;
    char source_dir[MAX_DIR_LENGTH];

    char target_host[MAX_HOST_LENGTH];
    int target_port;
    char target_dir[MAX_DIR_LENGTH];

    int active;                
    int error_count;            
    time_t last_sync_time; 

    struct sync_info_mem *next;    
} sync_info_mem;

typedef struct {
    sync_info_mem *head;
    int size;
} sync_info_mem_store;

void usage(const char *prog_name);
void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store);
void add_sync_info(sync_info_mem_store *store, sync_info_mem *info);
sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir);
void free_sync_info_store(sync_info_mem_store *store);

#endif // UTILS_H
