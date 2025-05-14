#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#define MAX_LINE_LENGTH 1024
#define MAX_HOST_LENGTH 64
#define MAX_DIR_LENGTH 256
#define MAX_PATH_LENGTH 512

extern FILE *global_log_fp;
extern pthread_mutex_t log_mutex;
extern volatile int shutting_down;

typedef struct sync_info_mem {
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

typedef struct {
    char source_host[MAX_HOST_LENGTH];
    int source_port;
    char source_path[MAX_PATH_LENGTH];

    char target_host[MAX_HOST_LENGTH];
    int target_port;
    char target_path[MAX_PATH_LENGTH];

    sync_info_mem *parent_entry;  
} sync_task;

typedef struct {
    sync_task *tasks;      
    int capacity;          
    int count;              
    int front;             
    int rear;             

    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} task_queue;

typedef struct {
    int id;
    task_queue *queue;
    FILE *log_fp;
    pthread_mutex_t *log_mutex;
} worker_args;

void usage_m(const char *prog_name);
void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store);
void add_sync_info(sync_info_mem_store *store, sync_info_mem *info);
sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir);
void free_sync_info_store(sync_info_mem_store *store);
int create_server_socket(int port);
int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store, pthread_mutex_t *log_mutex);
int connect_to_client(const char *ip, int port);
int send_list_command(int sockfd, const char *source_dir, FILE *logfile, sync_info_mem *entry);
int pull_file(const char *host, int port, const char *filepath,
              char **out_data, int *out_size, int *out_errno);
int push_file(const char *host, int port, const char *filepath,
              const char *data, int size, int *out_errno);
void init_task_queue(task_queue *q, int capacity);
void destroy_task_queue(task_queue *q);
void enqueue_task(task_queue *q, sync_task *task);
void dequeue_task(task_queue *q, sync_task *task_out);
void send_list_and_enqueue_tasks(sync_info_mem_store *store, task_queue *queue, FILE *logfile);
int send_list_and_enqueue(sync_info_mem *entry, task_queue *queue, FILE *logfile);


void usage_c(const char *progname);
int create_socket(const char *host_ip, int host_port); 
void command_loop(int sockfd, FILE *logfile);


int start_server_socket(int port);
void handle_client(int client_fd);
void *handle_client_thread(void *arg);
int handle_pull(int client_fd, const char *filepath);
int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data);

void* worker_thread(void *arg);

void send_list_and_process_all(sync_info_mem_store *store, FILE *logfile);
int send_list_and_process(sync_info_mem *entry, FILE *logfile);
void process_task_serially(sync_task task, FILE *logfile, pthread_mutex_t *log_mutex);
char *strip_extension(const char *path);

#endif // UTILS_H
