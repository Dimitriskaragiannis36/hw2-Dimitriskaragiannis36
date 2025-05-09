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
#define MAX_LINE 1024

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

void usage_m(const char *prog_name);
void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store);
void add_sync_info(sync_info_mem_store *store, sync_info_mem *info);
sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir);
void free_sync_info_store(sync_info_mem_store *store);
int create_server_socket(int port);
int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store);
int connect_to_client(const char *ip, int port);
int send_list_command(int sockfd, const char *source_dir, FILE *logfile, sync_info_mem *entry);
int pull_file(const char *host, int port, const char *filepath, char **out_data, int *out_size);
int push_file(const char *host, int port, const char *filepath, const char *data, int size);


void usage_c(const char *progname);
int create_socket(const char *host_ip, int host_port); 
void command_loop(int sockfd, FILE *logfile);


int start_server_socket(int port);
void handle_client(int client_fd);
int handle_pull(int client_fd, const char *filepath);
int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data);


#endif // UTILS_H
