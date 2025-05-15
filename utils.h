#ifndef UTILS_H
#define UTILS_H

//βιβλιοθήκες που θα χρειαστεί και το utils.c
#include <stdio.h> //για FILE *
#include <stdlib.h> //για free
#include <string.h>  //για strcpy, strcmp
#include <time.h>   //για strftime
#include <sys/socket.h> //για bind, socket
#include <arpa/inet.h> //για πληρότητα και αποφυγή απροσδόκητης συμπεριφοράς
#include <netinet/in.h> //για struct sockaddr_in
#include <unistd.h> //για close, read

//ορισμοί σταθερών
#define MAX_LINE_LENGTH 1024
#define MAX_HOST_LENGTH 64
#define MAX_DIR_LENGTH 256
#define MAX_PATH_LENGTH 512

extern FILE *global_log_fp; //global pointer στο manager_logfile
extern pthread_mutex_t log_mutex; //mutex για πρόσβαση στο manager_logfile
extern volatile int shutting_down; //flag για shutdown 

//δομή εκφώνησης
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

//δομή λίστας
typedef struct {
    sync_info_mem *head;
    int size;
} sync_info_mem_store;

//δομή εργασιών
typedef struct {
    char source_host[MAX_HOST_LENGTH];
    int source_port;
    char source_path[MAX_PATH_LENGTH];

    char target_host[MAX_HOST_LENGTH];
    int target_port;
    char target_path[MAX_PATH_LENGTH];

    sync_info_mem *parent_entry;  //προέλευση
} sync_task;

//δομή κυκλικής ουράς
typedef struct {
    sync_task *tasks;      
    int capacity;          
    int count;              
    int front;             
    int rear;             

    pthread_mutex_t mutex;
    pthread_cond_t not_full;  //υπάρχει χώρος 
    pthread_cond_t not_empty;  //υπάρχει εργασία
} task_queue;

//δομή worker
typedef struct {
    int id;
    task_queue *queue;
    FILE *log_fp;
    pthread_mutex_t *log_mutex;
} worker_args;


/*--------------------------------------------   NFS_MANAGER  -----------------------------------------------*/

//συνάρτηση σφάλματος
void usage_m(const char *prog_name);

//συνάρτηση για διάβασμα από config
void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store);

//συνάρτηση διαχείρισης εντολής από console
int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store, pthread_mutex_t *log_mutex);

//συνάρτηση προσθήκης στην δομή
void add_sync_info(sync_info_mem_store *store, sync_info_mem *info);

//συνάρτηση αναζήτησης στην δομή
sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir);

//συνάρτηση καθαρισμού δομής
void free_sync_info_store(sync_info_mem_store *store);

//συνάρτηση δημιουργίας socket
int create_server_socket(int port);

//συνάρτηση αποστολής LIST και εισαγωγής των εργασιών στην ουρά (βοηθητική)
void send_list_and_enqueue_tasks(sync_info_mem_store *store, task_queue *queue, FILE *logfile);

//συνάρτηση αποστολής LIST και εισαγωγής των εργασιών στην ουρά (κύρια)
int send_list_and_enqueue(sync_info_mem *entry, task_queue *queue, FILE *logfile);

//συνάρτηση σύνδεσης με τον client
int connect_to_client(const char *ip, int port);

//συνάρτηση εργασιών συγχρονισμού
void process_task_serially(sync_task task, FILE *logfile, pthread_mutex_t *log_mutex);

//συνάρτηση υλοποίησης PULL
int pull_file(const char *host, int port, const char *filepath,
              char **out_data, int *out_size, int *out_errno);

//συνάρτηση υλοποίησης PUSH
int push_file(const char *host, int port, const char *filepath,
              const char *data, int size, int *out_errno);

//συνάρτηση αρχικοποίησης ουράς εργασιών
void init_task_queue(task_queue *q, int capacity);

//συνάρτηση καταστροφής ουράς εργασιών
void destroy_task_queue(task_queue *q);

//συνάρτησης εισόδου στην ουρά εργασιών
void enqueue_task(task_queue *q, sync_task *task);

//πολυνηματική συνάρτηση των workers
void* worker_thread(void *arg);

//συνάρτηση εξόδου από την ουρά εργασιών
void dequeue_task(task_queue *q, sync_task *task_out);

//συνάρτηση απαλοιφής κατάληξης
char *strip_extension(const char *path);



/*--------------------------------------------   NFS_CONSOLE  -----------------------------------------------*/
//συνάρτηση σφάλματος
void usage_c(const char *progname);

//συνάρτηση δημιουργίας socket
int create_socket(const char *host_ip, int host_port); 

//συνάρτηση εντολής μέσω γραμμής εντολών
void command_loop(int sockfd, FILE *logfile);



/*--------------------------------------------   NFS_CLIENT  -----------------------------------------------*/
//συνάρτηση εκκίνησης socket
int start_server_socket(int port);

//πολυνηματική συνάρτηση διαχείρισης εντολών manager (βοηθητική)
void *handle_client_thread(void *arg);

//συνάρτηση διαχείρισης εντολών manager (κύρια)
void handle_client(int client_fd);

//συνάρτηση διαχειρισης εντολής PULL
int handle_pull(int client_fd, const char *filepath);

//συνάρτηση διαχειρισης εντολής PUSH
int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data);


/*--------------------------------------------   ΚΟΙΝΗ ΣΥΝΑΡΤΗΣΗ  -----------------------------------------------*/
//συνάρτηση απαλοιφής αρχικού / για relative path
const char* strip_leading_slash(const char* path);

#endif // UTILS_H
