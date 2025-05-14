#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <dirent.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>

FILE *global_log_fp = NULL;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void usage_m(const char *prog_name) {
    fprintf(stderr, "Usage: %s -l <manager_logfile> -c <config_file> -n <worker_limit> -p <port_number> -b <bufferSize>\n", prog_name);
    exit(EXIT_FAILURE);
}

void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen config_file");
        exit(EXIT_FAILURE);
    }

    char src_entry[MAX_LINE_LENGTH];
    char tgt_entry[MAX_LINE_LENGTH];

    while (fscanf(fp, "%s %s", src_entry, tgt_entry) == 2) {
        sync_info_mem *info = malloc(sizeof(sync_info_mem));
        if (!info) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memset(info, 0, sizeof(sync_info_mem));

        char *at = strchr(src_entry, '@');
        char *colon = strchr(src_entry, ':');
        if (!at || !colon || at > colon) {
            fprintf(stderr, "Invalid source entry: %s\n", src_entry);
            free(info);
            continue;
        }
        *at = '\0';
        *colon = '\0';
        strncpy(info->source_dir, src_entry, MAX_DIR_LENGTH);
        strncpy(info->source_host, at + 1, MAX_HOST_LENGTH);
        info->source_port = atoi(colon + 1);

        at = strchr(tgt_entry, '@');
        colon = strchr(tgt_entry, ':');
        if (!at || !colon || at > colon) {
            fprintf(stderr, "Invalid target entry: %s\n", tgt_entry);
            free(info);
            continue;
        }
        *at = '\0';
        *colon = '\0';
        strncpy(info->target_dir, tgt_entry, MAX_DIR_LENGTH);
        strncpy(info->target_host, at + 1, MAX_HOST_LENGTH);
        info->target_port = atoi(colon + 1);

        info->active = 1;
        info->error_count = 0;
        info->last_sync_time = 0;

        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char timestamp[64];
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", tm_info);

        fprintf(log_fp, "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                timestamp,
                info->source_dir, info->source_host, info->source_port,
                info->target_dir, info->target_host, info->target_port);

        fflush(log_fp); 

        printf("%s Added file: %s@%s:%d -> %s@%s:%d\n",
            timestamp,
            info->source_dir, info->source_host, info->source_port,
            info->target_dir, info->target_host, info->target_port);

        add_sync_info(store, info);
    }

    fclose(fp);
}


void add_sync_info(sync_info_mem_store *store, sync_info_mem *info) {
    info->next = store->head;
    store->head = info;
    store->size++;
}

sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir) {
    sync_info_mem *current = store->head;
    while (current) {
        if (strcmp(current->source_dir, source_dir) == 0) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}

void free_sync_info_store(sync_info_mem_store *store) {
    sync_info_mem *current = store->head;
    while (current) {
        sync_info_mem *next = current->next;
        free(current);
        current = next;
    }
    store->head = NULL;
    store->size = 0;
}

int create_server_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int optval = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        perror("setsockopt");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    if (listen(sockfd, 5) < 0) {
        perror("listen");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    return sockfd;
}
int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store, pthread_mutex_t *log_mutex) {
    char buffer[MAX_LINE_LENGTH];
    char response[MAX_LINE_LENGTH];
    int shutdown_requested = 0;

    while (1) {
        int bytes = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
        if (bytes <= 0) {
            if (bytes == 0) {
                printf("Console disconnected.\n");
                return 1;
            } else {
                perror("recv");
                printf("Error receiving command from console.\n");
            }
            break; 
        }

        buffer[bytes] = '\0';

        char timestamp[64];
        time_t now = time(NULL);
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

        response[0] = '\0';  

        if (strncmp(buffer, "shutdown", 8) == 0) {
            snprintf(response, sizeof(response),
                     "%s Shutting down manager...\n"
                     "%s Waiting for all active workers to finish.\n"
                     "%s Processing remaining queued tasks.\n"
                     "%s Manager shutdown complete\n",
                     timestamp, timestamp, timestamp, timestamp);
            shutdown_requested = 1;
        }

        else if (strncmp(buffer, "add ", 4) == 0) {
            char src_full[MAX_LINE_LENGTH], dst_full[MAX_LINE_LENGTH];
            if (sscanf(buffer + 4, "%1023s %1023s", src_full, dst_full) == 2) {
                char src_dir[MAX_DIR_LENGTH], src_host[MAX_HOST_LENGTH];
                int src_port;
                char dst_dir[MAX_DIR_LENGTH], dst_host[MAX_HOST_LENGTH];
                int dst_port;

                char *at = strchr(src_full, '@');
                char *colon = strrchr(src_full, ':');
                if (!at || !colon || at > colon) {
                    snprintf(response, sizeof(response), "%s Invalid source format.\n", timestamp);
                } else {
                    *at = '\0'; *colon = '\0';
                    strncpy(src_dir, src_full, MAX_DIR_LENGTH - 1);
                    strncpy(src_host, at + 1, MAX_HOST_LENGTH - 1);
                    src_port = atoi(colon + 1);
                }

                at = strchr(dst_full, '@');
                colon = strrchr(dst_full, ':');
                if (!at || !colon || at > colon) {
                    snprintf(response, sizeof(response), "%s Invalid target format.\n", timestamp);
                } else {
                    *at = '\0'; *colon = '\0';
                    strncpy(dst_dir, dst_full, MAX_DIR_LENGTH - 1);
                    strncpy(dst_host, at + 1, MAX_HOST_LENGTH - 1);
                    dst_port = atoi(colon + 1);
                }

                sync_info_mem *existing_info = find_sync_info(store, src_dir);
                if (existing_info && existing_info->active) {
                     snprintf(response, sizeof(response), "%s Already in queue: %.900s\n", timestamp, src_dir);
                } else {
                    sync_info_mem *info = malloc(sizeof(sync_info_mem));
                    if (info) {
                        memset(info, 0, sizeof(sync_info_mem));
                        strncpy(info->source_dir, src_dir, MAX_DIR_LENGTH - 1);
                        strncpy(info->source_host, src_host, MAX_HOST_LENGTH - 1);
                        info->source_port = src_port;

                        strncpy(info->target_dir, dst_dir, MAX_DIR_LENGTH - 1);
                        strncpy(info->target_host, dst_host, MAX_HOST_LENGTH - 1);
                        info->target_port = dst_port;

                        info->active = 1;
                        info->error_count = 0;
                        info->last_sync_time = 0;

                        add_sync_info(store, info);

                        DIR *dir = opendir(src_dir);
                        if (dir == NULL) {
                            snprintf(response, sizeof(response), "%s Cannot open directory: %s\n", timestamp, src_dir);
                        } else {
                            struct dirent *entry;
                            while ((entry = readdir(dir)) != NULL) {
                                if (entry->d_type == DT_REG) { 
                                    
                                char filename_no_ext[256];
                                strncpy(filename_no_ext, entry->d_name, 256 - 1);
                                filename_no_ext[256 - 1] = '\0';

                                char *dot = strrchr(filename_no_ext, '.');
                                if (dot) {
                                    *dot = '\0';
                                }

                                char log_src_path[MAX_PATH_LENGTH];
                                char log_dst_path[MAX_PATH_LENGTH];
                                snprintf(log_src_path, sizeof(log_src_path), "%s/%s", src_dir, filename_no_ext);
                                snprintf(log_dst_path, sizeof(log_dst_path), "%s/%s", dst_dir, filename_no_ext);

                                fprintf(logfile, "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                                        timestamp, log_src_path, src_host, src_port,
                                        log_dst_path, dst_host, dst_port);
                                fflush(logfile);

                                char src_path[MAX_PATH_LENGTH];
                                char dst_path[MAX_PATH_LENGTH];
                                snprintf(src_path, sizeof(src_path), "%s/%s", src_dir, entry->d_name);
                                snprintf(dst_path, sizeof(dst_path), "%s/%s", dst_dir, entry->d_name);
                                    sync_task task;
                                    strncpy(task.source_path, src_path, MAX_PATH_LENGTH);
                                    strncpy(task.source_host, src_host, MAX_HOST_LENGTH);
                                    task.source_port = src_port;

                                    strncpy(task.target_path, dst_path, MAX_PATH_LENGTH);
                                    strncpy(task.target_host, dst_host, MAX_HOST_LENGTH);
                                    task.target_port = dst_port;

                                    process_task_serially(task, logfile, log_mutex);
                                }
                            }
                            closedir(dir);
                            snprintf(response, sizeof(response), "%s Successfully synchronized directory.\n", timestamp);
                        }
                    } else {
                        snprintf(response, sizeof(response), "%s Error allocating memory.\n", timestamp);
                    }
                }
            } else {
                snprintf(response, sizeof(response), "%s Invalid add command format.\n", timestamp);
            }
        }

        else if (strncmp(buffer, "cancel ", 7) == 0) {
            char src[256];
            if (sscanf(buffer + 7, "%1023s", src) == 1) {
                sync_info_mem *curr = store->head;
                int any_active_found = 0;
                int any_inactive_found = 0;
                response[0] = '\0';

                while (curr) {
                    if (strcmp(curr->source_dir, src) == 0) {
                        if (curr->active) {
                            curr->active = 0;
                            any_active_found = 1;

                            char msg[512];
                            snprintf(msg, sizeof(msg), "%s Synchronization stopped for %s@%s:%d\n",
                                    timestamp, curr->source_dir, curr->source_host, curr->source_port);
                            strncat(response, msg, sizeof(response) - strlen(response) - 1);

                            fprintf(logfile, "%s", msg);
                            fflush(logfile);
                        } else {
                            any_inactive_found = 1;

                            char msg[512];
                            snprintf(msg, sizeof(msg),
                                    "%s Directory not being synchronized: %s@%s:%d\n",
                                    timestamp, curr->source_dir, curr->source_host, curr->source_port);
                            strncat(response, msg, sizeof(response) - strlen(response) - 1);
                        }
                    }
                    curr = curr->next;
                }

                if (!any_active_found && !any_inactive_found) {
                     snprintf(response, sizeof(response),
             "%s Directory not being synchronized: %s\n", timestamp, src);
                }

            } else {
                snprintf(response, sizeof(response), "%s Invalid cancel command format.\n", timestamp);
            }
        }

        else {
           snprintf(response, sizeof(response), "%s Unknown command: %.500s\n", timestamp, buffer);
        }

        send(client_sock, response, strlen(response), 0);

        if (shutdown_requested)
            break;
    }

    
    close(client_sock);
    return shutdown_requested;
}

/*int send_list_command(int sockfd, const char *source_dir, FILE *logfile, sync_info_mem *entry) {
    char command[1024];
    snprintf(command, sizeof(command), "LIST %s", source_dir);

    if (send(sockfd, command, strlen(command), 0) < 0) {
        perror("send");
        return -1;
    }

    FILE *sock_stream = fdopen(sockfd, "r");
    if (!sock_stream) {
        perror("fdopen");
        return -1;
    }

    char line[1024];
    while (fgets(line, sizeof(line), sock_stream)) {
        line[strcspn(line, "\n")] = '\0';
        if (strcmp(line, ".") == 0) break;

    fprintf(logfile,
        "[%s] Added file: %s/%s@%s:%d -> %s/%s@%s:%d\n",
        entry->source_dir,    
        entry->source_dir,        
        line,                    
        entry->source_host,       
        entry->source_port,       
        entry->target_dir,        
        line,                     
        entry->target_host,       
        entry->target_port       
    );
        fflush(logfile);
    }

    fclose(sock_stream);
    return 0;
}*/

int pull_file(const char *host, int port, const char *filepath,
              char **out_data, int *out_size, int *out_errno) {
    int sockfd;
    struct sockaddr_in serv_addr;

    *out_data = NULL;
    *out_size = 0;
    if (out_errno) *out_errno = 0;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (out_errno) *out_errno = errno;
        return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    char msg[1024];
    int msg_len = snprintf(msg, sizeof(msg), "PULL %s\n", filepath);
    if (msg_len < 0 || msg_len >= (int)sizeof(msg)) {
        if (out_errno) *out_errno = EINVAL;
        close(sockfd);
        return -1;
    }

    if (write(sockfd, msg, msg_len) != msg_len) {
        if (out_errno) *out_errno = errno;
        close(sockfd);
        return -1;
    }

    char header[64];  
    int header_pos = 0;
    char c;

    while (header_pos < (int)sizeof(header) - 1) {
        int n = recv(sockfd, &c, 1, 0);
        if (n <= 0) {
            if (out_errno) *out_errno = (n == 0 ? ECONNRESET : errno);
            close(sockfd);
            return -1;
        }
        if (c == ' ') break;
        header[header_pos++] = c;
    }

    header[header_pos] = '\0';
    int filesize = atoi(header);

    if (filesize == -1) {

        char *errbuf = malloc(1024);
        if (!errbuf) {
            if (out_errno) *out_errno = ENOMEM;
            close(sockfd);
            return -1;
        }

        int capacity = 1024;
        int len = 0;
        while (1) {
            int n = recv(sockfd, errbuf + len, capacity - len, 0);
            if (n <= 0) break;  
            len += n;
            if (len == capacity) {
                char *newbuf = realloc(errbuf, capacity * 2);
                if (!newbuf) {
                    free(errbuf);
                    if (out_errno) *out_errno = ENOMEM;
                    close(sockfd);
                    return -1;
                }
                errbuf = newbuf;
                capacity *= 2;
            }
        }

        errbuf[len] = '\0';
        fprintf(stderr, "Server error: %s\n", errbuf);
      
        if (out_errno) {
            if (strstr(errbuf, "Permission denied")) *out_errno = EACCES;
            else if (strstr(errbuf, "No such file or directory")) *out_errno = ENOENT;
            else if (strstr(errbuf, "Is a directory")) *out_errno = EISDIR;
            else if (strstr(errbuf, "Not a directory")) *out_errno = ENOTDIR;
            else if (strstr(errbuf, "Bad file descriptor")) *out_errno = EBADF;
            else if (strstr(errbuf, "Invalid argument")) *out_errno = EINVAL;
            else if (strstr(errbuf, "File exists")) *out_errno = EEXIST;
            else *out_errno = EIO;  
        }

        free(errbuf);
        close(sockfd);
        return -1;
    }

    if (filesize <= 0) {
        if (out_errno) *out_errno = EIO;
        close(sockfd);
        return -1;
    }

    char *data = malloc(filesize);
    if (!data) {
        if (out_errno) *out_errno = ENOMEM;
        close(sockfd);
        return -1;
    }

    int total_received = 0;
    while (total_received < filesize) {
        int n = recv(sockfd, data + total_received, filesize - total_received, 0);
        if (n <= 0) {
            int err = (n == 0) ? ECONNRESET : errno;
            free(data);
            close(sockfd);
            if (out_errno) *out_errno = err;
            return -1;
        }
        total_received += n;
    }

    *out_data = data;
    *out_size = filesize;
    close(sockfd);
    return 0;
}



int push_file(const char *host, int port, const char *filepath,
              const char *data, int size, int *out_errno) {
    int sockfd;
    struct sockaddr_in serv_addr;

    if (out_errno) *out_errno = 0;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (out_errno) *out_errno = errno;
        return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    char header[1024];
    int header_len = snprintf(header, sizeof(header), "PUSH %s %d ", filepath, size);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        close(sockfd);
        if (out_errno) *out_errno = EINVAL;
        return -1;
    }

    char *packet = malloc(header_len + size);
    if (!packet) {
        close(sockfd);
        if (out_errno) *out_errno = ENOMEM;
        return -1;
    }

    memcpy(packet, header, header_len);
    memcpy(packet + header_len, data, size);

    int total_sent = 0;
    while (total_sent < header_len + size) {
        int n = send(sockfd, packet + total_sent, header_len + size - total_sent, 0);
        if (n <= 0) {
            int err = (n == 0) ? ECONNRESET : errno;
            free(packet);
            close(sockfd);
            if (out_errno) *out_errno = err;
            return -1;
        }
        total_sent += n;
    }

    free(packet);

    int result_net;
    if (recv(sockfd, &result_net, sizeof(int), 0) != sizeof(int)) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    close(sockfd);

    int result = ntohl(result_net);
    if (result & 0x80000000) {
        if (out_errno) *out_errno = result & 0x7FFFFFFF;
        return -1;
    }

    return 0;
}




void init_task_queue(task_queue *q, int capacity) {
    q->capacity = capacity;
    q->count = 0;
    q->front = 0;
    q->rear = 0;
    q->tasks = malloc(capacity * sizeof(sync_task));

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

void destroy_task_queue(task_queue *q) {
    free(q->tasks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
}

void enqueue_task(task_queue *q, sync_task *task) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == q->capacity) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    q->tasks[q->rear] = *task;
    q->rear = (q->rear + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

void dequeue_task(task_queue *q, sync_task *task_out) {
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !shutting_down) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    if (shutting_down && q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        task_out->source_path[0] = '\0'; 
        return;
    }

    *task_out = q->tasks[q->front];
    q->front = (q->front + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}


void* worker_thread(void *arg) {
    worker_args *args = (worker_args*) arg;
    int id = args->id;
    task_queue *q = args->queue;

    while (1) {
        sync_task task;
        dequeue_task(q, &task);

        if (shutting_down && task.source_path[0] == '\0') {
            break; 
        }

        pthread_mutex_lock(&log_mutex);
        fprintf(args->log_fp, "[THREAD %d] Processing task from %s:%d:%s -> %s:%d:%s\n",
                id,
                task.source_host, task.source_port, task.source_path,
                task.target_host, task.target_port, task.target_path);
        pthread_mutex_unlock(&log_mutex);

        process_task_serially(task, args->log_fp, args->log_mutex);
    }

    return NULL;
}


void send_list_and_enqueue_tasks(sync_info_mem_store *store, task_queue *queue, FILE *logfile) {
    sync_info_mem *curr = store->head;
    while (curr) {
        send_list_and_enqueue(curr, queue, logfile);
        curr = curr->next;
    }
}

int send_list_and_enqueue(sync_info_mem *entry, task_queue *queue, FILE *logfile) {
    int sockfd = connect_to_client(entry->source_host, entry->source_port);
    if (sockfd < 0) {
        fprintf(logfile, "Failed to connect to %s:%d\n", entry->source_host, entry->source_port);
        return -1;
    }

    char command[1024];
    snprintf(command, sizeof(command), "LIST %s\n", entry->source_dir);
    if (send(sockfd, command, strlen(command), 0) < 0) {
        perror("send");
        close(sockfd);
        return -1;
    }

    FILE *sock_stream = fdopen(sockfd, "r");
    if (!sock_stream) {
        perror("fdopen");
        close(sockfd);
        return -1;
    }

    char line[1024];
    while (fgets(line, sizeof(line), sock_stream)) {
        line[strcspn(line, "\n")] = '\0';
        if (strcmp(line, ".") == 0) break;

        sync_task task;
        if (snprintf(task.source_path, MAX_PATH_LENGTH, "%s/%s", entry->source_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated source path\n");
            continue;
        }

        if (snprintf(task.target_path, MAX_PATH_LENGTH, "%s/%s", entry->target_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated source path\n");
            continue;
        }

        strncpy(task.source_host, entry->source_host, MAX_HOST_LENGTH);
        strncpy(task.target_host, entry->target_host, MAX_HOST_LENGTH);
        task.source_port = entry->source_port;
        task.target_port = entry->target_port;
        task.parent_entry = entry;

        enqueue_task(queue, &task);
    }

    fclose(sock_stream);  
    return 0;
}




void usage_c(const char *progname) {
    fprintf(stderr, "Usage: %s -l <console-logfile> -h <host_IP> -p <host_port>\n", progname);
    exit(EXIT_FAILURE);
}

int create_socket(const char *host_ip, int host_port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(host_port);
    if (inet_pton(AF_INET, host_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        exit(EXIT_FAILURE);
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

void command_loop(int sockfd, FILE *logfile) {
    char line[MAX_LINE_LENGTH];
  
    while (1) {
        printf("> ");
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\n")] = '\0';  

        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char timestamp[64];
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", tm_info);

        if (strncmp(line, "add ", 4) == 0) {
            char src[MAX_LINE_LENGTH], dst[MAX_LINE_LENGTH];
            if (sscanf(line + 4, "%s %s", src, dst) == 2) {
                fprintf(logfile, "%s Command add %s -> %s\n", timestamp, src, dst);
            } else {
                fprintf(logfile, "%s Command add (invalid format)\n", timestamp);
            }
        } else if (strncmp(line, "cancel ", 7) == 0) {
            char full_path[MAX_LINE_LENGTH];
            if (sscanf(line + 7, "%s", full_path) == 1) {
                char display_path[MAX_LINE_LENGTH];
                strncpy(display_path, full_path, sizeof(display_path));
                char *at = strchr(display_path, '@');
                if (at) {
                    *at = '\0';
                }
                fprintf(logfile, "%s Command cancel %s\n", timestamp, display_path);
            } else {
                fprintf(logfile, "%s Command cancel (invalid format)\n", timestamp);
            }
        } else if (strncmp(line, "shutdown", 8) == 0) {
            fprintf(logfile, "%s Command shutdown\n", timestamp);
        } else {
            fprintf(logfile, "%s Command %s\n", timestamp, line); 
        }

        fflush(logfile);

        if (send(sockfd, line, strlen(line), 0) < 0) {
            perror("send");
            break;
        }

        char response[MAX_LINE_LENGTH * 2];
        int n = recv(sockfd, response, sizeof(response) - 1, 0);
        if (n <= 0) {
            perror("recv");
            break;
        }
        response[n] = '\0';
        printf("%s", response);

        if (strncmp(line, "shutdown", 8) == 0)
            break;
    }
    close(sockfd);
}




int start_server_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    if (listen(sockfd, 5) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

void handle_client(int client_fd) {
    char buffer[MAX_LINE_LENGTH];
    int bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) return;

    buffer[bytes] = '\0';

    if (strncmp(buffer, "LIST ", 5) == 0) {
        char *dir_path = buffer + 5;
        dir_path[strcspn(dir_path, "\r\n")] = '\0';
        DIR *dir = opendir(dir_path);
        if (!dir) {
            perror("opendir");
            send(client_fd, ".\n", 2, 0);
            return;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (entry->d_type == DT_REG) {
                send(client_fd, entry->d_name, strlen(entry->d_name), 0);
                send(client_fd, "\n", 1, 0);
            }
        }
        send(client_fd, ".\n", 2, 0);
        closedir(dir);
    }

    else if (strncmp(buffer, "PULL ", 5) == 0) {
        char *filepath = buffer + 5;
        filepath[strcspn(filepath, "\n")] = '\0';
        printf("[nfs_client] Received PULL for path: '%s'\n", filepath);
        handle_pull(client_fd, filepath);
    }

    else if (strncmp(buffer, "PUSH ", 5) == 0) {
        char filepath[1024];
        int chunk_size;

        char *after_cmd = buffer + 5;
        char *first_space = strchr(after_cmd, ' ');
        if (!first_space) return;
        char *second_space = strchr(first_space + 1, ' ');
        if (!second_space) return;

        *second_space = '\0';  

        strncpy(filepath, after_cmd, first_space - after_cmd);
        filepath[first_space - after_cmd] = '\0';

        chunk_size = atoi(first_space + 1);

        char *data_start = second_space + 1;
        int data_in_buffer = bytes - (data_start - buffer);

        char *chunk_data = malloc(chunk_size);
        if (!chunk_data) return;

        memcpy(chunk_data, data_start, data_in_buffer);

        int total_read = data_in_buffer;
        while (total_read < chunk_size) {
            int n = recv(client_fd, chunk_data + total_read, chunk_size - total_read, 0);
            if (n <= 0) {
                free(chunk_data);
                return;
            }
            total_read += n;
        }


        handle_push(client_fd, filepath, chunk_size, chunk_data);
        free(chunk_data);
    }
}

void *handle_client_thread(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);

    handle_client(client_fd);
    close(client_fd);

    return NULL;
}

int connect_to_client(const char *ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

int handle_pull(int client_fd, const char *filepath) {
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to open file: %s\n", strerror(errno));
        send(client_fd, msg, strlen(msg), 0);
        perror("open (PULL)");
        return -1;
    }

    off_t filesize = lseek(fd, 0, SEEK_END);
    if (filesize < 0) {
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to stat file: %s\n", strerror(errno));
        close(fd);
        send(client_fd, msg, strlen(msg), 0);
        perror("lseek (SEEK_END)");
        return -1;
    }

    if (lseek(fd, 0, SEEK_SET) < 0) {
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to rewind file: %s\n", strerror(errno));
        close(fd);
        send(client_fd, msg, strlen(msg), 0);
        perror("lseek (SEEK_SET)");
        return -1;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%ld ", (long)filesize);
    if (send(client_fd, header, header_len, 0) != header_len) {
        perror("send (header)");
        close(fd);
        return -1;
    }

    char buffer[4096];
    ssize_t n;
    while ((n = read(fd, buffer, sizeof(buffer))) > 0) {
        if (send(client_fd, buffer, n, 0) != n) {
            perror("send (file data)");
            close(fd);
            return -1;
        }
    }

    if (n < 0) {
        perror("read (file)");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data) {
    int fd;

    if (chunk_size == -1) {
        fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            int err = htonl(0x80000000 | errno);
            send(client_fd, &err, sizeof(int), 0);
            perror("open create");
            return -1;
        }
        close(fd);

        int ok = htonl(0);
        send(client_fd, &ok, sizeof(int), 0);
        return 0;
    }

    if (chunk_size == 0) {
        int ok = htonl(0);
        send(client_fd, &ok, sizeof(int), 0);
        return 0;
    }

    fd = open(filepath, O_WRONLY | O_APPEND, 0644);
    if (fd == -1) {
        int err = htonl(0x80000000 | errno);
        send(client_fd, &err, sizeof(int), 0);
        perror("open append");
        return -1;
    }

    ssize_t written = write(fd, data, chunk_size);
    if (written != chunk_size) {
        int err = htonl(0x80000000 | EIO);
        send(client_fd, &err, sizeof(int), 0);
        perror("write");
        close(fd);
        return -1;
    }

    close(fd);

    int ok = htonl(0);
    send(client_fd, &ok, sizeof(int), 0);
    return 0;
}





void process_task_serially(sync_task task, FILE *logfile, pthread_mutex_t *log_mutex) {
    char *file_data = NULL;
    int file_size = 0;

    char timestamp[64];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    pid_t pid = getpid();
    char *source_no_ext = strip_extension(task.source_path);
    char *target_no_ext = strip_extension(task.target_path);
    int pull_errno = 0;
    if (pull_file(task.source_host, task.source_port, task.source_path, &file_data, &file_size, &pull_errno) == 0) {
        pthread_mutex_lock(log_mutex);    
        fprintf(logfile,
            "[%s] [%s@%s:%d] [%s@%s:%d] [%d] [PULL] [SUCCESS] [%d bytes pulled]\n",
            timestamp,
            source_no_ext, task.source_host, task.source_port,
            target_no_ext, task.target_host, task.target_port,
            pid, file_size
        );
        fflush(logfile);
        pthread_mutex_unlock(log_mutex);

        int push_errno = 0;
        if (push_file(task.target_host, task.target_port, task.target_path, file_data, file_size, &push_errno) == 0) {
            now = time(NULL);
            tm_info = localtime(&now);
            strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

            pthread_mutex_lock(log_mutex);
            fprintf(logfile,
                "[%s] [%s@%s:%d] [%s@%s:%d] [%d] [PUSH] [SUCCESS] [%d bytes pushed]\n",
                timestamp,
                source_no_ext, task.source_host, task.source_port,
                target_no_ext, task.target_host, task.target_port,
                pid, file_size
            );
            fflush(logfile);
            pthread_mutex_unlock(log_mutex);
        } else {
            const char *filename = strrchr(task.target_path, '/');
            filename = filename ? filename + 1 : task.target_path;

            now = time(NULL);
            tm_info = localtime(&now);
            strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

            pthread_mutex_lock(log_mutex);
            fprintf(logfile,
                "[%s] [%s@%s:%d] [%s@%s:%d] [%d] [PUSH] [ERROR] [File: %s - %s]\n",
                timestamp,
                source_no_ext, task.source_host, task.source_port,
                target_no_ext, task.target_host, task.target_port,
                pid, filename, strerror(push_errno)
            );
            fflush(logfile);
            pthread_mutex_unlock(log_mutex);
        }

        free(file_data);
    } else {
        const char *filename = strrchr(task.source_path, '/');
        filename = filename ? filename + 1 : task.source_path;

        pthread_mutex_lock(log_mutex);
        fprintf(logfile,
            "[%s] [%s@%s:%d] [%s@%s:%d] [%d] [PULL] [ERROR] [File: %s - %s]\n",
            timestamp,
            source_no_ext, task.source_host, task.source_port,
            target_no_ext, task.target_host, task.target_port,
            pid, filename, strerror(pull_errno)
        );
        fflush(logfile);
        pthread_mutex_unlock(log_mutex);
    }
}



/*int send_list_and_process(sync_info_mem *entry, FILE *logfile) {
    int sockfd = connect_to_client(entry->source_host, entry->source_port);
    if (sockfd < 0) {
        fprintf(logfile, "Failed to connect to %s:%d\n", entry->source_host, entry->source_port);
        return -1;
    }

    char command[1024];
    snprintf(command, sizeof(command), "LIST %s\n", entry->source_dir);
    if (send(sockfd, command, strlen(command), 0) < 0) {
        perror("send");
        close(sockfd);
        return -1;
    }

    FILE *sock_stream = fdopen(sockfd, "r");
    if (!sock_stream) {
        perror("fdopen");
        close(sockfd);
        return -1;
    }

    char line[1024];
    while (fgets(line, sizeof(line), sock_stream)) {
        line[strcspn(line, "\n")] = '\0';
        if (strcmp(line, ".") == 0) break;

        sync_task task;
        if (snprintf(task.source_path, MAX_PATH_LENGTH, "%s/%s", entry->source_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated source path\n");
            continue;
        }

        if (snprintf(task.target_path, MAX_PATH_LENGTH, "%s/%s", entry->target_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated target path\n");
            continue;
        }

        strncpy(task.source_host, entry->source_host, MAX_HOST_LENGTH);
        strncpy(task.target_host, entry->target_host, MAX_HOST_LENGTH);
        task.source_port = entry->source_port;
        task.target_port = entry->target_port;
        task.parent_entry = entry;

        process_task_serially(task, logfile);
    }

    fclose(sock_stream);  
    return 0;
}

void send_list_and_process_all(sync_info_mem_store *store, FILE *logfile) {
    sync_info_mem *curr = store->head;
    while (curr) {
        send_list_and_process(curr, logfile);
        curr = curr->next;
    }
}*/



char *strip_extension(const char *path) {
    char *path_copy = strdup(path);  
    if (!path_copy) return NULL;

    char *basename = strrchr(path_copy, '/');
    basename = basename ? basename + 1 : path_copy;

    char *dot = strrchr(basename, '.');
    if (dot) {
        *dot = '\0';  
    }

    return path_copy;
}
