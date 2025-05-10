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

int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store) {
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

                if (find_sync_info(store, src_dir)) {
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

                        snprintf(response, sizeof(response), "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                                 timestamp, src_dir, src_host, src_port,
                                 dst_dir, dst_host, dst_port);
                        fprintf(logfile, "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                                timestamp, src_dir, src_host, src_port,
                                dst_dir, dst_host, dst_port);
                        fflush(logfile);
                    } else {
                        snprintf(response, sizeof(response), "%s Error allocating memory.\n", timestamp);
                    }
                }
            } else {
                snprintf(response, sizeof(response), "%s Invalid add command format.\n", timestamp);
            }
        }

        else if (strncmp(buffer, "cancel ", 7) == 0) {
            char src[MAX_LINE_LENGTH];
            if (sscanf(buffer + 7, "%1023s", src) == 1) {
                sync_info_mem *curr = store->head;
                sync_info_mem *found = NULL;

                while (curr) {
                    if (strcmp(curr->source_dir, src) == 0) {
                        found = curr;
                        break;
                    }
                    curr = curr->next;
                }

                if (found) {
                    found->active = 0;
                    snprintf(response, sizeof(response), "%s Synchronization stopped for %s@%s:%d\n",
                             timestamp, found->source_dir, found->source_host, found->source_port);
                    fprintf(logfile, "%s Synchronization stopped for %s@%s:%d\n",
                            timestamp, found->source_dir, found->source_host, found->source_port);
                    fflush(logfile);
                } else {
                    snprintf(response, sizeof(response), "%s Directory not being synchronized: %.900s\n", timestamp, src);
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

int send_list_command(int sockfd, const char *source_dir, FILE *logfile, sync_info_mem *entry) {
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
}

int pull_file(const char *host, int port, const char *filepath, char **out_data, int *out_size) {
    int sockfd;
    struct sockaddr_in serv_addr;
    
    *out_data = NULL;
    *out_size = 0;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        return -1;
    }

    dprintf(sockfd, "PULL %s\n", filepath);

    int filesize;
    if (recv(sockfd, &filesize, sizeof(int), 0) != sizeof(int)) {
        close(sockfd);
        return -1;
    }

    filesize = ntohl(filesize);
    if (filesize <= 0) {
        close(sockfd);
        return -1;
    }

    char *data = malloc(filesize);
    if (!data) {
        close(sockfd);
        return -1;
    }

    int total_received = 0;
    while (total_received < filesize) {
        int n = recv(sockfd, data + total_received, filesize - total_received, 0);
        if (n <= 0) {
            free(data);
            close(sockfd);
            return -1;
        }
        total_received += n;
    }

    *out_data = data;
    *out_size = filesize;
    close(sockfd);
    return 0;
}

int push_file(const char *host, int port, const char *filepath, const char *data, int size) {
    int sockfd;
    struct sockaddr_in serv_addr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        return -1;
    }

    dprintf(sockfd, "PUSH %s\n", filepath);

    int net_size = htonl(size);
    if (send(sockfd, &net_size, sizeof(int), 0) != sizeof(int)) {
        close(sockfd);
        return -1;
    }

    int total_sent = 0;
    while (total_sent < size) {
        int n = send(sockfd, data + total_sent, size - total_sent, 0);
        if (n <= 0) {
            close(sockfd);
            return -1;
        }
        total_sent += n;
    }

    close(sockfd);
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
    while (q->count == 0) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    *task_out = q->tasks[q->front];
    q->front = (q->front + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

void *worker_thread(void *arg) {
    task_queue *queue = (task_queue *)arg;

    while (1) {
        sync_task task;
        dequeue_task(queue, &task); 

        char *file_data = NULL;
        int file_size = 0;

        if (pull_file(task.source_host, task.source_port, task.source_path, &file_data, &file_size) == 0) {
            // Κλείδωμα του mutex πριν την εγγραφή στο log
            pthread_mutex_lock(&log_mutex);
            fprintf(global_log_fp, "[%s] Pulled file: %s (%d bytes)\n",
                    task.parent_entry->source_dir, task.source_path, file_size);
            fflush(global_log_fp);
            pthread_mutex_unlock(&log_mutex);

            if (push_file(task.target_host, task.target_port, task.target_path, file_data, file_size) == 0) {
                pthread_mutex_lock(&log_mutex);
                fprintf(global_log_fp, "[%s] Pushed file: %s\n",
                        task.parent_entry->target_dir, task.target_path);
                fflush(global_log_fp);
                pthread_mutex_unlock(&log_mutex);
            } else {
                pthread_mutex_lock(&log_mutex);
                fprintf(global_log_fp, "[%s] Failed to push: %s\n",
                        task.parent_entry->target_dir, task.target_path);
                fflush(global_log_fp);
                pthread_mutex_unlock(&log_mutex);
            }
            free(file_data);
        } else {
            pthread_mutex_lock(&log_mutex);
            fprintf(global_log_fp, "[%s] Failed to pull: %s\n",
                    task.parent_entry->source_dir, task.source_path);
            fflush(global_log_fp);
            pthread_mutex_unlock(&log_mutex);
        }
    }

    return NULL;
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
        char *data = second_space + 1;

        handle_push(client_fd, filepath, chunk_size, data);
    }
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
    FILE *file = fopen(filepath, "rb");
    if (!file) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "-1 File not found or cannot open\n");
        send(client_fd, msg, strlen(msg), 0);
        return -1;
    }

    fseek(file, 0, SEEK_END);
    long filesize = ftell(file);
    rewind(file);

    char *buffer = malloc(filesize);
    if (!buffer) {
        fclose(file);
        char msg[] = "-1 Memory allocation failed\n";
        send(client_fd, msg, strlen(msg), 0);
        return -1;
    }

    fread(buffer, 1, filesize, file);
    fclose(file);

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%ld ", filesize);
    send(client_fd, header, header_len, 0);
    send(client_fd, buffer, filesize, 0);

    free(buffer);
    return 0;
}

int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data) {
    FILE *file;

    if (chunk_size == -1) {
        file = fopen(filepath, "wb");
        if (!file) {
            perror("fopen");
            return -1;
        }
        fclose(file); 
        return 0;
    }

    if (chunk_size == 0) {
        return 0; 
    }

    file = fopen(filepath, "ab");
    if (!file) {
        perror("fopen append");
        return -1;
    }

    fwrite(data, 1, chunk_size, file);
    fflush(file);
    fclose(file);

    return 0;
}



