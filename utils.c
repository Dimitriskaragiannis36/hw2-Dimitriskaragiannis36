#include "utils.h" //η βιβλιοθήκη που έφτιαξα
#include <stdlib.h> //για free
#include <string.h> //για strcpy
#include <sys/socket.h> //για socket
#include <arpa/inet.h> //για htons
#include <netinet/in.h>  //για struct sockaddr_in
#include <unistd.h> //για read, write
#include <time.h> //για strftime
#include <fcntl.h> //για O_APPEND
#include <dirent.h> //για opendir
#include <pthread.h> //για pthread_mutex_t
#include <errno.h> //για errno, EINTR
#include <sys/stat.h>  //για stat

#define CHUNK_SIZE 1024

FILE *global_log_fp = NULL; //αρχικοποίηση global pointer του manager_logfile
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER; //αρχικοποίηση mytex


/*--------------------------------------------   NFS_MANAGER  -----------------------------------------------*/
//συνάρτηση σφάλματος
void usage_m(const char *prog_name) {
    fprintf(stderr, "Usage: %s -l <manager_logfile> -c <config_file> -n <worker_limit> -p <port_number> -b <bufferSize>\n", prog_name);
    exit(EXIT_FAILURE);
}

//συνάρτηση για διάβασμα από config
void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen config_file");
        exit(EXIT_FAILURE);
    }

    char src_entry[MAX_LINE_LENGTH];
    char tgt_entry[MAX_LINE_LENGTH];

    //διάβασμα κάθε γραμμής που περιέχει ζεύγη source και target
    while (fscanf(fp, "%s %s", src_entry, tgt_entry) == 2) {
        sync_info_mem *info = malloc(sizeof(sync_info_mem));
        if (!info) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memset(info, 0, sizeof(sync_info_mem)); //μηδενισμός πεδίων

        //ανάλυση source entry 
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

        //ανάλυση target entry 
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

        add_sync_info(store, info); //κλήση συνάρτησης για προσθήκη στη λίστα
    }

    fclose(fp);
}

//συνάρτηση διαχείρισης εντολής από console
int handle_command(int client_sock, FILE *logfile, sync_info_mem_store *store, pthread_mutex_t *log_mutex) {
    char buffer[MAX_LINE_LENGTH];
    char response[MAX_LINE_LENGTH];
    int shutdown_requested = 0;

    while (1) {
        //ανάγνωση εντολής από socket
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

        buffer[bytes] = '\0';  //null-terminate

        char timestamp[64];
        time_t now = time(NULL);
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

        response[0] = '\0';  //καθαρισμός response

        //εντολή shutdown
        if (strncmp(buffer, "shutdown", 8) == 0) {
            snprintf(response, sizeof(response),
                     "%s Shutting down manager...\n"
                     "%s Waiting for all active workers to finish.\n"
                     "%s Processing remaining queued tasks.\n"
                     "%s Manager shutdown complete\n",
                     timestamp, timestamp, timestamp, timestamp);
            printf("%s", response);
            fflush(stdout);

            shutdown_requested = 1;
        }

        //εντολή add <source> <target>
        else if (strncmp(buffer, "add ", 4) == 0) {
            char src_full[MAX_LINE_LENGTH], dst_full[MAX_LINE_LENGTH];
            if (sscanf(buffer + 4, "%1023s %1023s", src_full, dst_full) == 2) {
                char src_dir[MAX_DIR_LENGTH], src_host[MAX_HOST_LENGTH];
                int src_port;
                char dst_dir[MAX_DIR_LENGTH], dst_host[MAX_HOST_LENGTH];
                int dst_port;

                //parsing source
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

                // parsing target
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

                //έλεγχος αν υπάρχει ήδη το source
                sync_info_mem *existing_info = find_sync_info(store, src_dir);
                if (existing_info && existing_info->active) {
                    snprintf(response, sizeof(response), "%s Already in queue: %.900s\n", timestamp, src_dir);
                    printf("%s", response);
                    fflush(stdout);
                } else {
                    //δημιουργία νέου sync entry
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

                        add_sync_info(store, info); //κλήση συνάρτησης προσθήκης

                        //ανίχνευση αρχείων στον source κατάλογο
                        DIR *dir = opendir(strip_leading_slash(src_dir));
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

                                    //κατασκευή source/target path για log
                                    char log_src_path[MAX_PATH_LENGTH];
                                    char log_dst_path[MAX_PATH_LENGTH];
                                    snprintf(log_src_path, sizeof(log_src_path), "%s/%s", src_dir, filename_no_ext);
                                    snprintf(log_dst_path, sizeof(log_dst_path), "%s/%s", dst_dir, filename_no_ext);

                                    fprintf(logfile, "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                                            timestamp, log_src_path, src_host, src_port,
                                            log_dst_path, dst_host, dst_port);
                                    fflush(logfile);
                                    
                                    //εκτύπωση στην οθόνη
                                    char line[512];
                                    int len = snprintf(line, sizeof(line), "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                                                    timestamp, log_src_path, src_host, src_port,
                                                    log_dst_path, dst_host, dst_port);

                                    if (len >= sizeof(line)) {
                                        fprintf(stderr, "Warning: log line truncated\n");
                                        line[sizeof(line) - 2] = '\n';  
                                        line[sizeof(line) - 1] = '\0';  
                                    }
                                    fputs(line, stdout);
                                    fflush(stdout);
                                    strncat(response, line, sizeof(response) - strlen(response) - 1);
                                    
                                    //δημιουργία και επεξεργασία του sync task
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

                        }
                    } else {
                        snprintf(response, sizeof(response), "%s Error allocating memory.\n", timestamp);
                    }
                }
            } else {
                snprintf(response, sizeof(response), "%s Invalid add command format.\n", timestamp);
            }
        }
        //εντολή cancel <source>
        else if (strncmp(buffer, "cancel ", 7) == 0) {
            char src[256];
            if (sscanf(buffer + 7, "%1023s", src) == 1) {
                sync_info_mem *curr = store->head;
                int any_active_found = 0;
                int any_inactive_found = 0;
                response[0] = '\0';

                //προχωράει μέχρι να το βρει
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
                            printf("%s", msg);
                            fflush(stdout);
                        } else {
                            any_inactive_found = 1;

                            char msg[512];
                            snprintf(msg, sizeof(msg),
                                    "%s Directory not being synchronized: %s@%s:%d\n",
                                    timestamp, curr->source_dir, curr->source_host, curr->source_port);
                            strncat(response, msg, sizeof(response) - strlen(response) - 1);
                            printf("%s", msg);
                            fflush(stdout);
                        }
                    }
                    curr = curr->next;
                }

                //ακόμα και αν δεν έχει προστεθεί εξαρχής
                if (!any_active_found && !any_inactive_found) {
                    snprintf(response, sizeof(response),
                     "%s Directory not being synchronized: %s\n", timestamp, src);
                    printf("%s", response);
                    fflush(stdout);
                }

            } else {
                snprintf(response, sizeof(response), "%s Invalid cancel command format.\n", timestamp);
            }
        }

        else {
           snprintf(response, sizeof(response), "%s Unknown command: %.500s\n", timestamp, buffer);
        }

        send(client_sock, response, strlen(response), 0); //στέλνει στον console

        if (shutdown_requested)
            break;
    }

    
    close(client_sock);
    return shutdown_requested;
}

//συνάρτηση προσθήκης στην δομή
void add_sync_info(sync_info_mem_store *store, sync_info_mem *info) {
    info->next = store->head;
    store->head = info;
    store->size++;
}

//συνάρτηση αναζήτησης στην δομή
sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir) {
    sync_info_mem *current = store->head;
    while (current) { //όταν το βρει
        if (strcmp(current->source_dir, source_dir) == 0) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}

//συνάρτηση καθαρισμού δομής
void free_sync_info_store(sync_info_mem_store *store) {
    sync_info_mem *current = store->head;
    while (current) { //προχωρά στο επόμενο
        sync_info_mem *next = current->next;
        free(current);
        current = next;
    }
    store->head = NULL;
    store->size = 0;
}

//συνάρτηση δημιουργίας socket
int create_server_socket(int port) {
    //δημιουργία socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    //ρύθμιση για επαναχρησιμοποίηση του port
    int optval = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        perror("setsockopt");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    //ορισμός διεύθυνσης socket
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port); //μετατροπή σε network byte order
    addr.sin_addr.s_addr = INADDR_ANY; //σε οποιαδήποτε

    //σύνδεση socket
    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    //ακρόαση socket
    if (listen(sockfd, 5) < 0) {
        perror("listen");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

//συνάρτηση αποστολής LIST και εισαγωγής των εργασιών στην ουρά (βοηθητική)
void send_list_and_enqueue_tasks(sync_info_mem_store *store, task_queue *queue, FILE *logfile) {
    sync_info_mem *curr = store->head;
    while (curr) {
        send_list_and_enqueue(curr, queue, logfile);
        curr = curr->next;
    }
}

//συνάρτηση αποστολής LIST και εισαγωγής των εργασιών στην ουρά (κύρια)
int send_list_and_enqueue(sync_info_mem *entry, task_queue *queue, FILE *logfile) {
    //δημιουργία σύνδεσης με client
    int sockfd = connect_to_client(entry->source_host, entry->source_port);
    if (sockfd < 0) {
        fprintf(logfile, "Failed to connect to %s:%d\n", entry->source_host, entry->source_port);
        return -1;
    }

    //αποστολή εντολής LIST
    char command[1024];
    snprintf(command, sizeof(command), "LIST %s\n", entry->source_dir);
    if (send(sockfd, command, strlen(command), 0) < 0) {
        perror("send");
        close(sockfd);
        return -1;
    }

    //άνοιγμα stream για ανάγνωση της απάντησης
    FILE *sock_stream = fdopen(sockfd, "r");
    if (!sock_stream) {
        perror("fdopen");
        close(sockfd);
        return -1;
    }

    char line[1024];
    while (fgets(line, sizeof(line), sock_stream)) {
        line[strcspn(line, "\n")] = '\0'; //αφαίρεση newline
        if (strcmp(line, ".") == 0) break; //τέλος λίστας

        //σε περίπτωση cancel
        if (!entry->active) {
            fprintf(logfile, "Task skipped due to cancellation of %s\n", entry->source_dir);
            break;
        }

        //δημιουργία δομής εργασίας
        sync_task task;
        if (snprintf(task.source_path, MAX_PATH_LENGTH, "%s/%s", entry->source_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated source path\n");
            continue;
        }

        if (snprintf(task.target_path, MAX_PATH_LENGTH, "%s/%s", entry->target_dir, line) >= MAX_PATH_LENGTH) {
            fprintf(logfile, "snprintf truncated source path\n");
            continue;
        }

        //αντιγραφή υπόλοιπων πεδίων 
        strncpy(task.source_host, entry->source_host, MAX_HOST_LENGTH);
        strncpy(task.target_host, entry->target_host, MAX_HOST_LENGTH);
        task.source_port = entry->source_port;
        task.target_port = entry->target_port;
        task.parent_entry = entry;

        //προσθήκη εργασίας στην ουρά
        enqueue_task(queue, &task);
    }

    //κλείσιμο stream και socket
    fclose(sock_stream);  
    return 0;
}

//συνάρτηση σύνδεσης με τον client
int connect_to_client(const char *ip, int port) {
    //δημιουργία socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    //προετοιμασία δομής διεύθυνσης server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    //μετατροπή IP από string σε binary μορφή
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        return -1;
    }

    //σύνδεση στον server
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

//συνάρτηση εργασιών συγχρονισμού
void process_task_serially(sync_task task, FILE *logfile, pthread_mutex_t *log_mutex) {

    // χρονική σήμανση για log
    char timestamp[64];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    pthread_t thread_pid = pthread_self();
    //αφαίρεση της επέκτασης από το source και target path
    char *source_no_ext = strip_extension(task.source_path); 
    char *target_no_ext = strip_extension(task.target_path);
    int pull_errno = 0, push_errno = 0;
    int tmp_fd = -1, file_size = 0;
    //
    if (pull_file(task.source_host, task.source_port, task.source_path,
                      &tmp_fd, &file_size, &pull_errno) != 0) { 
        //PULL failed
        const char *filename = strrchr(task.source_path, '/');
        filename = filename ? filename + 1 : task.source_path;

        pthread_mutex_lock(log_mutex); //κλείδωμα mutex
        fprintf(logfile,
            "[%s] [%s@%s:%d] [%s@%s:%d] [%ld] [PULL] [ERROR] [File: %s - %s]\n",
            timestamp,
            source_no_ext, task.source_host, task.source_port,
            target_no_ext, task.target_host, task.target_port,
            (long)thread_pid, filename, strerror(pull_errno)
        );
        fflush(logfile);
        pthread_mutex_unlock(log_mutex); //ξεκλείδωμα mutex
        return;
    }

    //PULL success
    pthread_mutex_lock(log_mutex); //κλείδωμα mutex
    fprintf(logfile,
        "[%s] [%s@%s:%d] [%s@%s:%d] [%ld] [PULL] [SUCCESS] [%d bytes pulled]\n",
        timestamp,
        source_no_ext, task.source_host, task.source_port,
        target_no_ext, task.target_host, task.target_port,
        (long)thread_pid, file_size
    );
    fflush(logfile);
    pthread_mutex_unlock(log_mutex); //ξεκλείδωμα mutex
    //PUSH success
    if (push_file(task.target_host, task.target_port,
                          task.target_path, tmp_fd, file_size, &push_errno) == 0) {
        now = time(NULL);
        tm_info = localtime(&now);
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

        pthread_mutex_lock(log_mutex); //κλείδωμα mutex
        fprintf(logfile,
            "[%s] [%s@%s:%d] [%s@%s:%d] [%ld] [PUSH] [SUCCESS] [%d bytes pushed]\n",
            timestamp,
            source_no_ext, task.source_host, task.source_port,
            target_no_ext, task.target_host, task.target_port,
            (long)thread_pid, file_size
        );
        fflush(logfile);
        pthread_mutex_unlock(log_mutex); //ξεκλείδωμα mutex
    } else { //PUSH failed
        const char *filename = strrchr(task.target_path, '/');
        filename = filename ? filename + 1 : task.target_path;

        now = time(NULL);
        tm_info = localtime(&now);
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

        pthread_mutex_lock(log_mutex); //κλείδωμα mutex
        fprintf(logfile,
            "[%s] [%s@%s:%d] [%s@%s:%d] [%ld] [PUSH] [ERROR] [File: %s - %s]\n",
            timestamp,
            source_no_ext, task.source_host, task.source_port,
            target_no_ext, task.target_host, task.target_port,
            (long)thread_pid, filename, strerror(push_errno)
        );
        fflush(logfile);
        pthread_mutex_unlock(log_mutex); //ξεκλείδωμα mutex
    }

    close(tmp_fd);
}

//συνάρτηση υλοποίησης PULL
int pull_file(const char *host, int port, const char *filepath,
                      int *out_fd, int *out_size, int *out_errno) {
    int sockfd = -1;
    int fd = -1;
    //αρχικοποίηση των παραμέτρων εξόδου
    if (out_fd) *out_fd = -1;
    if (out_size) *out_size = 0;
    if (out_errno) *out_errno = 0;
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    //δημιουργία socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (out_errno) *out_errno = errno;
        return -1;
    }
    //ρύθμιση διεύθυνσης server
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        if (out_errno) *out_errno = errno;
        close(sockfd);
        return -1;
    }
    //σύνδεση στο socket
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        if (out_errno) *out_errno = errno;
        close(sockfd);
        return -1;
    }

    //στείλε την εντολή PULL
    char msg[1024];
    int msg_len = snprintf(msg, sizeof(msg), "PULL %s\n", filepath);
    if (msg_len < 0 || msg_len >= (int)sizeof(msg)) {
        if (out_errno) *out_errno = EINVAL;
        close(sockfd);
        return -1;
    }
    //στέλνει το μήνυμα
    if (write(sockfd, msg, msg_len) != msg_len) {
        if (out_errno) *out_errno = errno;
        close(sockfd);
        return -1;
    }

    //διάβασε το header
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
    //έλεγχος αν το header είναι έγκυρο
    int filesize = atoi(header);
    if (filesize <= 0) {
        if (out_errno) *out_errno = EIO;
        close(sockfd);
        return -1;
    }

    //δημιουργία προσωρινού αρχείου
    char tmp_path[] = "/tmp/nfs_pull_XXXXXX"; //το XXXXXX θα αντικατασταθεί
    fd = mkstemp(tmp_path);
    if (fd < 0) {
        if (out_errno) *out_errno = errno;
        close(sockfd);
        return -1;
    }
    unlink(tmp_path);  //να διαγραφεί μόλις κλείσει

    //λήψη δεδομένων και εγγραφή στο αρχείο
    char buf[4096];
    int total = 0;
    while (total < filesize) {
        int to_read = (filesize - total > (int)sizeof(buf)) ? sizeof(buf) : filesize - total;
        int n = recv(sockfd, buf, to_read, 0);
        if (n <= 0) {
            if (out_errno) *out_errno = (n == 0 ? ECONNRESET : errno);
            close(sockfd);
            close(fd);
            return -1;
        }
        
        int written = 0;
        while (written < n) { //στέλνει το buffer στο αρχείο
            int m = write(fd, buf + written, n - written);
            if (m <= 0) {
                if (out_errno) *out_errno = errno;
                close(sockfd);
                close(fd);
                return -1;
            }
            written += m;
        }

        total += n;
    }
    //θέτει το offset στο αρχείο
    if (lseek(fd, 0, SEEK_SET) < 0) { 
        if (out_errno) *out_errno = errno;
        close(sockfd);
        close(fd);
        return -1;
    }

    //επιτυχία
    if (out_fd) *out_fd = fd;
    if (out_size) *out_size = filesize;
    close(sockfd);
    return 0;
}

//συνάρτηση υλοποίησης PUSH
int push_file(const char *host, int port, const char *filepath,
                      int fd, int size, int *out_errno) {
    if (out_errno) *out_errno = 0;

    //δημιουργία socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (out_errno) *out_errno = errno;
        return -1;
    }
    //ρύθμιση διεύθυνσης server
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }
    //σύνδεση στο socket
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }

    //αποστολή εντολής PUSH
    char header[1024];
    int header_len = snprintf(header, sizeof(header), "PUSH %s %d ", filepath, size);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        close(sockfd);
        if (out_errno) *out_errno = EINVAL;
        return -1;
    }
    //στέλνουμε το header
    int total_sent = 0;
    while (total_sent < header_len) {
        int n = send(sockfd, header + total_sent, header_len - total_sent, 0);
        if (n <= 0) {
            int err = (n == 0) ? ECONNRESET : errno;
            close(sockfd);
            if (out_errno) *out_errno = err;
            return -1;
        }
        total_sent += n;
    }

    //αποστολή αρχείου σε chunks
    char buffer[CHUNK_SIZE];
    int remaining = size;

    while (remaining > 0) {
        int to_read = (remaining > CHUNK_SIZE) ? CHUNK_SIZE : remaining;
        ssize_t nread = read(fd, buffer, to_read);
        if (nread < 0) {
            int err = errno;
            close(sockfd);
            if (out_errno) *out_errno = err;
            return -1;
        }
        if (nread == 0) break; //EOF

        int sent = 0;
        while (sent < nread) {
            int n = send(sockfd, buffer + sent, nread - sent, 0);
            if (n <= 0) {
                int err = (n == 0) ? ECONNRESET : errno;
                close(sockfd);
                if (out_errno) *out_errno = err;
                return -1;
            }
            sent += n;
        }

        remaining -= nread;
    }

    //λήψη απάντησης από server
    int result_net;
    if (recv(sockfd, &result_net, sizeof(int), 0) != sizeof(int)) {
        int err = errno;
        close(sockfd);
        if (out_errno) *out_errno = err;
        return -1;
    }
    //μετατροπή από network byte order σε host byte order
    close(sockfd);
    int result = ntohl(result_net);
    if (result & 0x80000000) {
        if (out_errno) *out_errno = result & 0x7FFFFFFF;
        return -1;
    }

    return 0;
}

//συνάρτηση αρχικοποίησης ουράς εργασιών
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

//συνάρτηση καταστροφής ουράς εργασιών
void destroy_task_queue(task_queue *q) {
    free(q->tasks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
}

//συνάρτησης εισόδου στην ουρά εργασιών
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

//πολυνηματική συνάρτηση των workers
void* worker_thread(void *arg) {
    worker_args *args = (worker_args*) arg;
    task_queue *q = args->queue;

    while (1) {
        sync_task task;
        dequeue_task(q, &task);  //περιμένει σε άδεια ουρά 

        if (shutting_down && task.source_path[0] == '\0') {
            break; //τότε βγαίνει από το loop
        }

        //η τρέχουσα χρονική σήμανση
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char timestamp[64];
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", tm_info);

        //κλήση απαλοιφής κατάληξης για log
        char *stripped_source = strip_extension(task.source_path);
        char *stripped_target = strip_extension(task.target_path);

        pthread_mutex_lock(&log_mutex); //κλείδωμα για καταγραφή
        fprintf(args->log_fp, "%s Added file: %s@%s:%d -> %s@%s:%d\n",
                timestamp,
                stripped_source, task.source_host, task.source_port,
                stripped_target, task.target_host, task.target_port);
                    
        printf("%s Added file: %s@%s:%d -> %s@%s:%d\n",
               timestamp,
               stripped_source, task.source_host, task.source_port,
               stripped_target, task.target_host, task.target_port);
        pthread_mutex_unlock(&log_mutex); //ξεκλείδωμα για καταγραφή

        process_task_serially(task, args->log_fp, args->log_mutex);
        //κλήση συνάρτησης συγχρονισμού
    }

    return NULL;
}

//συνάρτηση εξόδου από την ουρά εργασιών
void dequeue_task(task_queue *q, sync_task *task_out) {
    pthread_mutex_lock(&q->mutex);
    //αν ουρά άδεια και όχι shutdown, περιμένουμε
    while (q->count == 0 && !shutting_down) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    //αν ουρά άδεια και shutdown
    if (shutting_down && q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        task_out->source_path[0] = '\0'; 
        return;
    }

    //παίρνω την εργασία από την αρχή
    *task_out = q->tasks[q->front];
    q->front = (q->front + 1) % q->capacity;
    q->count--;

    //ειδοποίηση άλλων νημάτων για χώρο στην ουρά
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

//συνάρτηση απαλοιφής κατάληξης
char *strip_extension(const char *path) {
    char *path_copy = strdup(path); //δημιουργία αντιγράφου 
    if (!path_copy) return NULL;

    //εντοπισμός του τελεύταίου / για να πάρω μόνο όνομα
    char *basename = strrchr(path_copy, '/');
    basename = basename ? basename + 1 : path_copy;

    //εντοπισμός του τελευταίου . 
    char *dot = strrchr(basename, '.');
    if (dot) {
        *dot = '\0';  //αφαίρεση του .
    }

    return path_copy;
}


/*--------------------------------------------   NFS_CONSOLE  -----------------------------------------------*/
//συνάρτηση σφάλματος
void usage_c(const char *progname) {
    fprintf(stderr, "Usage: %s -l <console-logfile> -h <host_IP> -p <host_port>\n", progname);
    exit(EXIT_FAILURE);
}

//συνάρτηση δημιουργίας socket
int create_socket(const char *host_ip, int host_port) {
    //δημιουργία socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    //ρύθμιση διεύθυνσης
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(host_port); //μετατροπή port σε network byte order

    //μετατροπή IP σε binary μορφή
    if (inet_pton(AF_INET, host_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
        exit(EXIT_FAILURE);
    }

    //σύνδεση
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

//συνάρτηση εντολής μέσω γραμμής εντολών
void command_loop(int sockfd, FILE *logfile) {
    char line[MAX_LINE_LENGTH]; //buffer
  
    while (1) {
        printf("> ");
        fflush(stdout);

        //ανάγνωση γραμής από τον χρήστη
        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\n")] = '\0';  

        //χρονική σήμανση
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char timestamp[64];
        strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", tm_info);

        //εντολή add
        if (strncmp(line, "add ", 4) == 0) {
            char src[MAX_LINE_LENGTH], dst[MAX_LINE_LENGTH];
            if (sscanf(line + 4, "%s %s", src, dst) == 2) {
                fprintf(logfile, "%s Command add %s -> %s\n", timestamp, src, dst);
            } else {
                fprintf(logfile, "%s Command add (invalid format)\n", timestamp);
            }
            //εντολή cancel
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
            //εντολή shutdown
        } else if (strncmp(line, "shutdown", 8) == 0) {
            fprintf(logfile, "%s Command shutdown\n", timestamp);
        } else { //άγνωστη εντολή
            fprintf(logfile, "%s Command %s\n", timestamp, line); 
        }

        fflush(logfile); //άμεση καταγραφή 

        //αποστολή εντολής
        if (send(sockfd, line, strlen(line), 0) < 0) {
            perror("send");
            break;
        }

        //απάντηση
        char response[MAX_LINE_LENGTH * 2];
        int n = recv(sockfd, response, sizeof(response) - 1, 0);
        if (n <= 0) {
            perror("recv");
            break;
        }
        response[n] = '\0';
        printf("%s", response); //εκτύπωση στην οθόνη

        //σε περίπτωση που δοθεί shutdown
        if (strncmp(line, "shutdown", 8) == 0)
            break;
    }
    close(sockfd);
}



/*--------------------------------------------   NFS_CLIENT  -----------------------------------------------*/
//συνάρτηση εκκίνησης socket
int start_server_socket(int port) {
    //δημιουργία TCP socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    //προετοιμασία διεύθυνσης
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    //δέσμευση
    if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    //ακρόαση
    if (listen(sockfd, 5) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

//πολυνηματική συνάρτηση διαχείρισης εντολών manager (βοηθητική)
void *handle_client_thread(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);

    handle_client(client_fd);
    close(client_fd);

    return NULL;
}

//συνάρτηση διαχείρισης εντολών manager (κύρια)
void handle_client(int client_fd) {
    char buffer[MAX_LINE_LENGTH]; //buffer
    //λήψη δεδομένων από το socket
    int bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) return;

    buffer[bytes] = '\0'; //τερματισμός string

    //εντολή LIST <dir_path>
    if (strncmp(buffer, "LIST ", 5) == 0) {
        char *dir_path = buffer + 5;
        dir_path[strcspn(dir_path, "\r\n")] = '\0'; //αφαίρεση newline

        //άνοιγμα καταλόγου και περικοπή
        DIR *dir = opendir(strip_leading_slash(dir_path)); 
        if (!dir) {
            perror("opendir");
            send(client_fd, ".\n", 2, 0); //σήμα τερματισμού
            return;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (entry->d_type == DT_REG) {  //μόνο κανονικά αρχεία
                send(client_fd, entry->d_name, strlen(entry->d_name), 0);
                send(client_fd, "\n", 1, 0);
            }
        }
        send(client_fd, ".\n", 2, 0);  //σήμα τερματισμού λίστας
        closedir(dir);
    }

    //εντολή PULL <filepath>
    else if (strncmp(buffer, "PULL ", 5) == 0) {
        char *filepath = buffer + 5;
        filepath[strcspn(filepath, "\n")] = '\0'; //αφαίρεση newline
        //κλήση συνάρτησης διαχείρισης PULL
        handle_pull(client_fd, strip_leading_slash(filepath));
    }

    //εντολή PUSH <filepath> <size> <data>
    else if (strncmp(buffer, "PUSH ", 5) == 0) {
        char filepath[1024];
        int chunk_size;

        //εντοπισμός ορίων μεταξύ filepath, chunk_size και data
        char *after_cmd = buffer + 5;
        char *first_space = strchr(after_cmd, ' ');
        if (!first_space) return;
        char *second_space = strchr(first_space + 1, ' ');
        if (!second_space) return;

        *second_space = '\0';   //τερματισμός chunk_size string

        //αντιγραφή filepath
        strncpy(filepath, after_cmd, first_space - after_cmd);
        filepath[first_space - after_cmd] = '\0';

        //μετατροπή chunk_size σε int
        chunk_size = atoi(first_space + 1);

        //εντοπισμός αρχής δεδομένων στο buffer
        char *data_start = second_space + 1;
        int data_in_buffer = bytes - (data_start - buffer); //πόσα δεδομένα διαβάσαμε ήδη

        //δέσμευση μνήμης για τα πλήρη δεδομένα
        char *chunk_data = malloc(chunk_size);
        if (!chunk_data) return;

        //αντιγραφή ήδη ληφθέντων δεδομένων
        memcpy(chunk_data, data_start, data_in_buffer);

        //αν δεν έχουμε όλο το chunk, συνεχίζουμε να διαβάζουμε
        int total_read = data_in_buffer;
        while (total_read < chunk_size) {
            int n = recv(client_fd, chunk_data + total_read, chunk_size - total_read, 0);
            if (n <= 0) {
                free(chunk_data);
                return;
            }
            total_read += n;
        }
        //κλήση συνάρτησης διαχείρισης PUSH
        handle_push(client_fd, strip_leading_slash(filepath), chunk_size, chunk_data);
        free(chunk_data);
    }
}

//συνάρτηση διαχειρισης εντολής PULL
int handle_pull(int client_fd, const char *filepath) {
    //άνοιγμα αρχείου για ανάγνωση
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        //περίπτωση αποτυχίας
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to open file: %s\n", strerror(errno));
        send(client_fd, msg, strlen(msg), 0);
        perror("open (PULL)");
        return -1;
    }

    //βρίσκω μέγεθος και πάω την ακίδα στο τέλος
    off_t filesize = lseek(fd, 0, SEEK_END);
    if (filesize < 0) {
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to stat file: %s\n", strerror(errno));
        close(fd);
        send(client_fd, msg, strlen(msg), 0);
        perror("lseek (SEEK_END)");
        return -1;
    }

    //επιστρέφω στην αρχή για ανάγνωση
    if (lseek(fd, 0, SEEK_SET) < 0) {
        char msg[512];
        snprintf(msg, sizeof(msg), "-1 Failed to rewind file: %s\n", strerror(errno));
        close(fd);
        send(client_fd, msg, strlen(msg), 0);
        perror("lseek (SEEK_SET)");
        return -1;
    }

    //αποστολή header με το μέγεθος του αρχείου
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%ld ", (long)filesize);
    if (send(client_fd, header, header_len, 0) != header_len) {
        perror("send (header)");
        close(fd);
        return -1;
    }

    //αποστολή περιεχομένων αρχείου σε chunks των 4096 bytes
    char buffer[4096];
    ssize_t n;
    while ((n = read(fd, buffer, sizeof(buffer))) > 0) {
        if (send(client_fd, buffer, n, 0) != n) {
            perror("send (file data)");
            close(fd);
            return -1;
        }
    }

    if (n < 0) { //έλεγχος για σφάλμα
        perror("read (file)");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

//συνάρτηση διαχειρισης εντολής PUSH
int handle_push(int client_fd, const char *filepath, int chunk_size, const char *data) {
    int fd;

    //αν -1 τότε δημιουργία/εκκίνηση νέου
    if (chunk_size == -1) {
        fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            int err = htonl(0x80000000 | errno); //κωδικοποίηση σφάλαμτος
            send(client_fd, &err, sizeof(int), 0);
            perror("open create");
            return -1;
        }
        close(fd);

        int ok = htonl(0); //επιτυχία
        send(client_fd, &ok, sizeof(int), 0);
        return 0;
    }

    //αν μηδέν τιποτα
    if (chunk_size == 0) {
        int ok = htonl(0);
        send(client_fd, &ok, sizeof(int), 0);
        return 0;
    }

    //γράφω με append
    fd = open(filepath, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd == -1) {
        int err = htonl(0x80000000 | errno);
        send(client_fd, &err, sizeof(int), 0);
        perror("open append");
        return -1;
    }

    //εγγραφή δεδομένων
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



/*--------------------------------------------   ΚΟΙΝΗ ΣΥΝΑΡΤΗΣΗ  -----------------------------------------------*/
//συνάρτηση απαλοιφής αρχικού / για relative path
const char* strip_leading_slash(const char* path) {
    if (path[0] == '/')
        return path + 1;
    return path;
}