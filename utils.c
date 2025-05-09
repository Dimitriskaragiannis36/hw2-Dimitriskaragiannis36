#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <time.h>


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

    int bytes = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) {
        close(client_sock);
        return 0;
    }
    buffer[bytes] = '\0';

    char timestamp[64];
    time_t now = time(NULL);
    strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

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

                    snprintf(response, sizeof(response), "%s Added: %s@%s:%d -> %s@%s:%d\n",
                            timestamp, src_dir, src_host, src_port,
                            dst_dir, dst_host, dst_port);
                    fprintf(logfile, "%s Added: %s@%s:%d -> %s@%s:%d\n",
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

    send(client_sock, response, strlen(response), 0);
    close(client_sock);
    return shutdown_requested;
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

void command_loop(int sockfd_unused, FILE *logfile) {
    char line[MAX_LINE];
    char host_ip[64];
    int host_port;

    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    getpeername(sockfd_unused, (struct sockaddr *)&addr, &len);
    inet_ntop(AF_INET, &addr.sin_addr, host_ip, sizeof(host_ip));
    host_port = ntohs(addr.sin_port);
    close(sockfd_unused);  

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
            char src[MAX_LINE], dst[MAX_LINE];
            if (sscanf(line + 4, "%s %s", src, dst) == 2) {
                fprintf(logfile, "%s Command add %s -> %s\n", timestamp, src, dst);
            } else {
                fprintf(logfile, "%s Command add (invalid format)\n", timestamp);
            }
        } else if (strncmp(line, "cancel ", 7) == 0) {
            char full_path[MAX_LINE];
            if (sscanf(line + 7, "%s", full_path) == 1) {
                char display_path[MAX_LINE];
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

        int sockfd = create_socket(host_ip, host_port);
        if (send(sockfd, line, strlen(line), 0) < 0) {
            perror("send");
            close(sockfd);
            break;
        }

        char response[MAX_LINE * 2];
        int n = recv(sockfd, response, sizeof(response) - 1, 0);
        if (n <= 0) {
            perror("recv");
            close(sockfd);
            break;
        }
        response[n] = '\0';
        close(sockfd);

        printf("%s", response);

        if (strncmp(line, "shutdown", 8) == 0)
            break;
    }
}

