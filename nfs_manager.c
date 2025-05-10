#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

int main(int argc, char *argv[]) {
    FILE *manager_logfile = NULL;
    char *config_file = NULL;
    int worker_limit = 5;
    int port_number = 0;
    int bufferSize = 0;

    int opt;
    while ((opt = getopt(argc, argv, "l:c:n:p:b:")) != -1) {
        switch (opt) {
            case 'l':
                manager_logfile = fopen(optarg, "w");
                if (!manager_logfile) {
                    perror("fopen log");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'c':
                config_file = strdup(optarg);
                break;
            case 'n':
                worker_limit = atoi(optarg);
                break;
            case 'p':
                port_number = atoi(optarg);
                break;
            case 'b':
                bufferSize = atoi(optarg);
                break;
            default:
                usage_m(argv[0]);
        }
    }

    if (!manager_logfile || !config_file || worker_limit <= 0 || port_number <= 0 || bufferSize <= 0) {
        usage_m(argv[0]);
    }

    sync_info_mem_store store;
    store.head = NULL;
    store.size = 0;

    read_config_file(config_file, manager_logfile, &store);

    sync_info_mem *curr = store.head;
    while (curr) {
        int sockfd = connect_to_client(curr->source_host, curr->source_port);
        if (sockfd < 0) {
            fprintf(manager_logfile, "Failed to connect to %s:%d\n", curr->source_host, curr->source_port);
            fflush(manager_logfile);
            curr = curr->next;
            continue;
        }

        if (send_list_command(sockfd, curr->source_dir, manager_logfile, curr) < 0) {
            fprintf(manager_logfile, "Failed to send LIST command to %s:%d\n", curr->source_host, curr->source_port);
            fflush(manager_logfile);
            close(sockfd);
            curr = curr->next;
            continue;
        }

        FILE *sock_stream = fdopen(sockfd, "r");
        if (!sock_stream) {
            perror("fdopen");
            close(sockfd);
            curr = curr->next;
            continue;
        }

        char line[1024];
        while (fgets(line, sizeof(line), sock_stream)) {
            line[strcspn(line, "\n")] = '\0';
            if (strcmp(line, ".") == 0) break;

            char source_path[512], target_path[512];
            snprintf(source_path, sizeof(source_path), "%s/", curr->source_dir);
            strncat(source_path, line, sizeof(source_path) - strlen(source_path) - 1);

            snprintf(target_path, sizeof(target_path), "%s/", curr->target_dir);
            strncat(target_path, line, sizeof(target_path) - strlen(target_path) - 1);

            char *file_data = NULL;
            int file_size = 0;

            if (pull_file(curr->source_host, curr->source_port, source_path, &file_data, &file_size) == 0) {
                fprintf(manager_logfile, "[%s] Pulled file: %s (%d bytes)\n", curr->source_dir, source_path, file_size);
                if (push_file(curr->target_host, curr->target_port, target_path, file_data, file_size) == 0) {
                    fprintf(manager_logfile, "[%s] Pushed file: %s\n", curr->target_dir, target_path);
                } else {
                    fprintf(manager_logfile, "[%s] Failed to push: %s\n", curr->target_dir, target_path);
                }
                free(file_data);
            } else {
                fprintf(manager_logfile, "[%s] Failed to pull: %s\n", curr->source_dir, source_path);
            }
            fflush(manager_logfile);
        }

        fclose(sock_stream);
        curr = curr->next;
    }

    int server_sock = create_server_socket(port_number);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("accept");
            continue;
        }

        int shutdown_requested = handle_command(client_sock, manager_logfile, &store);
        if (shutdown_requested) {
            break;
        }
    }

    free_sync_info_store(&store);
    fclose(manager_logfile);
    free(config_file);

    return 0;
}
