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

    task_queue queue;
    init_task_queue(&queue, 100);  

    pthread_t workers[worker_limit];
    for (int i = 0; i < worker_limit; ++i) {
        pthread_create(&workers[i], NULL, worker_thread, (void*)&queue);
    }

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

            sync_task task;
            strncpy(task.source_host, curr->source_host, MAX_HOST_LENGTH);
            task.source_port = curr->source_port;
            strncpy(task.source_path, source_path, MAX_PATH_LENGTH);

            strncpy(task.target_host, curr->target_host, MAX_HOST_LENGTH);
            task.target_port = curr->target_port;
            strncpy(task.target_path, target_path, MAX_PATH_LENGTH);

            task.parent_entry = curr;

            enqueue_task(&queue, &task);
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

    for (int i = 0; i < worker_limit; ++i) {
    pthread_cancel(workers[i]);  
    pthread_join(workers[i], NULL);
    }
    destroy_task_queue(&queue);

    free_sync_info_store(&store);
    fclose(manager_logfile);
    free(config_file);

    return 0;
}
