#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>

volatile int shutting_down = 0;

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
                global_log_fp = manager_logfile; 
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

    //βάζω πολυνηματισμό
    task_queue queue;
    init_task_queue(&queue, bufferSize);  

    pthread_t workers[worker_limit];
    worker_args args[worker_limit];
    for (int i = 0; i < worker_limit; ++i) {
        args[i].id = i;
        args[i].queue = &queue;
        args[i].log_fp = manager_logfile;
        pthread_create(&workers[i], NULL, worker_thread, &args[i]);
    }

    send_list_and_enqueue_tasks(&store, &queue, manager_logfile);

    //send_list_and_process_all(&store, manager_logfile);

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
            shutting_down = 1;

            pthread_mutex_lock(&queue.mutex);
            pthread_cond_broadcast(&queue.not_empty); 
            pthread_mutex_unlock(&queue.mutex);

            for (int i = 0; i < worker_limit; ++i) {
                pthread_join(workers[i], NULL);
            }
            destroy_task_queue(&queue);
            break;
        }
    }

    free_sync_info_store(&store);
    fclose(manager_logfile);
    free(config_file);

    return 0;
}
