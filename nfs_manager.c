#include "utils.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

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
                manager_logfile = fopen(optarg, "a");
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

    fprintf(manager_logfile, "[INFO] NFS Manager started with config: workers=%d, port=%d, buffer=%d\n",
            worker_limit, port_number, bufferSize);
    fflush(manager_logfile);

    sync_info_mem_store store;
    store.head = NULL;
    store.size = 0;

    read_config_file(config_file, manager_logfile, &store);

    int server_sock = create_server_socket(port_number);
    fprintf(manager_logfile, "[INFO] Listening on port %d...\n", port_number);
    fflush(manager_logfile);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("accept");
            continue;
        }

        handle_client_command(client_sock, manager_logfile, &store);
    }

    free_sync_info_store(&store);
    fclose(manager_logfile);
    free(config_file);

    return 0;
}
