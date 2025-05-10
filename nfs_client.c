#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "utils.h"

#define PORT 8080  
#define MAX_LINE 1024

int main(int argc, char *argv[]) {
     int port = PORT; 
    int opt;

    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
            case 'p':  
                port = atoi(optarg);  
                break;
            default:
                fprintf(stderr, "Usage: %s [-p port_number]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    int server_fd = start_server_socket(port);
    printf("nfs_client listening on port %d...\n", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }
        handle_client(client_fd);  
        close(client_fd);
    }

    close(server_fd);
    return 0;
}
