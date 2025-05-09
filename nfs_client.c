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
    if (argc == 2) {
        port = atoi(argv[1]);
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
