#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>
#include <sys/select.h>
#include <pthread.h>
#include "utils.h"

#define PORT 8080  
#define MAX_LINE 1024

volatile sig_atomic_t running = 1;
volatile int shutting_down = 0;

void handle_sigterm(int sig) {
    running = 0;
}

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

    signal(SIGTERM, handle_sigterm);
    signal(SIGINT, handle_sigterm);

    int server_fd = start_server_socket(port);
    printf("nfs_client listening on port %d...\n", port);
    while (running) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(server_fd, &fds);

        struct timeval timeout;
        timeout.tv_sec = 1;  
        timeout.tv_usec = 0;

        int ret = select(server_fd + 1, &fds, NULL, NULL, &timeout);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        if (ret == 0) continue;  

        if (FD_ISSET(server_fd, &fds)) {
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
            if (client_fd < 0) {
                if (errno == EINTR) continue;
                perror("accept");
                continue;
            }
            pthread_t tid;
            int *fd_ptr = malloc(sizeof(int));
            *fd_ptr = client_fd;
            pthread_create(&tid, NULL, (void *(*)(void *))handle_client_thread, fd_ptr);
            pthread_detach(tid);
        }
    }

    close(server_fd);
    printf("nfs_client shutting down.\n");
    return 0;
}
