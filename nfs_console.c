#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

int main(int argc, char *argv[]) {
    FILE *logfile = NULL;
    char *host_ip = NULL;
    int host_port = 0;

    int opt;
    while ((opt = getopt(argc, argv, "l:h:p:")) != -1) {
        switch (opt) {
            case 'l':
                logfile = fopen(optarg, "a");
                if (!logfile) {
                    perror("fopen logfile");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'h':
                host_ip = strdup(optarg);
                break;
            case 'p':
                host_port = atoi(optarg);
                break;
            default:
                usage_c(argv[0]);
        }
    }

    if (!logfile || !host_ip || host_port <= 0) {
        usage_c(argv[0]);
    }

    int sockfd = create_socket(host_ip, host_port);

    command_loop(sockfd, logfile);

    fclose(logfile);
    free(host_ip);
    close(sockfd);

    return 0;
}