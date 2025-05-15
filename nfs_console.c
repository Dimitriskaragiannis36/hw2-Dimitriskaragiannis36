#include "utils.h" //η βιβλιοθήκη που έφτιαξα
#include <stdio.h> //για fopen
#include <stdlib.h>  //για free
#include <string.h> //για strdup
#include <unistd.h>  //για getopt
#include <getopt.h> //getopt για parsing παραμέτρων
#include <arpa/inet.h> //για πληρότητα και αποφυγή απροσδόκητης συμπεριφοράς
#include <netinet/in.h>  //για struct sockaddr_in
#include <sys/socket.h> //για create_socket

volatile int shutting_down = 0; //global flag για threads
//αν δεν έμπαινε γκρίνιαζε ο gcc

int main(int argc, char *argv[]) {
    FILE *logfile = NULL;
    char *host_ip = NULL;
    int host_port = 0;

    //parsing παραμέτρων γραμμής εντολών
    int opt;
    while ((opt = getopt(argc, argv, "l:h:p:")) != -1) {
        switch (opt) {
            case 'l':
                logfile = fopen(optarg, "w");
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
                usage_c(argv[0]); //κλήση συνάρτησης σφάλματος
        }
    }

    //έλεγχος παραμέτρων γραμμής εντολών
    if (!logfile || !host_ip || host_port <= 0) {
        usage_c(argv[0]);  //κλήση συνάρτησης σφάλματος
    }

    //δημιουργία και σύνδεση TCP socket προς τον server
    int sockfd = create_socket(host_ip, host_port);

    //κλήση συνάρτησης εντολών από χρήστη
    command_loop(sockfd, logfile);

    //καθαρισμός πόρων
    fclose(logfile);
    free(host_ip);
    close(sockfd);

    return 0;
}