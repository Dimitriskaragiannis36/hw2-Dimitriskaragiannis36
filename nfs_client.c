#include <stdio.h> //για printf
#include <stdlib.h> //για exit
#include <unistd.h> //για getopt
#include <netinet/in.h>  //για struct sockaddr_in
#include <arpa/inet.h> //για πληρότητα και αποφυγή απροσδόκητης συμπεριφοράς
#include <signal.h> //για signal
#include <errno.h> //για errno, EINTR
#include <sys/select.h> //για select
#include <pthread.h> //για pthread_create κτλ
#include "utils.h"  //η βιβλιοθήκη που έφτιαξα

#define PORT 8080  //default port αν δεν δώωθεί
#define MAX_LINE 1024

volatile sig_atomic_t running = 1; //για το signal handler
volatile int shutting_down = 0; //global flag για threads
//αν δεν έμπαινε γκρίνιαζε ο gcc

void handle_sigterm(int sig) {
    running = 0; //με SIGTERM ή SIGINT 
}

int main(int argc, char *argv[]) {
     int port = PORT; //η χρήση του default
    int opt;

    ////parsing παραμέτρων γραμμής εντολών
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

    //ρύθμιση signal handlers με kill και ctrl + c
    signal(SIGTERM, handle_sigterm);
    signal(SIGINT, handle_sigterm);

    //εκκίνηση socket server στη θύρα που καθορίστηκε
    int server_fd = start_server_socket(port);
    printf("nfs_client listening on port %d...\n", port);
    while (running) { //αναμονή για νέες συνδέσεις
        fd_set fds;
        FD_ZERO(&fds);  //αρχικοποίηση συνόλου fds
        FD_SET(server_fd, &fds); //παρακολουθούμε μόνο το server socket

        struct timeval timeout;  //timeout για select
        timeout.tv_sec = 1;     //1 δευτ
        timeout.tv_usec = 0;

        //νέα σύνδεση (non-blocking με timeout)
        int ret = select(server_fd + 1, &fds, NULL, NULL, &timeout);
        if (ret < 0) {
            if (errno == EINTR) continue; //αν διακοπεί, επανεκκίνηση
            perror("select");
            break;
        }

        if (ret == 0) continue;  //αν timeout, επανεκκίνηση loop
        
        //αν νέα σύνδεση
        if (FD_ISSET(server_fd, &fds)) {
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
            if (client_fd < 0) {
                if (errno == EINTR) continue; //αν διακόπηκε από σήμα
                perror("accept");
                continue;
            }
            //δημιουργία thread για εξυπηρέτηση client
            pthread_t tid;
            int *fd_ptr = malloc(sizeof(int));
            *fd_ptr = client_fd;
            pthread_create(&tid, NULL, (void *(*)(void *))handle_client_thread, fd_ptr);
            pthread_detach(tid); //δεν περιμένουμε την ολοκλήρωση του
        }
    }

    //κλείσιμο server socket και καθαρισμός πόρων
    close(server_fd);
    printf("nfs_client shutting down.\n");
    return 0;
}
