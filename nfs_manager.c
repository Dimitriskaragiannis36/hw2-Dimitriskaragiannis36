#include "utils.h" //η βιβλιοθήκη που έφτιαξα
#include <stdlib.h> //για το atoi, free
#include <string.h> //για το strdup
#include <unistd.h> //για το getopt
#include <getopt.h> //getopt για parsing παραμέτρων
#include <sys/socket.h> //για accept
#include <arpa/inet.h> //για πληρότητα και αποφυγή απροσδόκητης συμπεριφοράς
#include <netinet/in.h> //για struct sockaddr_in
#include <pthread.h> //για pthread_create κτλ

volatile int shutting_down = 0; //global flag για threads

int main(int argc, char *argv[]) {
    FILE *manager_logfile = NULL; 
    char *config_file = NULL;
    int worker_limit = 5;
    int port_number = 0;
    int bufferSize = 0;

    //parsing παραμέτρων γραμμής εντολών
    int opt;
    while ((opt = getopt(argc, argv, "l:c:n:p:b:")) != -1) {
        switch (opt) {
            case 'l':
                manager_logfile = fopen(optarg, "w"); //write
                if (!manager_logfile) {
                    perror("fopen log");
                    exit(EXIT_FAILURE);
                }
                global_log_fp = manager_logfile; //ορισμός global pointer
                break;
            case 'c':
                config_file = strdup(optarg); //string duplicate
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
                usage_m(argv[0]); //κλήση συνάρτησης σφάλματος
        }
    }
    //έλεγχος παραμέτρων γραμμής εντολών
    if (!manager_logfile || !config_file || worker_limit <= 0 || port_number <= 0 || bufferSize <= 0) {
        usage_m(argv[0]);
    }
    //αρχικοποίηση δομής αποθήκης συγχρονισμού 
    sync_info_mem_store store;
    store.head = NULL;
    store.size = 0;

    //κλήση συνάρτησης για διάβασμα από το config
    read_config_file(config_file, manager_logfile, &store);

    //αρχικοποίηση ουράς εργασιών
    task_queue queue;
    init_task_queue(&queue, bufferSize);  

    //δημιουργία worker threads
    pthread_t workers[worker_limit];
    pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
    worker_args args[worker_limit];
    for (int i = 0; i < worker_limit; ++i) {
        args[i].id = i;
        args[i].queue = &queue;
        args[i].log_fp = manager_logfile;
        args[i].log_mutex = &log_mutex;
        pthread_create(&workers[i], NULL, worker_thread, &args[i]);
    }

    //κλήση συνάρτησης για αρχική λίστα εργασιών και εισαγωγή τους στην ουρά
    send_list_and_enqueue_tasks(&store, &queue, manager_logfile);

    //δημιουργία socket server
    int server_sock = create_server_socket(port_number);
    
    //δομή όπως στις διαφάνειες
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        //αναμονή για αποδοχή
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("accept");
            continue;
        }

        //κλήση συνάρτησης για εντολές από console
        int shutdown_requested = handle_command(client_sock, manager_logfile, &store, &log_mutex);
        if (shutdown_requested) { //αν επιστρέψει shutdown
            shutting_down = 1;

            //κλέιδωμα - ξύπνημα όλων -ξεκλείδωμα
            pthread_mutex_lock(&queue.mutex);
            pthread_cond_broadcast(&queue.not_empty); 
            pthread_mutex_unlock(&queue.mutex);

            //επαναληπτικά τα μαζέυουμε όλα
            for (int i = 0; i < worker_limit; ++i) {
                pthread_join(workers[i], NULL);
            }
            //καθαρίζουμε την ουρά εργασιών
            destroy_task_queue(&queue);
            break;
        }
    }

    //απελευθέρωση μνήμης και πόρων
    free_sync_info_store(&store);
    fclose(manager_logfile);
    free(config_file);

    return 0;
}
