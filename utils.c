#include "utils.h"
#include <stdlib.h>
#include <string.h>

void usage(const char *prog_name) {
    fprintf(stderr, "Usage: %s -l <manager_logfile> -c <config_file> -n <worker_limit> -p <port_number> -b <bufferSize>\n", prog_name);
    exit(EXIT_FAILURE);
}

void read_config_file(const char *filename, FILE *log_fp, sync_info_mem_store *store) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen config_file");
        exit(EXIT_FAILURE);
    }

    char src_entry[MAX_LINE_LENGTH];
    char tgt_entry[MAX_LINE_LENGTH];

    while (fscanf(fp, "%s %s", src_entry, tgt_entry) == 2) {
        sync_info_mem *info = malloc(sizeof(sync_info_mem));
        if (!info) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memset(info, 0, sizeof(sync_info_mem));

        char *at = strchr(src_entry, '@');
        char *colon = strchr(src_entry, ':');
        if (!at || !colon || at > colon) {
            fprintf(stderr, "Invalid source entry: %s\n", src_entry);
            free(info);
            continue;
        }
        *at = '\0';
        *colon = '\0';
        strncpy(info->source_dir, src_entry, MAX_DIR_LENGTH);
        strncpy(info->source_host, at + 1, MAX_HOST_LENGTH);
        info->source_port = atoi(colon + 1);

        at = strchr(tgt_entry, '@');
        colon = strchr(tgt_entry, ':');
        if (!at || !colon || at > colon) {
            fprintf(stderr, "Invalid target entry: %s\n", tgt_entry);
            free(info);
            continue;
        }
        *at = '\0';
        *colon = '\0';
        strncpy(info->target_dir, tgt_entry, MAX_DIR_LENGTH);
        strncpy(info->target_host, at + 1, MAX_HOST_LENGTH);
        info->target_port = atoi(colon + 1);

        info->active = 1;
        info->error_count = 0;
        info->last_sync_time = 0;

        fprintf(log_fp, "[CONFIG] %s@%s:%d -> %s@%s:%d\n",
                info->source_dir, info->source_host, info->source_port,
                info->target_dir, info->target_host, info->target_port);

        add_sync_info(store, info);
    }

    fclose(fp);
}


void add_sync_info(sync_info_mem_store *store, sync_info_mem *info) {
    info->next = store->head;
    store->head = info;
    store->size++;
}

sync_info_mem* find_sync_info(sync_info_mem_store *store, const char *source_dir) {
    sync_info_mem *current = store->head;
    while (current) {
        if (strcmp(current->source_dir, source_dir) == 0) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}

void free_sync_info_store(sync_info_mem_store *store) {
    sync_info_mem *current = store->head;
    while (current) {
        sync_info_mem *next = current->next;
        free(current);
        current = next;
    }
    store->head = NULL;
    store->size = 0;
}

