#ο compiler
CC = gcc

#οι σημαίες για τα warnings, debugs
CFLAGS = -Wall -g

#make βασικός στόχος
all: nfs_manager nfs_console nfs_client

#manager με link στο utils
nfs_manager: nfs_manager.c utils.c
	$(CC) $(CFLAGS) -o nfs_manager nfs_manager.c utils.c

#console με link στο utils
nfs_console: nfs_console.c utils.c
	$(CC) $(CFLAGS) -o nfs_console nfs_console.c utils.c

#client με link στο utils
nfs_client: nfs_client.c utils.c
	$(CC) $(CFLAGS) -o nfs_client nfs_client.c utils.c

#εκτέλεση manager με στανταρ ορίσματα + -n 5 (worker_limit)
run: nfs_manager
	./nfs_manager -l manager_logfile -c config_file -n 5 -p 5000 -b 1024 

#εκτέλεση console με στανταρ όρισμα
console: nfs_console
	./nfs_console -l console_logfile -h 127.0.0.1 -p 5000

#καθαρισμός όλων των binaries και log_files
clean:
	rm -f nfs_manager nfs_console nfs_client *.o manager_logfile console_logfile