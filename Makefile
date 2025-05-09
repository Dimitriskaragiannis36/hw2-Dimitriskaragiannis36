CC = gcc
CFLAGS = -Wall -Wextra -g

OBJS = utils.o

all: nfs_manager nfs_console

nfs_manager: nfs_manager.o $(OBJS)
	$(CC) $(CFLAGS) -o nfs_manager nfs_manager.o $(OBJS)

nfs_console: nfs_console.o $(OBJS)
	$(CC) $(CFLAGS) -o nfs_console nfs_console.o $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o manager console
