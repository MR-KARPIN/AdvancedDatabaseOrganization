# Compiler and flags
CC = -gcc
CFLAGS = -g -Wall

# Source files
SRCS = buffer_mgr.c buffer_mgr_stat.c dberror.c storage.c  # Adjusted name here

# Header files
HDRS = buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h test_helper.h storage_mgr.h

# Object files
OBJS = buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o  # Adjusted object file here

# Executables
EXEC1 = test_assign2_1
EXEC2 = test_assign2_2

# Compile rules
all: $(EXEC1) $(EXEC2)

# Rule to build test_assign2_1
$(EXEC1): $(OBJS) test_assign2_1.o
	$(CC) $(CFLAGS) -o $(EXEC1) $(OBJS) test_assign2_1.o

# Rule to build test_assign2_2
$(EXEC2): $(OBJS) test_assign2_2.o
	$(CC) $(CFLAGS) -o $(EXEC2) $(OBJS) test_assign2_2.o

# Compile object files for test_assign2_1
test_assign2_1.o: test_assign2_1.c $(HDRS)
	$(CC) $(CFLAGS) -c test_assign2_1.c

# Compile object files for test_assign2_2
test_assign2_2.o: test_assign2_2.c $(HDRS)
	$(CC) $(CFLAGS) -c test_assign2_2.c

# Compile object files for buffer_mgr
buffer_mgr.o: buffer_mgr.c buffer_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c buffer_mgr.c

# Compile object files for buffer_mgr_stat
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h $(HDRS)
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

# Compile object files for dberror
dberror.o: dberror.c dberror.h $(HDRS)
	$(CC) $(CFLAGS) -c dberror.c

# Compile object files for storage (or adjust accordingly)
storage.o: storage.c storage_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c storage.c

# Clean rule to remove compiled files
clean:
	rm -f *.o $(EXEC1) $(EXEC2)
