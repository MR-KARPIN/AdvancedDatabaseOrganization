CC = gcc
CFLAGS = -Wall -g

# Source files
SRCS = buffer_mgr.c buffer_mgr_stat.c dberror.c storage_mgr.c record_mgr.c expr.c rm_serializer.c btree_mgr.c # Adjusted name here

# Header files
HDRS = buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h test_helper.h storage_mgr.h record_mgr.h expr.h btree_mgr.h

# Object files
OBJS = buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o record_mgr.o expr.o rm_serializer.o btree_mgr.o # Adjusted object file here

# Executables
EXEC1 = test_assign4_1

# Compile rules
all: $(EXEC1)

# Rule to build test_assign4_1
$(EXEC1): $(OBJS) test_assign4_1.o
	$(CC) $(CFLAGS) -o $(EXEC1) $(OBJS) test_assign4_1.o

# Compile object files for test_assign4_1
test_assign4_1.o: test_assign4_1.c $(HDRS)
	$(CC) $(CFLAGS) -c test_assign4_1.c


# Compile object files for buffer_mgr
buffer_mgr.o: buffer_mgr.c buffer_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c buffer_mgr.c

# Compile object files for buffer_mgr_stat
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h $(HDRS)
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

# Compile object files for dberror
dberror.o: dberror.c dberror.h $(HDRS)
	$(CC) $(CFLAGS) -c dberror.c

# Compile object files for storage
storage_mgr.o: storage_mgr.c storage_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c storage_mgr.c

# Compile object files for btree_mgr
btree_mgr.o: btree_mgr.c btree_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c btree_mgr.c


# Compile object files for record
record.o: record_mgr.c record_mgr.h $(HDRS)
	$(CC) $(CFLAGS) -c record_mgr.c

# Clean rule to remove compiled files
clean:
	rm -f *.o $(EXEC1) $(EXEC2)
