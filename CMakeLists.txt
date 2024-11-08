cmake_minimum_required(VERSION 3.29)
project(AdvancedDatabaseOrganization C)

set(CMAKE_C_STANDARD 11)

include_directories(assign3)

add_executable(AdvancedDatabaseOrganization
    assign3/buffer_mgr.c
    assign3/buffer_mgr.h
    assign3/buffer_mgr_stat.c
    assign3/buffer_mgr_stat.h
    assign3/dberror.c
    assign3/dberror.h
    assign3/dt.h
    assign3/expr.c
    assign3/expr.h
    assign3/record_mgr.c
    assign3/record_mgr.h
    assign3/rm_serializer.c
    assign3/storage_mgr.c
    assign3/storage_mgr.h
    assign3/tables.h
    assign3/test_assign3_1.c
    assign3/test_helper.h)
