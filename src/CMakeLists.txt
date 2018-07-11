set(sources
      aggregation.c  debug.c        gmt_malloc.c  gmt_put_get.c   main.c    network.c    uthread.c
      comm_server.c  gmt_execute.c  gmt_misc.c    gmt_ucontext.c  memory.c  profiling.c  utils.c
      config.c       gmt_for.c      helper.c      mtask.c   timing.c     worker.c
      scheduler.c    dta.c
)
set_source_files_properties(${sources} PROPERTIES LANGUAGE CXX )

find_package(Threads REQUIRED)
include_directories(${THREADS_PTHREADS_INCLUDE_DIR})

add_library(gmt ${sources})
set_target_properties(gmt PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(gmt ${MPI_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS gmt DESTINATION lib)
