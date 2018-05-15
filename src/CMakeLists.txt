set(sources
      aggregation.c  debug.c        gmt_malloc.c  gmt_put_get.c   main.c    network.c    uthread.c
      comm_server.c  gmt_execute.c  gmt_misc.c    gmt_ucontext.c  memory.c  profiling.c  utils.c
      config.c       gmt_for.c      helper.c      mtask.c   timing.c     worker.c
)

add_library(gmt ${sources})
target_link_libraries(gmt ${MPI_LIBRARIES})
install(TARGETS gmt DESTINATION lib)
