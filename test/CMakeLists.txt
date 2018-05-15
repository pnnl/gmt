set(indv_tests 
    test_alloc.c
    test_atomic.c
    test_execute.c
    test_execute_on_node.c
    test_execute_with_handle.c
    test_file_write.c
    test_for_each.c
    test_for_loop.c
    test_for_loop_nested.c
    test_for_loop_whandle.c
    test_get.c
    test_get_replica.c
    test_local_ptr.c
    test_memcpy.c
    #test_os_memcpy.c
    test_put.c
    #test_spawn_at.c
    testUtils.c
    test_yield.c
) 

add_library(gmttestlib ${indv_tests})
set(LIBS )
foreach(indv_test ${indv_tests})
  get_filename_component(test_name ${indv_test} NAME_WE)
  set(LIBS ${LIBS} ${test_name})
endforeach()

set(test_pars 
    alloc
    atomic
    execute
    execute_on_node
    execute_with_handle
    file_write
    for_each
    for_loop
    for_loop_nested
    for_loop_whandle
    get
    get_replica
    local_ptr
    memcpy
    put
    yield
)

add_executable(gmttest main.c)
target_link_libraries(gmttest gmttestlib gmt ${MPI_LIBRARIES})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/run_test.sh ${CMAKE_CURRENT_BINARY_DIR}/run_test.sh COPYONLY)
