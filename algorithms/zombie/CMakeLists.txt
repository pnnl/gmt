set(indv_ccs bfs.cc tables.cc sort.cc convert.cc)
set_source_files_properties(${indv_ccs} PROPERTIES LANGUAGE CXX)
add_library(mylib ${indv_ccs})
set_target_properties(mylib PROPERTIES LINKER_LANGUAGE CXX)

set_source_files_properties(main.cc PROPERTIES LANGUAGE CXX)
add_executable(zombie main.cc)
set_target_properties(zombie PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(zombie mylib gmt ${MPI_LIBRARIES})
