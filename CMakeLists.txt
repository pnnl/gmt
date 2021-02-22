# See README.md for instruction on how to build GMT with CMake

cmake_minimum_required(VERSION 2.8.12)

if (NOT DEFINED GMT_VERSION_MAJOR)
  set(GMT_VERSION_MAJOR 2)
endif()
if (NOT DEFINED GMT_VERSION_MINOR)
  set(GMT_VERSION_MINOR 0)
endif()
if (NOT DEFINED GMT_VERSION_PATCH)
  set(GMT_VERSION_PATCH 0)
endif()

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
  set(cmake_3_0_PROJ_VERSION
    VERSION ${GMT_VERSION_MAJOR}.${GMT_VERSION_MINOR}.${GMT_VERSION_PATCH})
  set(cmake_3_0_LANGUAGES LANGUAGES)
endif()

if (NOT PACKAGE_VERSION)
  set(PACKAGE_VERSION
    "${GMT_VERSION_MAJOR}.${GMT_VERSION_MINOR}.${GMT_VERSION_PATCH}")
endif()

project(GMT ${cmake_3_0_PROJ_VERSION} ${cmake_3_0_LANGUAGES} CXX)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "No build type selected, default to Debug")
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type (default Debug)" FORCE)
endif()


# No in-tree build allowed.
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR
    "In-source build are not allowed.
Please create a directory directory and run cmake from there, passing the path
to this source directory as the last argumente.
This process created the file `CMakeCache.txt' and the directory `CMakeFiles'.
Please delete them.")
endif()


string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)

if (CMAKE_BUILD_TYPE AND
    NOT uppercase_CMAKE_BUILD_TYPE MATCHES "^(DEBUG|RELEASE|RELWITHDEBINFO|MINSIZEREL)$")
  message(FATAL_ERROR "Invalid value for CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
endif()


set(GMT_MAIN_SRC_DIR     ${CMAKE_CURRENT_SOURCE_DIR})
set(GMT_MAIN_INCLUDE_DIR ${GMT_MAIN_SRC_DIR}/include)
set(GMT_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/include)

# check for dependencies
find_package(MPI REQUIRED)

include_directories(${MPI_INCLUDE_PATH})
link_libraries(rt)
link_libraries(m)

include_directories(${GMT_MAIN_INCLUDE_DIR} ${GMT_INCLUDE_DIR})

# Appending -O3 to gmt because otherwise it will fail to link.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wabi -Wextra")

configure_file(${GMT_MAIN_INCLUDE_DIR}/gmt/config.h.in ${GMT_INCLUDE_DIR}/gmt/config.h)
install(FILES ${GMT_INCLUDE_DIR}/gmt/config.h DESTINATION include/gmt)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(algorithms/zombie)

install(DIRECTORY ${GMT_MAIN_INCLUDE_DIR}/gmt
  DESTINATION include
  PATTERN "*.h"
  PATTERN "*.hpp"
  PATTERN ".*" EXCLUDE
  PATTERN "*.in" EXCLUDE
  PATTERN "*~" EXCLUDE
  PATTERN "CMakeLists.txt"
)
