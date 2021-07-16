cmake_minimum_required (VERSION 3.13)
if (POLICY CMP0068)
    cmake_policy(SET CMP0068 NEW)
endif ()
# Define file(GENERATE) behavior for relative paths.
if (POLICY CMP0070)
    cmake_policy(SET CMP0070 NEW)
endif ()
# target_sources: use absolute path for INTERFACE_SOURCES.
if (POLICY CMP0076)
    cmake_policy(SET CMP0076 NEW)
endif ()
# SWIG: use standard target name.
if (POLICY CMP0078)
    cmake_policy(SET CMP0078 NEW)
endif ()
# SWIG: use SWIG_MODULE_NAME property.
if (POLICY CMP0086)
    cmake_policy(SET CMP0086 NEW)
endif ()

project(rtidb)

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()
message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
set(OPENMLDB_VERSION_MAJOR 0)
set(OPENMLDB_VERSION_MINOR 1)
set(OPENMLDB_VERSION_BUG 5)
set(OPENMLDB_COMMIT_ID 0)

function(get_commitid CODE_DIR COMMIT_ID)
    find_package(Git REQUIRED)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h
        OUTPUT_VARIABLE COMMIT_ID_TMP
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        WORKING_DIRECTORY
        ${CODE_DIR}
    )
    set("${COMMIT_ID}" ${COMMIT_ID_TMP} PARENT_SCOPE)
endfunction()

get_commitid(${PROJECT_SOURCE_DIR} OPENMLDB_COMMIT_ID)
configure_file (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions('-g')
add_definitions('-Wall')
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions(-Wreorder)
    add_definitions(-Wno-unused-private-field)
    add_definitions(-Wno-unused-function)
    add_definitions(-Wno-overloaded-virtual)
    add_definitions(-Wno-inconsistent-missing-override)
    add_definitions(-Wno-deprecated-declarations)
    add_definitions(-Wno-null-dereference)
    add_definitions(-Wno-deprecated-register)
    add_definitions(-Wno-c++17-extensions)
    add_definitions(-Wno-defaulted-function-deleted)
    add_definitions(-Wno-pessimizing-move)
    add_definitions(-Wno-unused-value)
    add_definitions(-Wno-\#pragma-messages)
endif ()
add_definitions(-DHAVE_STDBOOL_H=1)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
option(TESTING_ENABLE "Enable test" OFF)
option(TCMALLOC_ENABLE "Enable TCMALLOC" ON)
option(SQL_PYSDK_ENABLE "Enable sql pysdk" OFF)
option(SQL_JAVASDK_ENABLE "Enable sql javasdk" OFF)
option(MAC_TABLET_ENABLE "Enable Table on Mac OS" OFF)
message (STATUS "MAC_TABLET_ENABLE: ${MAC_TABLET_ENABLE}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if (MAC_TABLET_ENABLE)
    add_compile_definitions(__mac_tablet__=1)
endif ()
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(BRPC_LIBS brpc protobuf glog gflags unwind ssl crypto leveldb z snappy dl pthread ${OS_LIB})
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OS_LIB 
            ${CMAKE_THREAD_LIBS_INIT}
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
    set(BRPC_LIBS brpc protobuf glog gflags ssl crypto leveldb z snappy dl pthread ${OS_LIB})
endif ()

if (NOT DEFINED CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/thirdparty)
endif()

set(VM_LIBS hybridse_core hybridse_sdk hybridse_flags boost_filesystem boost_date_time boost_regex)
set(LLVM_DIR "${CMAKE_PREFIX_PATH}/lib/cmake/llvm")
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
llvm_map_components_to_libnames(LLVM_LIBS support core orcjit nativecodegen)
add_definitions(${LLVM_DEFINITIONS})
if(UNIX AND NOT APPLE)
    # TODO: swig 4.0.1 compiled is not portable, check 'swig -swiglib'
    set(SWIG_DIR ${CMAKE_PREFIX_PATH}/share/swig/4.0.1)
endif()
find_package(SWIG REQUIRED)
include(UseSWIG)
find_package(absl REQUIRED)

find_package(GTest 1.10 EXACT REQUIRED)

#set(Protobuf_USE_STATIC_LIBS ON)
#find_package(Protobuf 3.6 EXACT REQUIRED)
list(
        APPEND
        ABSL_LIBS
        absl::algorithm
        absl::base
        absl::cord
        absl::debugging
        absl::flat_hash_map
        absl::flags
        absl::flags_parse
        absl::memory
        absl::meta
        absl::numeric
        absl::random_random
        absl::strings
        absl::strings_internal
        absl::synchronization
        absl::time
        absl::status)

list(APPEND ICU_LIBS icui18n icuio icuuc icudata)
list(APPEND file_based_test_driver_LIBS
        alternations
        test_case_options
        test_case_outputs
        test_case_mode
        logging
        path
        status
        unified_diff_oss
        ret_check
        )
set(ZETASQL_LIBS
        zetasql
        ${file_based_test_driver_LIBS}
        ${ABSL_LIBS}
        re2
        ${ICU_LIBS}
        date_proto
        timeofday_proto
        latlng_proto
        )

if (TCMALLOC_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free ${CMAKE_EXE_LINKER_FLAGS}")
endif()

if(POLICY CMP0086)
  cmake_policy(SET CMP0086 NEW)
endif()

configure_file(
    "${PROJECT_SOURCE_DIR}/src/config.h.in"
    "${PROJECT_SOURCE_DIR}/src/config.h")

include_directories(${CMAKE_PREFIX_PATH}/include
        ${CMAKE_PREFIX_PATH}/hybridse/include)

link_directories(${CMAKE_PREFIX_PATH}/lib
    ${CMAKE_PREFIX_PATH}/lib64
    ${CMAKE_PREFIX_PATH}/hybridse/lib)

add_subdirectory(src)
add_custom_target(rtidbpy
COMMAND make
WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/swig
)
