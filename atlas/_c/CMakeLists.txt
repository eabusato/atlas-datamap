cmake_minimum_required(VERSION 3.16)
project(atlas_sigilo C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

set(ATLAS_SIGILO_SOURCES
    common/diagnostic.c
    common/errors.c
    sigil/sigil_parse.c
    sigil/sigil_validate.c
    atlas_render.c
    atlas_sigilo.c
)

add_library(atlas_sigilo SHARED ${ATLAS_SIGILO_SOURCES})
target_include_directories(atlas_sigilo PRIVATE .)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang" AND NOT APPLE)
    target_compile_definitions(atlas_sigilo PRIVATE _GNU_SOURCE)
endif()
target_compile_definitions(atlas_sigilo PRIVATE ATLAS_SIGILO_EXPORTS)

if(NOT WIN32)
    target_link_libraries(atlas_sigilo m)
endif()

if(NOT CMAKE_LIBRARY_OUTPUT_DIRECTORY)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/../")
endif()

set_target_properties(atlas_sigilo PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
    OUTPUT_NAME "atlas_sigilo"
)

if(WIN32)
    set_target_properties(atlas_sigilo PROPERTIES
        WINDOWS_EXPORT_ALL_SYMBOLS ON
    )
endif()

add_library(atlas_sigilo_static STATIC ${ATLAS_SIGILO_SOURCES})
target_include_directories(atlas_sigilo_static PRIVATE .)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang" AND NOT APPLE)
    target_compile_definitions(atlas_sigilo_static PRIVATE _GNU_SOURCE)
endif()
target_compile_definitions(atlas_sigilo_static PRIVATE ATLAS_SIGILO_STATIC)
if(NOT WIN32)
    target_link_libraries(atlas_sigilo_static m)
endif()
