cmake_minimum_required(VERSION 3.28)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(btrfs-dump VERSION 20260330)

include(GNUInstallDirs)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/btrfs-dump.1.in ${CMAKE_CURRENT_BINARY_DIR}/btrfs-dump.1)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/btrfs-assemble.1.in ${CMAKE_CURRENT_BINARY_DIR}/btrfs-assemble.1)

find_package(PkgConfig REQUIRED)

add_executable(btrfs-dump src/btrfs-dump.cpp)

pkg_check_modules(BLKID REQUIRED IMPORTED_TARGET blkid)

target_sources(btrfs-dump PUBLIC FILE_SET CXX_MODULES FILES
    src/cxxbtrfs.cpp
    src/formatted_error.cpp
    src/b64.cpp
    src/crc32c.cpp
    src/xxhash.cpp
    src/sha256.cpp
    src/blake2b.cpp)

target_compile_options(btrfs-dump PUBLIC -Wall -Wextra -Wno-unqualified-std-cast-call -Wno-missing-field-initializers -fno-strict-aliasing)
target_include_directories(btrfs-dump PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(btrfs-dump PkgConfig::BLKID)

install(TARGETS btrfs-dump DESTINATION ${CMAKE_INSTALL_BINDIR}
    CXX_MODULES_BMI EXCLUDE_FROM_ALL
)

add_executable(btrfs-assemble src/btrfs-assemble.cpp)

target_sources(btrfs-assemble PUBLIC FILE_SET CXX_MODULES FILES
    src/cxxbtrfs.cpp
    src/formatted_error.cpp
    src/b64.cpp
    src/crc32c.cpp
    src/xxhash.cpp
    src/sha256.cpp
    src/blake2b.cpp)

target_compile_options(btrfs-assemble PUBLIC -Wall -Wextra -Wno-unqualified-std-cast-call -Wno-missing-field-initializers -fno-strict-aliasing)
target_include_directories(btrfs-assemble PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

install(TARGETS btrfs-assemble DESTINATION ${CMAKE_INSTALL_BINDIR}
    CXX_MODULES_BMI EXCLUDE_FROM_ALL
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/btrfs-dump.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/btrfs-assemble.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
