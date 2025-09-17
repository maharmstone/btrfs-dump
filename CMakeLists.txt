cmake_minimum_required(VERSION 3.28)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(btrfs-dump)

include(GNUInstallDirs)

find_package(PkgConfig REQUIRED)

add_executable(btrfs-dump src/btrfs-dump.cpp)

pkg_check_modules(BLKID REQUIRED IMPORTED_TARGET blkid)

target_sources(btrfs-dump PUBLIC FILE_SET CXX_MODULES FILES
    src/cxxbtrfs.cpp
    src/formatted_error.cpp
    src/b64.cpp)

target_compile_options(btrfs-dump PUBLIC -Wall -Wextra -Wno-unqualified-std-cast-call -Wno-missing-field-initializers)

target_link_libraries(btrfs-dump PkgConfig::BLKID)

install(TARGETS btrfs-dump DESTINATION ${CMAKE_INSTALL_BINDIR}
    CXX_MODULES_BMI EXCLUDE_FROM_ALL
)
