cmake_minimum_required(VERSION 3.15)
project(TicketSystem)

set(CMAKE_CXX_STANDARD 17)

add_library(container_lib main.cpp)
target_include_directories(
    container_lib PUBLIC
    include/container
)
add_library(buffer_lib main.cpp)
target_include_directories(
    buffer_lib PUBLIC
    include/buffer
)
add_executable(code main.cpp)
target_link_libraries(
    code PRIVATE
    container_lib
    buffer_lib
)