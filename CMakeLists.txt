cmake_minimum_required(VERSION 3.24)
project(TicketSystem)

set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)

# enable_testing()

add_subdirectory(template)
add_subdirectory(include)
add_subdirectory(src)
# add_subdirectory(test)

add_executable(code main.cpp)

target_link_libraries(code PRIVATE
    IncludeModule
    SrcModule
)