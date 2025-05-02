file(GLOB_RECURSE SrcFiles CONFIGURE_DEPENDS "*.cpp")

add_library(SrcModule STATIC ${SrcFiles})

file(GLOB_RECURSE SrcSubdirs LIST_DIRECTORIES true
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    "${CMAKE_CURRENT_SOURCE_DIR}/*"
)

target_include_directories(SrcModule INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

foreach(dir ${SrcSubdirs})
  if(IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${dir}")
    target_include_directories(SrcModule PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/${dir}"
    )
  endif()
endforeach()

target_link_libraries(SrcModule PRIVATE IncludeModule)