add_library(IncludeModule INTERFACE)

file(GLOB_RECURSE IncludeSubdirs LIST_DIRECTORIES true
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/*")

target_include_directories(IncludeModule INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

foreach(dir ${IncludeSubdirs})
  if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${dir})
    target_include_directories(IncludeModule INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${dir}>)
  endif()
endforeach()

target_link_libraries(IncludeModule INTERFACE TemplateModule)