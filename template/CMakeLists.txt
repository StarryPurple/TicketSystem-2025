add_library(TemplateModule INTERFACE)

file(GLOB_RECURSE TplSubdirs LIST_DIRECTORIES true
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    "${CMAKE_CURRENT_SOURCE_DIR}/*"
)

target_include_directories(TemplateModule INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

foreach(dir ${TplSubdirs})
  if(IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${dir}")
    target_include_directories(TemplateModule INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${dir}>
    )
  endif()
endforeach()

target_link_libraries(TemplateModule INTERFACE IncludeModule)