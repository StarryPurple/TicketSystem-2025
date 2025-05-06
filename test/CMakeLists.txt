add_subdirectory(
    ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/googletest-1.13.0
    ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
)


file(GLOB_RECURSE GTestSources CONFIGURE_DEPENDS "*_gtest.cpp")
foreach(gtest_source ${GTestSources})
  get_filename_component(gtest_name ${gtest_source} NAME_WE)
  add_executable(${gtest_name} ${gtest_source})
  target_link_libraries(${gtest_name} PRIVATE
      IncludeModule
      SrcModule
      gtest
      gtest_main)
  add_test(NAME ${gtest_name} COMMAND ${gtest_name})
endforeach()