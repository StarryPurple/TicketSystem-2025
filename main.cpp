#include <iostream>

#include "fstream.h"

struct Page {
  int data[1000];
  char padding[96];
  Page(int n) {
    for(int &val : data) val = n;
  }
};

int main() {
  std::filesystem::path path = std::filesystem::current_path();
  auto data_dir = path / "db_data";
  std::filesystem::create_directory(data_dir);
  auto test_file = data_dir / "test.dat";
  ism::disk::fstream<Page> fstream(test_file);
  size_t pid[100];
  for(int i = 0; i < 100; ++i) {
    pid[i] = fstream.alloc();
    Page page(i);
    fstream.write(pid[i], &page);
  }
  for(int i = 0; i < 10; ++i)
    fstream.dealloc(pid[i]);
  fstream.close();
  ism::disk::fstream<Page> fstream2(test_file);
  for(int i = 99; i >= 10; --i) {
    Page page(0);
    fstream2.read(pid[i], &page);
    if(page.data[0] != i) std::cout << "H" << std::endl;
  }
  return 0;
}