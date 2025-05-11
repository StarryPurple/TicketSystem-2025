#include <iostream>

#include "array.h"
#include "bplustree.h"

void BptTest() {
  using index_t = insomnia::array<char, 64>;
  using value_t = int;
  using MulBpt_t = insomnia::MultiBPlusTree<index_t, value_t>;

  auto dir = std::filesystem::current_path() / "data";
  // std::filesystem::remove_all(dir);
  std::filesystem::create_directory(dir);
  auto name_base = dir / "test";
  int k_param = 3;
  int buffer_cap = 1024;
  int thread_num = 4;
  MulBpt_t mul_bpt(name_base, k_param, buffer_cap, thread_num);

  /*
  freopen("temp/input.txt", "r", stdin);
  freopen("temp/output.txt", "w", stdout);
  */

  int optcnt, value;
  std::string opt, index;
  std::cin >> optcnt;
  for(int i = 1; i <= optcnt; ++i) {
    std::cin >> opt;
    if(opt[0] == 'i') {
      std::cin >> index >> value;
      mul_bpt.insert(index, value);
    } else if(opt[0] == 'f') {
      std::cin >> index;
      insomnia::vector<value_t> list = mul_bpt.search(index);
      if(list.empty())
        std::cout << "null" << std::endl;
      else {
        for(value_t &val : list)
          std::cout << val << ' ';
        std::cout << std::endl;
      }
    } else if(opt[0] == 'd') {
      std::cin >> index >> value;
      mul_bpt.remove(index, value);
    }
  }

  // system("diff -bB temp/output.txt temp/answer.txt");
}

int main() {
  BptTest();
  return 0;
}