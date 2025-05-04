#include <iostream>
#include <unordered_map>
#include "unordered_map.h"
#include "array.h"
#include <array>
#include <map>
#include <chrono>
#include "fstream.h"


int main() {
  constexpr int a = 20000, A = 50000;
  auto start = std::chrono::high_resolution_clock().now();
  ism::cntr::unordered_map<int, int> iumap;
  for(int i = 1; i < a; ++i)
    iumap.emplace(i, i);
  for(int i = 0; i < A; ++i) {
    auto it = iumap.find(i);
    if(it != iumap.end()) {
      if(i != it->second) std::cout << "H";
      i = it->second;
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "ism umap: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  start = std::chrono::high_resolution_clock().now();
  std::unordered_map<int, int> umap;
  for(int i = 1; i <= a; ++i)
    umap.emplace(i, i);
  for(int i = 0; i < A; ++i) {
    auto it = umap.find(i);
    if(it != umap.end())
      i = it->second;
  }
  end = std::chrono::high_resolution_clock::now();
  std::cout << "std umap: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  start = std::chrono::high_resolution_clock::now();
  std::map<int, int> map;
  for(int i = 1; i <= a; ++i)
    map.emplace(i, i);
  for(int i = 0; i < A; ++i) {
    auto it = map.find(i);
    if(it != map.end())
      i = it->second;
  }
  end = std::chrono::high_resolution_clock::now();
  std::cout << "std map: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  start = std::chrono::high_resolution_clock::now();
  std::pair<int, int> arr[A];
  for(int i = 1; i <= a; ++i)
    arr[i] = {i, i};
  for(int i = 0; i < A; ++i) {
    for(int j = 0; j < A; ++j) {
      if(arr[j].first == i) {
        i = arr[j].second;
        break;
      }
    }
  }
  end = std::chrono::high_resolution_clock::now();
  std::cout << "raw arr: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  start = std::chrono::high_resolution_clock::now();
  std::array<std::pair<int, int>, A> sarr;
  for(int i = 1; i <= a; ++i)
    sarr[i] = {i, i};
  for(int i = 0; i < A; ++i) {
    for(int j = 0; j < A; ++j) {
      if(sarr[j].first == i) {
        i = sarr[j].second;
        break;
      }
    }
  }
  end = std::chrono::high_resolution_clock::now();
  std::cout << "std arr: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  start = std::chrono::high_resolution_clock::now();
  ism::cntr::array<std::pair<int, int>, A> iarr;
  for(int i = 1; i <= a; ++i)
    iarr[i] = {i, i};
  for(int i = 0; i < A; ++i) {
    for(int j = 0; j < A; ++j) {
      if(iarr[j].first == i) {
        i = iarr[j].second;
        break;
      }
    }
  }
  end = std::chrono::high_resolution_clock::now();
  std::cout << "ism arr: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << std::endl;
  return 0;
}