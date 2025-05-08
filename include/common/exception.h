#ifndef INSOMNIA_EXCEPTION_H
#define INSOMNIA_EXCEPTION_H

namespace insomnia {

// inherits must be public for catch(std::exception &) to catch.

  class container_exception : public std::runtime_error {
  public:
    explicit container_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class container_is_empty : public container_exception {
  public:
    explicit container_is_empty(const char *detail = "") : container_exception(detail) {}
  };

  class invalid_iterator : public container_exception {
  public:
    explicit invalid_iterator(const char *detail = "") : container_exception(detail) {}
  };

  class index_out_of_bound : public container_exception {
  public:
    explicit index_out_of_bound(const char *detail = "") : container_exception(detail) {}
  };

  class disk_exception : public std::runtime_error {
  public:
    explicit disk_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class segmentation_fault : public disk_exception {
  public:
    explicit segmentation_fault(const char *detail = "") : disk_exception(detail) {}
  };

  class concurrent_exception : public std::runtime_error {
  public:
    explicit concurrent_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class pool_overflow : public concurrent_exception {
  public:
    explicit pool_overflow(const char *detail = "") : concurrent_exception(detail) {}
  };

  class invalid_pool : public concurrent_exception {
  public:
    explicit invalid_pool(const char *detail = "") : concurrent_exception(detail) {}
  };

  class database_exception : public std::runtime_error {
  public:
    explicit database_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class debug_exception : public database_exception {
  public:
    explicit debug_exception(const char *detail = "") : database_exception(detail) {}
  };

}

#endif