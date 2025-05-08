#ifndef INSOMNIA_EXCEPTION_H
#define INSOMNIA_EXCEPTION_H

namespace insomnia {

namespace container {
  class container_exception : std::runtime_error {
  public:
    explicit container_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class container_is_empty : container_exception {
  public:
    explicit container_is_empty(const char *detail = "") : container_exception(detail) {}
  };

  class invalid_iterator : container_exception {
  public:
    explicit invalid_iterator(const char *detail = "") : container_exception(detail) {}
  };

  class index_out_of_bound : container_exception {
  public:
    explicit index_out_of_bound(const char *detail = "") : container_exception(detail) {}
  };
}

namespace disk {
  class disk_exception : std::runtime_error {
  public:
    explicit disk_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class segmentation_fault : disk_exception {
  public:
    explicit segmentation_fault(const char *detail = "") : disk_exception(detail) {}
  };
}

namespace concurrent {
  class concurrent_exception : std::runtime_error {
  public:
    explicit concurrent_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class pool_overflow : concurrent_exception {
  public:
    explicit pool_overflow(const char *detail = "") : concurrent_exception(detail) {}
  };

  class invalid_pool : concurrent_exception {
  public:
    explicit invalid_pool(const char *detail = "") : concurrent_exception(detail) {}
  };
}

namespace database {
  class database_exception : std::runtime_error {
  public:
    explicit database_exception(const char *detail = "") : runtime_error(detail) {}
  };

  class debug_exception : database_exception {
  public:
    explicit debug_exception(const char *detail = "") : database_exception(detail) {}
  };
}

}

#endif