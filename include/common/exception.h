#ifndef INSOMNIA_EXCEPTION_H
#define INSOMNIA_EXCEPTION_H

namespace insomnia {
namespace container {
  class container_exception : std::exception {
    // using container_exceptions = std::exception;
  };

  class container_is_empty : container_exception {};

  class invalid_iterator : container_exception {};

  class index_out_of_bound : container_exception {};
}
namespace concurrent {
  class concurrent_exception : std::exception {};

  class pool_overflow : concurrent_exception {};

  class invalid_pool : concurrent_exception {};
}
}


#endif