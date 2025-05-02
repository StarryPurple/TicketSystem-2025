#ifndef TICKETSYSTEM_CONFIG_H
#define TICKETSYSTEM_CONFIG_H

#include <iostream>

#include "container/config.h"

namespace insomnia {
namespace disk {}
}

#ifndef BASIC_PAGE_SIZE
#define BASIC_PAGE_SIZE 4096
#endif

using frame_id_t = short;
using page_id_t = size_t;

#endif