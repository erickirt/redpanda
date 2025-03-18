/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/data_writer_interface.h"

#include <fmt/core.h>
namespace datalake {
std::ostream& operator<<(std::ostream& os, const writer_error& ev) {
    switch (ev) {
    case writer_error::ok:
        return os << "Ok";
    case writer_error::parquet_conversion_error:
        return os << "Parquet Conversion Error";
    case writer_error::file_io_error:
        return os << "File IO Error";
    case writer_error::no_data:
        return os << "No data";
    case writer_error::flush_error:
        return os << "Flush failed";
    case writer_error::oom_error:
        return os << "Memory exhausted";
    }
}
std::string data_writer_error_category::message(int ev) const {
    return fmt::to_string(static_cast<writer_error>(ev));
}

} // namespace datalake
