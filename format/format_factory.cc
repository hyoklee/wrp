#include "format_factory.h"
#include "binary_file_omni.h"
#ifdef  USE_HDF5
#include "hdf5_dataset_client.h"
#endif
#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace cae {

std::unique_ptr<FormatClient> FormatFactory::Get(Format format) {
  switch (format) {
  case Format::kPosix:
  case Format::kBinary:
    return std::make_unique<BinaryFileOmni>();
#ifdef  USE_HDF5    
  case Format::kHDF5:
    return std::make_unique<Hdf5DatasetClient>();
#endif
  default:
    throw std::runtime_error("Unknown format type");
  }
}

std::unique_ptr<FormatClient>
FormatFactory::Get(const std::string &format_str) {
  std::string lower_format = format_str;
  std::transform(lower_format.begin(), lower_format.end(), lower_format.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (lower_format == "posix" || lower_format == "binary") {
    return Get(Format::kPosix);
  } else if (lower_format == "hdf5") {
    return Get(Format::kHDF5);
  } else {
    throw std::runtime_error("Unknown format string: " + format_str);
  }
}

} // namespace cae
