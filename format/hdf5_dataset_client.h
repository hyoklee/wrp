#ifndef CAE_FORMAT_HDF5_DATASET_CLIENT_H_
#define CAE_FORMAT_HDF5_DATASET_CLIENT_H_

#include "dataset_config.h"
#include "format_client.h"
#include <hdf5.h>
#ifdef USE_MPI
#include <mpi.h>
#endif
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace cae {

/**
 * HDF5 Dataset Processing Client
 * 
 * This client can read datasets from HDF5 files according to a configuration
 * that specifies the dataset name, hyperslab parameters, and processing options.
 */
class Hdf5DatasetClient : public FormatClient {
private:
  static constexpr size_t DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB chunks

public:
  /** Default constructor */
  Hdf5DatasetClient() = default;

  /** Destructor */
  ~Hdf5DatasetClient() override = default;

  /** Describe the dataset */
  std::string Describe(const FormatContext &ctx) override {
    return "HDF5 dataset: " + ctx.filename_ +
           " (size: " + std::to_string(ctx.size_) +
           " bytes, offset: " + std::to_string(ctx.offset_) + ")";
  }

  /** Process an HDF5 dataset according to configuration */
  void Import(const FormatContext &ctx) override;

  /** 
   * Read dataset using configuration
   * @param config Configuration for reading the dataset
   * @param[out] buffer_size Size of the returned buffer in bytes
   * @return Pointer to the allocated buffer containing the dataset data, or nullptr on failure
   *         Caller is responsible for freeing this memory using delete[]
   */
  unsigned char* ReadDataset(const DatasetConfig& config, size_t& buffer_size);

  /** Execute the run script if specified */
  void ExecuteRunScript(const std::string& script_path, const std::string& input_file, const std::string& output_file);

protected:
  virtual void OnChunkProcessed(size_t bytes_processed) {}
  virtual void OnDatasetRead(const std::string& dataset_name, const std::vector<hsize_t>& dimensions) {}

private:
  /** Open HDF5 file */
  hid_t OpenHdf5File(const std::string& file_path);

  /** Close HDF5 file */
  void CloseHdf5File(hid_t file_id);

  /** Read dataset hyperslab */
  bool ReadDatasetHyperslab(hid_t file_id, const std::string& dataset_name,
                           const std::vector<hsize_t>& start,
                           const std::vector<hsize_t>& count,
                           const std::vector<hsize_t>& stride,
                           void* buffer, hid_t datatype);

  /** Get dataset information */
  bool GetDatasetInfo(hid_t file_id, const std::string& dataset_name,
                     std::vector<hsize_t>& dimensions, hid_t& datatype);

  /** Allocate buffer for dataset */
  std::unique_ptr<char[]> AllocateBuffer(const std::vector<hsize_t>& dimensions, hid_t datatype);

  /** Calculate total size of dataset */
  size_t CalculateDatasetSize(const std::vector<hsize_t>& dimensions, hid_t datatype);

  /** Print dataset values for debugging */
  void PrintDatasetValues(hid_t dataset_id, const std::string& dataset_name);

  /** Print hyperslab values after reading */
  void PrintHyperslabValues(const void* buffer, const std::vector<hsize_t>& dimensions,
                           hid_t datatype, const std::string& dataset_name);

#ifdef USE_MPI
  /** Open HDF5 file using MPI-IO with fallback to serial */
  hid_t OpenHdf5FileMPI(const std::string& file_path, MPI_Comm comm, MPI_Info info);

  /** Read dataset hyperslab using parallel HDF5 with fallback to serial */
  bool ReadDatasetHyperslabMPI(hid_t file_id, const std::string& dataset_name,
                               const std::vector<hsize_t>& start,
                               const std::vector<hsize_t>& count,
                               const std::vector<hsize_t>& stride,
                               void* buffer, hid_t datatype);

  /** Check if HDF5 parallel is available */
  bool IsHdf5ParallelAvailable();

  /** Flag to track if parallel HDF5 is available */
  bool mpi_initialized_ = false;
  bool parallel_hdf5_available_ = false;
#endif
};

} // namespace cae

#endif // CAE_FORMAT_HDF5_DATASET_CLIENT_H_
