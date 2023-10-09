/**
 * @file   vfs_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares some vfs-specfic test suite helper functions.
 */

#ifndef TILEDB_VFS_HELPERS_H
#define TILEDB_VFS_HELPERS_H

#include "test/support/src/helpers.h"
#include "test/support/tdb_catch.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

namespace tiledb {
namespace test {

#ifdef TILEDB_TESTS_AWS_CONFIG
constexpr bool aws_s3_config = true;
#else
constexpr bool aws_s3_config = false;
#endif

// Forward declaration
class SupportedFs;

/**
 * Create the vector of supported filesystems.
 */
std::vector<std::unique_ptr<SupportedFs>> vfs_test_get_fs_vec();

/**
 * Initialize the vfs test.
 *
 * @param fs_vec The vector of supported filesystems
 * @param ctx The TileDB context.
 * @param vfs The VFS object.
 * @param config An optional configuration argument.
 */
Status vfs_test_init(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs,
    tiledb_config_t* config = nullptr);

/**
 * Close the vfs test.
 *
 * @param fs_vec The vector of supported filesystems
 * @param ctx The TileDB context.
 * @param vfs The VFS object.
 */
Status vfs_test_close(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs);

void vfs_test_remove_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path);

void vfs_test_create_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path);

/**
 * Generates a random temp directory URI for use in VFS tests.
 *
 * @param prefix A prefix to use for the temp directory name. Should include
 *    `s3://`, `mem://` or other URI prefix for the backend.
 * @return URI which the caller can use to create a temp directory.
 */
tiledb::sm::URI test_dir(const std::string& prefix);

/**
 * This class defines and manipulates
 * a list of supported filesystems.
 */
class SupportedFs {
 public:
  virtual ~SupportedFs() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(
      tiledb_config_t* config, tiledb_error_t* error) = 0;

  /**
   * Creates bucket / container if does not exist
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /**
   * Removes bucket / container if exists
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir() = 0;
};

/**
 * This class provides support for
 * the S3 filesystem.
 */
class SupportedFsS3 : public SupportedFs {
 public:
  SupportedFsS3()
      : s3_prefix_("s3://")
      , s3_bucket_(s3_prefix_ + random_name("tiledb") + "/")
      , temp_dir_(s3_bucket_ + "tiledb_test/") {
  }

  ~SupportedFsS3() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates bucket if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes bucket if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the S3 filesystem. */
  const std::string s3_prefix_;

  /** The bucket name for the S3 filesystem. */
  const std::string s3_bucket_;

  /** The directory name of the S3 filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for
 * the HDFS filesystem.
 */
class SupportedFsHDFS : public SupportedFs {
 public:
  SupportedFsHDFS()
      : temp_dir_("hdfs:///tiledb_test/") {
  }

  ~SupportedFsHDFS() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * No-op
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory name of the HDFS filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for
 * the Azure filesystem.
 */
class SupportedFsAzure : public SupportedFs {
 public:
  SupportedFsAzure()
      : azure_prefix_("azure://")
      , container_(azure_prefix_ + random_name("tiledb") + "/")
      , temp_dir_(container_ + "tiledb_test/") {
  }

  ~SupportedFsAzure() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates container if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes container if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the Azure filesystem. */
  const std::string azure_prefix_;

  /** The container name for the Azure filesystem. */
  const std::string container_;

  /** The directory name of the Azure filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for the GCS filesystem.
 */
class SupportedFsGCS : public SupportedFs {
 public:
  SupportedFsGCS(std::string prefix = "gcs://")
      : prefix_(prefix)
      , bucket_(prefix_ + random_name("tiledb") + "/")
      , temp_dir_(bucket_ + "tiledb_test/") {
  }

  ~SupportedFsGCS() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates bucket if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes bucket if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the GCS filesystem. */
  const std::string prefix_;

  /** The bucket name for the GCS filesystem. */
  const std::string bucket_;

  /** The directory name of the GCS filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for
 * the Windows or Posix (local) filesystem.
 */
class SupportedFsLocal : public SupportedFs {
 public:
#ifdef _WIN32
  SupportedFsLocal()
      : temp_dir_(tiledb::sm::Win::current_dir() + "\\tiledb_test\\")
      , file_prefix_("") {
  }
#else
  SupportedFsLocal()
      : temp_dir_(tiledb::sm::Posix::current_dir() + "/tiledb_test/")
      , file_prefix_("file://") {
  }
#endif

  ~SupportedFsLocal() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * No-op
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Get the name of the filesystem's file prefix
   *
   * @return string prefix name
   */
  std::string file_prefix();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

#ifdef _WIN32
  /** The directory name of the Windows filesystem. */
  std::string temp_dir_;

  /** The file prefix name of the Windows filesystem. */
  std::string file_prefix_;

#else
  /** The directory name of the Posix filesystem. */
  std::string temp_dir_;

  /** The file prefix name of the Posix filesystem. */
  std::string file_prefix_;
#endif
};

/**
 * This class provides support for
 * the Mem filesystem.
 */
class SupportedFsMem : public SupportedFs {
 public:
  SupportedFsMem()
      : temp_dir_("mem://tiledb_test/") {
  }

  ~SupportedFsMem() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates container if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes container if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory name of the Mem filesystem. */
  std::string temp_dir_;
};

/**
 * Fixture for creating a temporary directory for a test case. This fixture
 * also manages the context and virtual file system for the test case.
 */
struct TemporaryDirectoryFixture {
 public:
  /** Fixture constructor. */
  TemporaryDirectoryFixture()
      : supported_filesystems_(vfs_test_get_fs_vec()) {
    // Initialize virtual filesystem and context.
    REQUIRE(vfs_test_init(supported_filesystems_, &ctx, &vfs_).ok());

    // Create temporary directory based on the supported filesystem
#ifdef _WIN32
    SupportedFsLocal windows_fs;
    temp_dir_ = windows_fs.file_prefix() + windows_fs.temp_dir();
#else
    SupportedFsLocal posix_fs;
    temp_dir_ = posix_fs.file_prefix() + posix_fs.temp_dir();
#endif
    create_dir(temp_dir_, ctx, vfs_);
  }

  /** Fixture destructor. */
  ~TemporaryDirectoryFixture() {
    remove_dir(temp_dir_, ctx, vfs_);
    tiledb_ctx_free(&ctx);
    tiledb_vfs_free(&vfs_);
  }

  /**
   * Allocate an TileDB context to use the same configuration as the context for
   * the temporary directory except for encryption settings.
   *
   * @param encryption_type Value to set on the configuration for
   * `sm.encryption_type`.
   * @param encryption_key Value to set on the configuration for
   * `sm.encryption_key`.
   * @param ctx_with_encrypt Context that will be allocated.
   */
  void alloc_encrypted_ctx(
      const std::string& encryption_type,
      const std::string& encryption_key,
      tiledb_ctx_t** ctx_with_encrypt) const;

  /**
   * Creates a new array array in the temporary directory and returns the
   * fullpath the array.
   *
   * @param name Name of the array relative to the temporary directory.
   * @param array_schema Schema for the array to be created.
   * @param array_schema Schema for the array to be created.
   * @param serialize To serialize or not the creation of the schema
   * @returns URI of the array.
   */
  std::string create_temporary_array(
      std::string&& name,
      tiledb_array_schema_t* array_schema,
      const bool serialize = false);

  /**
   * Check the return code for a TileDB C-API function is TILEDB_ERR and
   * compare the last error message from the local TileDB context to an expected
   * error message.
   *
   * @param rc Return code from a TileDB C-API function.
   * @param expected_msg The expected message from the last error.
   */
  inline void check_tiledb_error_with(
      int rc, const std::string& expected_msg) const {
    test::check_tiledb_error_with(ctx, rc, expected_msg);
  }

  /**
   * Checks the return code for a TileDB C-API function is TILEDB_OK. If not,
   * if will add a failed assert to the Catch2 test and print the last error
   * message from the local TileDB context.
   *
   * @param rc Return code from a TileDB C-API function.
   */
  inline void check_tiledb_ok(int rc) const {
    test::check_tiledb_ok(ctx, rc);
  }

  /** Create a path in the temporary directory. */
  inline std::string fullpath(std::string&& name) {
    return temp_dir_ + name;
  }

  /**
   * Returns the context pointer object.
   */
  inline tiledb_ctx_t* get_ctx() {
    return ctx;
  }

  /**
   * Require the return code for a TileDB C-API function is TILEDB_ERR and
   * compare the last error message from the local TileDB context to an expected
   * error message.
   *
   * @param rc Return code from a TileDB C-API function.
   * @param expected_msg The expected message from the last error.
   */
  inline void require_tiledb_error_with(
      int rc, const std::string& expected_msg) const {
    test::require_tiledb_error_with(ctx, rc, expected_msg);
  }

  /**
   * Requires the return code for a TileDB C-API function is TILEDB_OK. If not,
   * it will end the Catch2 test and print the last error message from the local
   * TileDB context.
   *
   * @param rc Return code from a TileDB C-API function.
   */
  inline void require_tiledb_ok(int rc) const {
    test::require_tiledb_ok(ctx, rc);
  }

 protected:
  /** TileDB context */
  tiledb_ctx_t* ctx;

  /** Name of the temporary directory to use for this test */
  std::string temp_dir_;

 private:
  /** Virtual file system */
  tiledb_vfs_t* vfs_;

  /** Vector of supported filesystems. Used to initialize ``vfs_``. */
  const std::vector<std::unique_ptr<SupportedFs>> supported_filesystems_;
};

struct VfsFixture {
 public:
  VfsFixture();

  ~VfsFixture();

  void setup_test();

  /**
   * Typedef for customizing ls_recursive CAPI callback behavior. Custom filter
   * behavior can also be implemented within this callback by handling result
   * entry names accordingly.
   */
  typedef std::function<int32_t(const char*, size_t, uint64_t, void*)>
      LsRecursiveCb;

  /** Typedef for customizing ls_recursive filter behavior. */
  typedef std::function<bool(const std::string_view&)> LsInclude;

  struct LsObjects {
    std::vector<std::string> object_paths_;
    std::vector<uint64_t> object_sizes_;
    LsInclude ls_include_;
  };

  /// Helper function to filter expected results using a custom filter function.
  void filter_expected(const LsInclude& filter);

  void test_ls_recursive(
      const LsInclude& filter = [](const std::string_view&) { return true; },
      bool filter_expected = true);

  /**
   * Tests ls_recursive against S3, Memfs, and Posix / Windows filesystems.
   * GCS, Azure, and HDFS are currently unsupported for ls_recursive.
   */
  void test_ls_recursive_capi(
      const LsCallback& callback,
      const LsInclude& filter = [](const std::string_view&) { return true; },
      bool filter_expected = true);

  std::string fs_name();

  void test_ls_recursive_cb(LsCallback const cb, LsObjects data);

 protected:
  tiledb::Config cfg_;
  tiledb::Context ctx_;
  tiledb_ctx_handle_t* ctx_c_;
  tiledb::VFS vfs_{ctx_};
  tiledb_vfs_handle_t* vfs_c_;

  // The filesystem prefix for this test.
  std::string fs_prefix_;
  // The temporary directory for this test.
  sm::URI temp_dir_;
  std::vector<std::pair<std::string, uint64_t>> expected_results_;

  struct LsRecursiveData {
    LsRecursiveData(
        char** data, size_t data_max, uint64_t* object_sizes, LsInclude filter)
        : path_pos_(0)
        , path_max_(data_max)
        , filter_(std::move(filter)) {
      path_data_ = data;
      object_sizes_ = object_sizes;
    }

    ~LsRecursiveData() {
      for (size_t i = 0; i < path_pos_; ++i) {
        free(path_data_[i]);
      }
      free(path_data_);
      free(object_sizes_);
    }

    // Buffers for storing paths and object sizes.
    char** path_data_;
    uint64_t* object_sizes_;
    // Current buffer position, max buffer size for paths collected.
    size_t path_pos_, path_max_;
    LsInclude filter_;
  };
};

}  // End of namespace test
}  // End of namespace tiledb

#endif
