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
#include "tiledb/sm/enums/vfs_mode.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

namespace tiledb {
namespace test {

// Forward declaration
class SupportedFs;

#ifdef TILEDB_TESTS_AWS_CONFIG
constexpr bool aws_s3_config = true;
#else
constexpr bool aws_s3_config = false;
#endif

/**
 * Generates a random temp directory URI for use in VFS tests.
 *
 * @param prefix A prefix to use for the temp directory name. Should include
 *    `s3://`, `mem://` or other URI prefix for the backend.
 * @return URI which the caller can use to create a temp directory.
 */
tiledb::sm::URI test_dir(const std::string& prefix);

/**
 * Creates a config for testing VFS storage backends over local emulators.
 *
 * @return Fully initialized configuration for testing VFS storage backends.
 */
tiledb::sm::Config create_test_config();

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

  /** Virtual file system */
  tiledb_vfs_t* vfs_;

 private:
  /** Vector of supported filesystems. Used to initialize ``vfs_``. */
  const std::vector<std::unique_ptr<SupportedFs>> supported_filesystems_;
};

class VFSTest {
 public:
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  explicit VFSTest(
      const std::vector<size_t>& test_tree,
      const std::string& prefix = "file://");

  virtual ~VFSTest();

  virtual void create_objects(
      const sm::URI& uri, size_t count, const std::string& prefix);

  virtual void setup_test();

  void test_ls_recursive(tiledb::sm::LsCallback cb, size_t expected_count = 0);

  inline bool supports_fs() const {
    return vfs_.supports_uri_scheme(temp_dir_);
  }

  std::vector<size_t> test_tree_;
  ThreadPool compute_, io_;
  tiledb::sm::VFS vfs_;
  tiledb::sm::URI temp_dir_;

  LsObjects expected_results_;
};

class S3Test : public VFSTest {
 public:
  explicit S3Test(const std::vector<size_t>& test_tree);

  ~S3Test() = default;

  void create_objects(
      const sm::URI& uri, size_t count, const std::string& prefix) override;

  void setup_test() override;

  void test_ls_cb(tiledb::sm::LsCallback cb, bool recursive);
};

}  // End of namespace test
}  // End of namespace tiledb

#endif
