#include <tiledb/tiledb_experimental.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;
using namespace std::chrono;

int main() {
  Config cfg;

  cfg["rest.username"] = "demo3";
  cfg["rest.password"] = "Demodemo1!";
  cfg["rest.server_address"] = "http://localhost:80";
  cfg["rest.curl.buffer_size"] = "104857600";  // 100MB
  Context ctx(cfg);
  std::string array_name("tiledb://demo3/data");

  Array array(ctx, array_name, TILEDB_READ);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);

  // Subarray subarray(ctx, array);
  // subarray.add_range(0, std::string("a"), std::string("z"));
  // query.set_subarray(subarray);

  uint64_t up_limit = 1048576;
  std::vector<char> contig(up_limit);
  std::vector<uint64_t> contig_offsets(up_limit / 8);
  std::vector<uint32_t> real_start_pos(up_limit / sizeof(uint32_t));
  query.set_data_buffer("contig", contig.data(), contig.size());
  query.set_offsets_buffer(
      "contig", contig_offsets.data(), contig_offsets.size());
  query.set_data_buffer(
      "real_start_pos", real_start_pos.data(), real_start_pos.size());
  query.set_layout(TILEDB_UNORDERED);

  Query::Status status;
  do {
    query.submit();
    status = query.query_status();
    std::cout << "read 1 chunk" << std::endl;
    // query.set_data_buffer("contig", contig.data(), contig.size());
    // query.set_offsets_buffer("contig", contig_offsets.data(),
    // contig_offsets.size()); query.set_data_buffer("real_start_pos",
    // real_start_pos.data(), real_start_pos.size());
  } while (status == Query::Status::INCOMPLETE);

  std::cout << query.query_status() << std::endl;
  std::cout << query.has_results() << std::endl;

  return 0;
}