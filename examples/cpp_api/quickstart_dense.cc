/**
 * @file   quickstart_dense.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2023 TileDB, Inc.
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
 * When run, this program will create a simple 2D dense array on memfs,
 * write some data to it, and read a slice of the data back.
 *
 * Note: MemFS lives on a single VFS instance on a global Context object
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name_("mem://quickstart_dense_array");

// Example-global Context object.
Context ctx_;

void create_array() {
  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  Domain domain(ctx_);
  domain.add_dimension(Dimension::create<int>(ctx_, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx_, "cols", {{1, 4}}, 4));

  // The array will be dense.
  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx_, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name_, schema);
}

void write_array() {
  // Prepare some data for the array
  std::vector<int> data = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  // Open the array for writing and create the query.
  Array array(ctx_, array_name_, TILEDB_WRITE);
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  // Prepare the array for reading
  Array array(ctx_, array_name_, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  Subarray subarray(ctx_, array);
  subarray.add_range(0, 1, 2).add_range(1, 2, 4);

  // Prepare the vector that will hold the result (of size 6 elements)
  std::vector<int> data(6);

  // Prepare the query
  Query query(ctx_, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  for (auto d : data)
    std::cout << d << " ";
  std::cout << "\n";
}

int main() {
  if (Object::object(ctx_, array_name_).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  read_array();
  return 0;
}
