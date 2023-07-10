/**
 * @file flow_graph/library/basic/basic_execution_platform.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 */

#ifndef TILEDB_FLOW_GRAPH_MINIMAL_EXECUTION_PLATFORM_H
#define TILEDB_FLOW_GRAPH_MINIMAL_EXECUTION_PLATFORM_H

#include "basic_execution_platform.h"

namespace tiledb::flow_graph {

/**
 * The minimal execution platform provides a placeholder for concept checking.
 *
 * Maturity Note: As is obvious from the definition, this definition is not
 * yet minimal.
 */
using MinimalExecutionPlatform = BasicExecutionPlatform;

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_MINIMAL_EXECUTION_PLATFORM_H
