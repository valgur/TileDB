/*
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 */

// clang-format is disabled on the first enum so that we can manually indent it
// properly.
// clang-format off
/**
 * NOTE: The values of these enums are serialized to the array schema and/or
 * fragment metadata. Therefore, the values below should never change,
 * otherwise backwards compatibility breaks.
 */
#ifdef TILEDB_QUERY_TYPE_ENUM
    /** Read query */
    TILEDB_QUERY_TYPE_ENUM(READ) = 0,
    /** Write query */
    TILEDB_QUERY_TYPE_ENUM(WRITE) = 1,
    /** Delete query */
    #if (defined(DELETE))
    // note: 'DELETE' is #define'd somewhere within windows headers as
    // something resolving to '(0x00010000L)', which causes problems with
    // query_type.h which does not qualify the 'id' like tiledb.h does.
    // #undef DELETE
    #error "'DELETE' should not be defined before now in tiledb_enum.h.\nHas it seeped out from include of windows.h somewhere that needs changing?\n(Catch2 includes have been a past culprit.)\nFind error message in tiledb_enum.h for more information."
    // If this is encountered 'too often', further consideration might be given to
    // simply qualifying the currently unqualified definition of TILEDB_QUERY_TYPE_ENUM in
    // query_type.h so 'DELETE' and any other enum items here would not collide with this
    // windows definition known to be in conflict.
    #endif
    TILEDB_QUERY_TYPE_ENUM(DELETE) = 2,
    /** Update query */
    TILEDB_QUERY_TYPE_ENUM(UPDATE) = 3,
    /** Exclusive Modification query */
    TILEDB_QUERY_TYPE_ENUM(MODIFY_EXCLUSIVE) = 4,
#endif
// clang-format on

#ifdef TILEDB_ARRAY_TYPE_ENUM
    /** Dense array */
    TILEDB_ARRAY_TYPE_ENUM(DENSE) = 0,
    /** Sparse array */
    TILEDB_ARRAY_TYPE_ENUM(SPARSE) = 1,
#endif

#ifdef TILEDB_LAYOUT_ENUM
    /** Row-major layout */
    TILEDB_LAYOUT_ENUM(ROW_MAJOR) = 0,
    /** Column-major layout */
    TILEDB_LAYOUT_ENUM(COL_MAJOR) = 1,
    /** Global-order layout */
    TILEDB_LAYOUT_ENUM(GLOBAL_ORDER) = 2,
    /** Unordered layout */
    TILEDB_LAYOUT_ENUM(UNORDERED) = 3,
    /** Hilbert layout */
    TILEDB_LAYOUT_ENUM(HILBERT) = 4,
#endif

#ifdef TILEDB_ENCRYPTION_TYPE_ENUM
    /** No encryption. */
    TILEDB_ENCRYPTION_TYPE_ENUM(NO_ENCRYPTION) = 0,
    /** AES-256-GCM encryption. */
    TILEDB_ENCRYPTION_TYPE_ENUM(AES_256_GCM) = 1,
#endif

#ifdef TILEDB_QUERY_STATUS_ENUM
    /** Query failed */
    TILEDB_QUERY_STATUS_ENUM(FAILED) = 0,
    /** Query completed (all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(COMPLETED) = 1,
    /** Query is in progress */
    TILEDB_QUERY_STATUS_ENUM(INPROGRESS) = 2,
    /** Query completed (but not all data has been read) */
    TILEDB_QUERY_STATUS_ENUM(INCOMPLETE) = 3,
    /** Query not initialized.  */
    TILEDB_QUERY_STATUS_ENUM(UNINITIALIZED) = 4,
#endif

#ifdef TILEDB_QUERY_STATUS_DETAILS_ENUM
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_NONE) = 0,
    /** User buffers are too small */
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_USER_BUFFER_SIZE) = 1,
    /** Exceeded memory budget: can resubmit without resize */
    TILEDB_QUERY_STATUS_DETAILS_ENUM(REASON_MEMORY_BUDGET) = 2,
#endif

#ifdef TILEDB_QUERY_CONDITION_OP_ENUM
    /** Less-than operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(LT) = 0,
    /** Less-than-or-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(LE) = 1,
    /** Greater-than operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(GT) = 2,
    /** Greater-than-or-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(GE) = 3,
    /** Equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(EQ) = 4,
    /** Not-equal operator */
    TILEDB_QUERY_CONDITION_OP_ENUM(NE) = 5,
#endif

#ifdef TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM
    /**'And' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(AND) = 0,
    /** 'Or' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(OR) = 1,
    /** 'Not' operator */
    TILEDB_QUERY_CONDITION_COMBINATION_OP_ENUM(NOT) = 2,
#endif

#ifdef TILEDB_SERIALIZATION_TYPE_ENUM
    /** Serialize to json */
    TILEDB_SERIALIZATION_TYPE_ENUM(JSON),
    /** Serialize to capnp */
    TILEDB_SERIALIZATION_TYPE_ENUM(CAPNP),
#endif

/** TileDB VFS mode */
#ifdef TILEDB_VFS_MODE_ENUM
    /** Read mode */
    TILEDB_VFS_MODE_ENUM(VFS_READ) = 0,
    /** Write mode */
    TILEDB_VFS_MODE_ENUM(VFS_WRITE) = 1,
    /** Append mode */
    TILEDB_VFS_MODE_ENUM(VFS_APPEND) = 2,
#endif

#ifdef TILEDB_MIME_TYPE_ENUM
    /** Unspecified MIME type*/
    TILEDB_MIME_TYPE_ENUM(MIME_AUTODETECT) = 0,
    /** image/tiff*/
    TILEDB_MIME_TYPE_ENUM(MIME_TIFF) = 1,
    /** application/pdf*/
    TILEDB_MIME_TYPE_ENUM(MIME_PDF) = 2,
#endif

#ifdef TILEDB_DATA_ORDER_ENUM
    /** Unordered dimension label. */
    TILEDB_DATA_ORDER_ENUM(UNORDERED_DATA) = 0,
    /** Ordered dimension label with increasing values. */
    TILEDB_DATA_ORDER_ENUM(INCREASING_DATA) = 1,
    /** Ordered dimension label with decreasing values. */
    TILEDB_DATA_ORDER_ENUM(DECREASING_DATA) = 2,
#endif
