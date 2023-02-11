/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include "writables.h"

namespace c2 {

struct ScatterFileStreamOptions {
  ScatterFileStreamOptions();
  // Byte size for each row group batch
  // Default: 4MB
  int64_t fragment_size;
  // Fragments are padded unless the following is true.
  // Padding may be skipped when all fragments are known to consume at least two
  // zfs records, in which case zfs will perform the padding for us.
  // Default: false
  bool skip_padding;
};

class ScatterFileStream : public ParquetOutputStream {
 public:
  ~ScatterFileStream() override;
  static arrow::Result<std::shared_ptr<ScatterFileStream>> Open(
      const ScatterFileStreamOptions& options, const std::string& path);

  arrow::Status Close() override;
  bool closed() const override;
  arrow::Result<int64_t> Tell() const override;

  // Clients are expected to call 0, 1, or more pairs of BeginRowGroup()
  // and EndRowGroup(), followed by a single Finish().
  arrow::Status BeginRowGroup() override;
  arrow::Status EndRowGroup() override;
  arrow::Status Finish() override;
  arrow::Status Write(const void* data, int64_t nbytes) override;
  using Writable::Write;

 private:
  arrow::Status FlushRowGroupBatch(bool force = false);
  ScatterFileStream(const ScatterFileStreamOptions& options,
                    std::shared_ptr<arrow::io::FileOutputStream> base,
                    const std::string& prefix);
  std::shared_ptr<arrow::io::FileOutputStream> base_;
  std::shared_ptr<arrow::io::FileOutputStream> rgb_;
  ScatterFileStreamOptions options_;
  std::string prefix_;
  int64_t file_offset_;
  bool closed_;
};

}  // namespace c2
