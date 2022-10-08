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

#include <arrow/io/file.h>

namespace c2 {

class ParquetOutputStream : public arrow::io::OutputStream {
 public:
  ParquetOutputStream() {}
  // Clients are expected to call 0, 1, or more pairs of BeginRowGroup()
  // and EndRowGroup(), followed by a single Finish().
  virtual arrow::Status BeginRowGroup() = 0;
  virtual arrow::Status EndRowGroup() = 0;
  virtual arrow::Status Finish() = 0;
};

class StashableOutputStream : public ParquetOutputStream {
 public:
  StashableOutputStream(std::shared_ptr<ParquetOutputStream> base);
  arrow::Status Close() override;
  bool closed() const override;
  // Stash incoming writes until StashResume(). Stashed writes are buffered in
  // memory while continuing to affect file size and be reflected at subsequent
  // Tell() function calls.
  void StashWrites();
  arrow::Result<int64_t> Tell() const override;
  arrow::Status BeginRowGroup() override;
  arrow::Status EndRowGroup() override;
  arrow::Status Finish() override;
  // Resume writing. Future writes are no longer stashed. Previously stashed
  // writes are not applied. They are applied only through StashPop().
  void StashResume();
  const std::string& StashGet() const;
  // Apply stashed writes.
  arrow::Status StashPop();
  arrow::Status Write(const void* data, int64_t nbytes) override;
  using Writable::Write;

 private:
  arrow::Status DoWrite(const void* data, int64_t nbytes);
  std::shared_ptr<ParquetOutputStream> base_;
  bool is_stash_enabled_;
  std::string stash_;
  int64_t file_offset_;
  bool closed_;
};

}  // namespace c2
