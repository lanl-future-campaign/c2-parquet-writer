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
#include "scatter-gather.h"

#include <arrow/filesystem/localfs.h>

namespace c2 {

ScatterFileStream::ScatterFileStream(
    std::shared_ptr<arrow::io::FileOutputStream> base, const std::string& p)
    : base_(std::move(base)), prefix_(p), file_offset_(0), closed_(false) {}

arrow::Result<std::shared_ptr<ScatterFileStream>> ScatterFileStream::Open(
    const std::string& prefix) {
  std::string path = prefix;
  size_t prefixlen = prefix.size();
  arrow::fs::LocalFileSystem fs;
  auto s = fs.CreateDir(path, true);
  if (!s.ok()) {
    return s;
  }
  path.resize(prefixlen);
  path += "/root";
  auto r = arrow::io::FileOutputStream::Open(path);
  if (!r.ok()) {
    return r.status();
  }
  return std::shared_ptr<ScatterFileStream>(new ScatterFileStream(*r, prefix));
}

arrow::Status ScatterFileStream::Write(const void* data, int64_t nbytes) {
  arrow::Status s;
  if (rg_)
    s = rg_->Write(data, nbytes);
  else
    s = base_->Write(data, nbytes);
  if (s.ok()) file_offset_ += nbytes;
  return s;
}

arrow::Result<int64_t> ScatterFileStream::Tell() const { return file_offset_; }

bool ScatterFileStream::closed() const { return closed_; }

arrow::Status ScatterFileStream::BeginRowGroup() {
  auto s = CloseRowGroup();
  if (!s.ok()) {
    return s;
  }
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%lld", static_cast<long long>(file_offset_));
  std::string path = prefix_;
  path += "/rg-";
  path += tmp;
  auto r = arrow::io::FileOutputStream::Open(path);
  if (!r.ok()) {
    return r.status();
  }
  rg_ = *r;
  return s;
}

arrow::Status ScatterFileStream::CloseRowGroup() {
  arrow::Status s;
  if (rg_) {
    s = rg_->Close();
    rg_ = NULLPTR;
  }
  return s;
}

arrow::Status ScatterFileStream::Close() {
  closed_ = true;
  if (rg_) {
    auto s = rg_->Close();
    if (!s.ok()) {
      return s;
    }
  }
  return base_->Close();
}

ScatterFileStream::~ScatterFileStream() {}

}  // namespace c2
