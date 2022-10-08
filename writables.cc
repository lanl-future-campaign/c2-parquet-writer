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
#include "writables.h"

namespace c2 {

StashableOutputStream::StashableOutputStream(
    std::shared_ptr<ParquetOutputStream> base)
    : base_(std::move(base)),
      is_stash_enabled_(false),
      file_offset_(0),
      closed_(false) {}

void StashableOutputStream::StashWrites() { is_stash_enabled_ = true; }

void StashableOutputStream::StashResume() { is_stash_enabled_ = false; }

const std::string& StashableOutputStream::StashGet() const { return stash_; }

arrow::Status StashableOutputStream::StashPop() {
  arrow::Status s;
  if (!stash_.empty()) {
    s = DoWrite(stash_.data(), stash_.size());
    stash_.resize(0);
  }
  return s;
}

arrow::Status StashableOutputStream::Write(const void* data, int64_t nbytes) {
  arrow::Status s;
  if (is_stash_enabled_) {
    stash_.append(static_cast<const char*>(data), nbytes);
  } else {
    s = DoWrite(data, nbytes);
  }
  return s;
}

arrow::Status StashableOutputStream::DoWrite(const void* data, int64_t nbytes) {
  arrow::Status s = base_->Write(data, nbytes);
  if (s.ok()) file_offset_ += nbytes;
  return s;
}

arrow::Status StashableOutputStream::BeginRowGroup() {
  return base_->BeginRowGroup();
}
arrow::Status StashableOutputStream::EndRowGroup() {
  return base_->EndRowGroup();
}
arrow::Status StashableOutputStream::Finish() { return base_->Finish(); }

arrow::Result<int64_t> StashableOutputStream::Tell() const {
  return file_offset_ + stash_.size();
}

bool StashableOutputStream::closed() const { return closed_; }

arrow::Status StashableOutputStream::Close() {
  closed_ = true;
  return base_->Close();
}

}  // namespace c2
