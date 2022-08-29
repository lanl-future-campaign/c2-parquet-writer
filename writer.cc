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
#include "parquet-writer.h"

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stddef.h>
#include <stdexcept>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>

namespace c2 {
// A simple wrapper over C FILE for particle reading.
// Implementation is not thread safe.
class Reader {
 public:
  explicit Reader(const std::string& filename);
  ~Reader();
  void Open();
  void NextParticle(Particle* particle);
  bool has_next() const { return (ftell(file_) + 48) < file_size_; }

 private:
  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader& reader);
  const std::string filename_;
  size_t file_size_;
  FILE* file_;
};

template <typename T>
inline void Decode(FILE* file, T* result) {
  int r = fread(result, sizeof(T), 1, file);
  if (r != 1) {
    char tmp[100];
    snprintf(tmp, sizeof(tmp), "Error reading data from file: %s\n",
             strerror(errno));
    throw std::runtime_error(tmp);
  }
}

Reader::Reader(const std::string& filename)
    : filename_(filename), file_size_(0), file_(NULL) {}

void Reader::Open() {
  if (file_) {
    return;
  }
  file_ = fopen(filename_.c_str(), "r");
  if (!file_) {
    char tmp[100];
    snprintf(tmp, sizeof(tmp), "Fail to open file %s: %s\n", filename_.c_str(),
             strerror(errno));
    throw std::runtime_error(tmp);
  }
  int r = fseek(file_, 0, SEEK_END);
  if (r != 0) {
    char tmp[100];
    snprintf(tmp, sizeof(tmp), "Error seeking to the end of file: %s\n",
             strerror(errno));
    throw std::runtime_error(tmp);
  }
  file_size_ = ftell(file_);
  rewind(file_);
}

void Reader::NextParticle(Particle* particle) {
  assert(file_);
  uint64_t ignored_padding;
  Decode(file_, &particle->id);
  Decode(file_, &ignored_padding);
  Decode(file_, &particle->x);
  Decode(file_, &particle->y);
  Decode(file_, &particle->z);
  Decode(file_, &particle->i);
  Decode(file_, &particle->ux);
  Decode(file_, &particle->uy);
  Decode(file_, &particle->uz);
  Decode(file_, &particle->ke);
}

Reader::~Reader() {
  if (file_) {
    fclose(file_);
  }
}

}  // namespace c2

int main(int argc, char* argv[]) {
  c2::Particle p;
  memset(&p, 0, sizeof(p));
  std::shared_ptr<c2::ScatterFileStream> file;
  PARQUET_ASSIGN_OR_THROW(file, c2::ScatterFileStream::Open("xyz.parquet"));
  c2::ParquetWriterOptions options;
  options.TEST_skip_scattering = true;
  c2::ParquetWriter writer(options, std::move(file));
  const int64_t rows = 10;
  for (int64_t i = 0; i < rows; i++) {
    p.id = i;
    writer.Add(p);
  }
  writer.Finish();
  return 0;
}
