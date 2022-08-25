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

namespace c2 {

ParquetWriterOptions::ParquetWriterOptions()
    : rowgroup_size(1 << 20), diskpage_size(1 << 9) {}

ParquetWriter::ParquetWriter(const ParquetWriterOptions& options,
                             const std::string& filename)
    : rowgrouprows_(options.rowgroup_size),
      rowsize_(1),
      rg_writer_(NULLPTR),
      options_(options),
      filename_(filename) {}

namespace {
int64_t SetupSchema(parquet::schema::NodeVector* fields) {
  fields->push_back(parquet::schema::PrimitiveNode::Make(
      "ID", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::UINT_64));
  fields->push_back(parquet::schema::PrimitiveNode::Make(
      "x", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
      parquet::ConvertedType::NONE));
  fields->push_back(parquet::schema::PrimitiveNode::Make(
      "y", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
      parquet::ConvertedType::NONE));
  fields->push_back(parquet::schema::PrimitiveNode::Make(
      "z", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
      parquet::ConvertedType::NONE));
  fields->push_back(parquet::schema::PrimitiveNode::Make(
      "ke", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
      parquet::ConvertedType::NONE));
  return 24;
}

int64_t CalculateRowGroupSize(const ParquetWriterOptions& options,
                              const parquet::schema::NodeVector& fields,
                              const int64_t rowsize) {
  int64_t result = INT64_MAX;
  const int64_t t0 = options.rowgroup_size / options.diskpage_size;
  const int64_t t = t0 - 1;
  for (int i = 0; i < fields.size(); ++i) {
    const int64_t s = GetTypeByteSize(
        std::static_pointer_cast<parquet::schema::PrimitiveNode>(fields[i])
            ->physical_type());
    int64_t n = (t * s / rowsize - 1) * options.diskpage_size / s;
    if (n < result) {
      result = n;
    }
  }
  return result;
}

}  // namespace

void ParquetWriter::Open() {
  assert(!root_writer_);
  assert(!filename_.empty());
  assert(!file_);
  PARQUET_ASSIGN_OR_THROW(file_, arrow::io::FileOutputStream::Open(filename_));
  parquet::WriterProperties::Builder builder;
  builder.encoding(parquet::Encoding::PLAIN);
  builder.disable_dictionary();
  builder.data_pagesize(options_.rowgroup_size);
  std::shared_ptr<parquet::WriterProperties> props = builder.build();
  rowsize_ = SetupSchema(&rowfields_);
  rowgrouprows_ = CalculateRowGroupSize(options_, rowfields_, rowsize_);
  std::shared_ptr<parquet::schema::GroupNode> schema =
      std::static_pointer_cast<parquet::schema::GroupNode>(
          parquet::schema::GroupNode::Make(
              "particle", parquet::Repetition::REQUIRED, rowfields_));
  root_writer_ = parquet::ParquetFileWriter::Open(file_, schema, props);
  rg_writer_ = NULLPTR;
}

void ParquetWriter::Add(const Particle& particle) {
  if (rg_writer_ && rg_writer_->num_rows() >= rowgrouprows_) {
    InternalFlush();
  }
  if (!rg_writer_) {
    rg_writer_ = root_writer_->AppendBufferedRowGroup();
  }
  int64_t id = particle.id;
  static_cast<parquet::Int64Writer*>(rg_writer_->column(0))
      ->WriteBatch(1, NULLPTR, NULLPTR, &id);
  static_cast<parquet::FloatWriter*>(rg_writer_->column(1))
      ->WriteBatch(1, NULLPTR, NULLPTR, &particle.x);
  static_cast<parquet::FloatWriter*>(rg_writer_->column(2))
      ->WriteBatch(1, NULLPTR, NULLPTR, &particle.y);
  static_cast<parquet::FloatWriter*>(rg_writer_->column(3))
      ->WriteBatch(1, NULLPTR, NULLPTR, &particle.z);
  static_cast<parquet::FloatWriter*>(rg_writer_->column(4))
      ->WriteBatch(1, NULLPTR, NULLPTR, &particle.ke);
}

void ParquetWriter::Flush() {
  if (rg_writer_) {
    InternalFlush();
  }
}

void ParquetWriter::InternalFlush() {
  std::string padding;
  padding.reserve(options_.diskpage_size << 1);
  const int64_t base = *file_->Tell();
  const int64_t t0 = options_.rowgroup_size / options_.diskpage_size;
  const int64_t t = t0 - 1;
  for (int i = 0; i < rowfields_.size(); ++i) {
    const int64_t colbase = *file_->Tell();
    const int64_t s = GetTypeByteSize(
        std::static_pointer_cast<parquet::schema::PrimitiveNode>(rowfields_[i])
            ->physical_type());
    rg_writer_->column(i)->Close();
    int64_t colsize = t * s / rowsize_ * options_.diskpage_size;
    int64_t cursize = *file_->Tell() - colbase;
    if (cursize < colsize) {
      padding.resize(colsize - cursize, 0);
      PARQUET_THROW_NOT_OK(file_->Write(arrow::util::string_view(padding)));
    } else if (cursize == colsize) {
      // OK!
    } else {
      abort();
    }
  }
  rg_writer_->Close();
  int64_t cur = *file_->Tell() - base;
  if (cur < options_.rowgroup_size) {
    padding.resize(options_.rowgroup_size - cur, 0);
    PARQUET_THROW_NOT_OK(file_->Write(arrow::util::string_view(padding)));
  } else if (cur == options_.rowgroup_size) {
    // OK!
  } else {
    abort();
  }
  rg_writer_ = NULLPTR;
}

void ParquetWriter::Finish() {
  Flush();
  if (root_writer_) {
    root_writer_->Close();
  }
}

}  // namespace c2
