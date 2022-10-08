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

namespace c2 {

ParquetWriterOptions::ParquetWriterOptions()
    : rowgroup_size(1 << 20),
      diskpage_size(1 << 9),
      TEST_skip_scattering(false) {}

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
  const int64_t t = t0 - 2;
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

ParquetWriter::ParquetWriter(const ParquetWriterOptions& options,
                             std::shared_ptr<ScatterFileStream> file)
    : file_(new StashableOutputStream(std::move(file))),
      rg_writer_(NULLPTR),
      rg_base_(0),
      options_(options),
      max_rowgroup_rows_(options.rowgroup_size),
      row_size_(1) {
  parquet::WriterProperties::Builder builder;
  builder.encoding(parquet::Encoding::PLAIN);
  builder.disable_dictionary();
  builder.data_pagesize(options_.rowgroup_size);
  builder.enable_statistics();
  properties_ = builder.build();
  row_size_ = SetupSchema(&children_);
  max_rowgroup_rows_ = CalculateRowGroupSize(options_, children_, row_size_);
  root_ = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make(
          "particle", parquet::Repetition::REQUIRED, children_));
}

namespace {

class FileOutputStreamWrapper : public arrow::io::OutputStream {
 public:
  FileOutputStreamWrapper(std::shared_ptr<arrow::io::OutputStream> base,
                          int64_t base_offset)
      : base_(std::move(base)), base_offset_(base_offset), closed_(false) {}
  ~FileOutputStreamWrapper() override {}

  arrow::Status Close() override {
    arrow::Status s;
    closed_ = true;
    return s;
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override {
    auto r = base_->Tell();
    if (r.ok()) {
      return *r - base_offset_;
    } else {
      return r;
    }
  }
  arrow::Status Write(const void* data, int64_t nbytes) override {
    return base_->Write(data, nbytes);
  }
  using Writable::Write;

 private:
  std::shared_ptr<arrow::io::OutputStream> base_;
  int64_t base_offset_;
  bool closed_;
};

}  // namespace

void ParquetWriter::Add(const Particle& particle) {
  if (rg_writer_ && rg_writer_->num_rows() >= max_rowgroup_rows_) {
    InternalFlush();
  }
  if (!rg_writer_) {
    if (!options_.TEST_skip_scattering) {
      PARQUET_THROW_NOT_OK(file_->BeginRowGroup());
    }
    rg_base_ = *file_->Tell();
    std::shared_ptr<arrow::io::OutputStream> f(
        new FileOutputStreamWrapper(file_, rg_base_));
    writer_ = parquet::ParquetFileWriter::Open(f, root_, properties_);
    int64_t cur = *f->Tell();
    if (cur < options_.diskpage_size) {
      std::string padding(options_.diskpage_size - cur, 0);
      PARQUET_THROW_NOT_OK(file_->Write(arrow::util::string_view(padding)));
    } else {
      abort();
    }
    rg_writer_ = writer_->AppendBufferedRowGroup();
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

#define PARQUET_WRITER_DEBUG 0
namespace {
#define LLD(X) static_cast<long long>(X)
std::string EscapedStringTo(const std::string& value) {
  std::string str;
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str.push_back(c);
    } else {
      char buf[10];
      std::snprintf(buf, sizeof(buf), "\\x%02x",
                    static_cast<unsigned int>(c) & 0xff);
      str.append(buf);
    }
  }
  return str;
}

void PrintColumnChunkMetaData(const parquet::ColumnChunkMetaData& col) {
  printf("Num values: %lld\n", LLD(col.num_values()));
  printf("File offset: %lld\n", LLD(col.file_offset()));
  printf("Page offsets: %lld (data), %lld (index), %lld (dict)\n",
         LLD(col.data_page_offset()), LLD(col.index_page_offset()),
         LLD(col.dictionary_page_offset()));
  printf("Total compressed size: %lld\n", LLD(col.total_compressed_size()));
  printf("Total uncompressed size: %lld\n", LLD(col.total_uncompressed_size()));
  if (col.is_stats_set()) {
    std::shared_ptr<parquet::Statistics> s = col.statistics();
    parquet::EncodedStatistics stats = s->Encode();
    if (stats.has_max && stats.has_min) {
      printf("Range: %s-%s\n", EscapedStringTo(stats.min()).c_str(),
             EscapedStringTo(stats.max()).c_str());
    }
  }
}

void PrintRowGroupMetaData(const parquet::RowGroupMetaData& rg) {
  printf("Num columns: %d\n", rg.num_columns());
  printf("Num rows: %lld\n", LLD(rg.num_rows()));
  printf("File offset: %lld\n", LLD(rg.file_offset()));
  for (int i = 0; i < rg.num_columns(); i++) {
    printf(
        "---------------------\n"
        "> Column chunk %d\n",
        i);
    std::unique_ptr<parquet::ColumnChunkMetaData> col = rg.ColumnChunk(i);
    PrintColumnChunkMetaData(*col);
  }
  printf(".....................\n");
  printf("Total compressed size: %lld\n", LLD(rg.total_compressed_size()));
  printf("Total byte size: %lld\n", LLD(rg.total_byte_size()));
}

void PrintFileMetaData(const parquet::FileMetaData& f) {
  for (int r = 0; r < f.num_row_groups(); r++) {
    printf(
        "=====================\n"
        "> Row group %d\n",
        r);
    std::unique_ptr<parquet::RowGroupMetaData> rg = f.RowGroup(r);
    PrintRowGroupMetaData(*rg);
  }
  printf("EOF\n");
}
#undef LLD
}  // namespace

void ParquetWriter::InternalFlush() {
  std::string padding;
  padding.reserve(options_.diskpage_size << 1);
  const int64_t t0 = options_.rowgroup_size / options_.diskpage_size;
  const int64_t t = t0 - 2;
  for (int i = 0; i < children_.size(); ++i) {
    const int64_t colbase = *file_->Tell();
    const int64_t s = GetTypeByteSize(
        std::static_pointer_cast<parquet::schema::PrimitiveNode>(children_[i])
            ->physical_type());
    rg_writer_->column(i)->Close();
    int64_t colsize = t * s / row_size_ * options_.diskpage_size;
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
  rg_writer_ = NULLPTR;
  // We will stash away the parquet metadata generated from calling
  // writer_->Close() and write it after we apply the padding so that the
  // metadata can be stored at the very end of the data chunk
  file_->StashWrites();
  writer_->Close();
  {
    std::shared_ptr<parquet::FileMetaData> f = writer_->metadata();
    if (PARQUET_WRITER_DEBUG) {
      PrintFileMetaData(*f);
    }
    rg_logs_.emplace_back(rg_base_, std::move(f));
  }
  writer_.reset();
  // File position includes the data we stashed away
  int64_t cur = *file_->Tell() - rg_base_;
  file_->StashResume();
  if (cur < options_.rowgroup_size) {
    padding.resize(options_.rowgroup_size - cur, 0);
    PARQUET_THROW_NOT_OK(file_->Write(arrow::util::string_view(padding)));
  } else if (cur == options_.rowgroup_size) {
    // OK!
  } else {
    abort();
  }
  rg_base_ = 0;
  PARQUET_THROW_NOT_OK(file_->StashPop());
  if (!options_.TEST_skip_scattering) {
    PARQUET_THROW_NOT_OK(file_->EndRowGroup());
  }
}

namespace {

void BuildColumnChunk(int64_t base,
                      const parquet::ColumnChunkMetaData& metadata,
                      parquet::ColumnChunkMetaDataBuilder* bu) {
  if (metadata.is_stats_set()) {
    std::shared_ptr<parquet::Statistics> stats = metadata.statistics();
    bu->SetStatistics(stats->Encode());
  }
  std::map<parquet::Encoding::type, int32_t> empty;
  bu->Finish(metadata.num_values(), 0, 0, metadata.data_page_offset() + base,
             metadata.total_compressed_size(),
             metadata.total_uncompressed_size(), false, false, empty, empty);
}

void BuildRowGroup(int64_t base, const parquet::RowGroupMetaData& metadata,
                   parquet::RowGroupMetaDataBuilder* bu) {
  int n = metadata.num_columns();
  for (int i = 0; i < n; i++) {
    std::unique_ptr<parquet::ColumnChunkMetaData> col = metadata.ColumnChunk(i);
    BuildColumnChunk(base, *col, bu->NextColumnChunk());
  }
  bu->set_num_rows(metadata.num_rows());
  bu->Finish(metadata.total_byte_size());
}

}  // namespace

void ParquetWriter::Finish() {
  Flush();  // Force ending the current row group with potential paddings
  if (!options_.TEST_skip_scattering) {
    PARQUET_THROW_NOT_OK(file_->Finish());
  }
  parquet::SchemaDescriptor schema;
  schema.Init(root_);
  std::unique_ptr<parquet::FileMetaDataBuilder> bu =
      parquet::FileMetaDataBuilder::Make(&schema, properties_, NULLPTR);
  for (auto& it : rg_logs_) {
    const parquet::FileMetaData& f = *it.second;
    if (f.num_row_groups() != 1) {
      abort();
    }
    std::unique_ptr<parquet::RowGroupMetaData> rg = f.RowGroup(0);
    BuildRowGroup(it.first, *rg, bu->AppendRowGroup());
  }
  std::unique_ptr<parquet::FileMetaData> result = bu->Finish();
  if (PARQUET_WRITER_DEBUG) {
    PrintFileMetaData(*result);
  }
  parquet::WriteMetaDataFile(*result, file_.get());
}

}  // namespace c2
