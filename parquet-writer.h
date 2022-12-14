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

#include "format.h"
#include "scatter-gather.h"

#include <parquet/api/writer.h>

namespace c2 {

struct ParquetWriterOptions {
  ParquetWriterOptions();
  // Size of each parquet row group.
  // Default: 1MB
  int64_t rowgroup_size;
  // Size of a single disk page (zfs ashift)
  // Default: 512B
  int64_t diskpage_size;
  // Skip padding row groups. Alignment is not ensured when padding is skipped.
  // Default: false
  bool skip_padding;
  // Skip calling a ParquetOutputStream's BeginRowGroup(), EndRowGroup(), and
  // Finish() function calls that are required to generate scattered parquet row
  // groups. This option is mainly used by internal test code.
  // Default: false
  bool TEST_skip_scattering;
};

// A custom parquet file writer that generates fixed-sized parquet rowgroups
// while replicating per-rowgroup metadata at the end of each rowgroup. This is
// done by padding, writing each parquet rowgroup as a full-fledged parquet
// file containing only 1 rowgroup, and storing a second copy of per-rowgroup
// metadata at the end of the parquet file.
//
// Custom parquet file:
//  - parquet subfile 1 with rowgroup 1 (exact 1MB or other configured sizes)
//    - header + header padding
//    - rowgroup 1
//      - column 1 + per-column padding
//      - column 2 + per-column padding
//      ...
//      - column M + per-column padding
//      - rowgroup padding
//    - footer with metadata for rowgroup 1
//  - parquet subfile 2 with rowgroup 2 (exact 1MB or other configured sizes)
//    - header + header padding
//    - rowgroup 2
//      - column 1 + per-column padding
//      - column 2 + per-column padding
//      ...
//      - column M + per-column padding
//      - rowgroup padding
//    - footer with metadata for rowgroup 2
//  ...
//  - parquet subfile N with rowgroup N (exact 1MB or other configured sizes)
//    - header + header padding
//    - rowgroup N
//      - column 1 + per-column padding
//      - column 2 + per-column padding
//      ...
//      - column M + per-column padding
//      - rowgroup padding
//    - footer with metadata for rowgroup N
//  - header + footer with metadata for all rowgroups
class ParquetWriter {
 public:
  ParquetWriter(const ParquetWriterOptions& options,
                std::shared_ptr<ParquetOutputStream> file);
  int64_t TEST_maxrowspergroup() const { return max_rowgroup_rows_; }
  void Add(const Particle& particle);
  // Force ending the current row group. Remaining space
  // in the group will be padded.
  void Flush();
  void Finish();

 private:
  void InternalFlush();
  std::vector<std::pair<int64_t, std::shared_ptr<parquet::FileMetaData>>>
      rg_logs_;
  std::shared_ptr<StashableOutputStream> file_;
  std::shared_ptr<parquet::ParquetFileWriter> writer_;
  parquet::RowGroupWriter* rg_writer_;
  int64_t rg_base_;
  std::shared_ptr<parquet::WriterProperties> properties_;
  std::shared_ptr<parquet::schema::GroupNode> root_;
  parquet::schema::NodeVector children_;
  ParquetWriterOptions options_;
  int64_t max_rowgroup_rows_;
  int64_t row_size_;
};

}  // namespace c2
