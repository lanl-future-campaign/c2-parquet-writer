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
#include "pthread_helper.h"

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

class ParquetFormatter {
 public:
  ParquetFormatter(const std::string& in, const std::string& out);
  ~ParquetFormatter();

  void Open();
  // Return the number of particles processed
  int Go();

 private:
  Reader* reader_;
  const std::string outputname_;
  std::shared_ptr<c2::ScatterFileStream> outputfile_;
  ParquetWriter* writer_;
};

ParquetFormatter::ParquetFormatter(const std::string& in,
                                   const std::string& out)
    : reader_(new Reader(in)), outputname_(out), writer_(NULLPTR) {}

void ParquetFormatter::Open() {
  reader_->Open();
  PARQUET_ASSIGN_OR_THROW(outputfile_, ScatterFileStream::Open(outputname_));
  writer_ = new ParquetWriter(ParquetWriterOptions(), outputfile_);
  printf("max rows per group: %d\n", int(writer_->TEST_maxrowspergroup()));
}

int ParquetFormatter::Go() {
  int r = 0;
  Particle p;
  memset(&p, 0, sizeof(p));
  while (reader_->has_next()) {
    reader_->NextParticle(&p);
    writer_->Add(p);
    r++;
  }
  writer_->Finish();
  return r;
}

ParquetFormatter::~ParquetFormatter() {
  delete reader_;
  delete writer_;
}

class JobScheduler {
 public:
  explicit JobScheduler(int j);
  ~JobScheduler();
  void AddTask(const std::string& in, const std::string& out);
  void Wait();

 private:
  struct Task {
    JobScheduler* parent;
    std::string in, out;
    int nparticles;
  };
  void ReapFinished();  // REQUIRES: mu_ must have been locked
  static void RunJob(void*);
  ThreadPool* const pool_;
  // State below protected by cv_;
  std::vector<Task*> finished_tasks_;
  port::Mutex mu_;
  port::CondVar cv_;
  int bg_scheduled_;
  int bg_completed_;
};

JobScheduler::JobScheduler(int j)
    : pool_(new ThreadPool(j)), cv_(&mu_), bg_scheduled_(0), bg_completed_(0) {}

void JobScheduler::ReapFinished() {
  if (!finished_tasks_.empty()) {
    for (auto& it : finished_tasks_) {
      printf("[FROM] %s [TO] %s [WHERE] %d particles were processed\n",
             it->in.c_str(), it->out.c_str(), it->nparticles);
      delete it;
    }
    finished_tasks_.resize(0);
  }
}

void JobScheduler::Wait() {
  MutexLock ml(&mu_);
  while (bg_completed_ < bg_scheduled_) {
    cv_.Wait();
    ReapFinished();
  }
  ReapFinished();
}

void JobScheduler::AddTask(const std::string& in, const std::string& out) {
  Task* const t = new Task;
  t->parent = this;
  t->in = in;
  t->out = out;
  t->nparticles = 0;
  MutexLock ml(&mu_);
  bg_scheduled_++;
  pool_->Schedule(RunJob, t);
}

void JobScheduler::RunJob(void* arg) {
  Task* t = static_cast<Task*>(arg);
  try {
    ParquetFormatter fmt(t->in, t->out);
    fmt.Open();
    t->nparticles = fmt.Go();
  } catch (const std::exception& e) {
    fprintf(stderr, "ERROR: %s\n", e.what());
  }
  JobScheduler* const p = t->parent;
  MutexLock ml(&p->mu_);
  p->finished_tasks_.push_back(t);
  p->bg_completed_++;
  p->cv_.SignalAll();
}

JobScheduler::~JobScheduler() {
  {
    MutexLock ml(&mu_);
    while (bg_completed_ < bg_scheduled_) {
      cv_.Wait();
    }
    ReapFinished();
  }
  delete pool_;
}

}  // namespace c2

int main(int argc, char* argv[]) {
  c2::JobScheduler master(4);
  master.AddTask(argv[1], argv[2]);
  master.Wait();
  return 0;
}
