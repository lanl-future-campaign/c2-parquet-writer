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
#include "reader.h"

#include <algorithm>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace c2 {

class StatsCollector {
 public:
  int Add(const std::string& file);
  void Finish();

 private:
  std::vector<float> ke_;
};

int StatsCollector::Add(const std::string& file) {
  int n = 0;
  Particle p;
  memset(&p, 0, sizeof(p));
  {
    Reader r(file);
    r.Open();
    while (r.has_next()) {
      r.NextParticle(&p);
      ke_.push_back(p.ke);
      n++;
    }
  }
  printf("Processed %s: %d particles\n", file.c_str(), n);
  return n;
}

void StatsCollector::Finish() {
  printf("Sorting...\n");
  std::sort(ke_.begin(), ke_.end());
  printf("Done\n");
  const size_t n = ke_.size();
  printf("Total particles: %llu\n", static_cast<unsigned long long>(n));
  for (double r : {0.3, 0.03, 0.003, 0.0003, 0.0003, 0.00003, 0.000003}) {
    printf("%.6f: %.6f\n", r, ke_[n - n * r]);
  }
}

}  // namespace c2

void process_dir(const char* inputdir) {
  c2::StatsCollector c;
  DIR* const dir = opendir(inputdir);
  if (!dir) {
    fprintf(stderr, "Fail to open input dir %s: %s\n", inputdir,
            strerror(errno));
    exit(EXIT_FAILURE);
  }
  std::string tmpsrc = inputdir;
  const size_t tmpsrc_prefix = tmpsrc.length();
  struct dirent* entry = readdir(dir);
  while (entry) {
    if (entry->d_type == DT_REG) {
      tmpsrc.resize(tmpsrc_prefix);
      tmpsrc += "/";
      tmpsrc += entry->d_name;
      c.Add(tmpsrc);
    }
    entry = readdir(dir);
  }
  closedir(dir);
  c.Finish();
  printf("Done\n");
}

static void usage(char* argv0, const char* msg) {
  if (msg) fprintf(stderr, "%s: %s\n\n", argv0, msg);
  fprintf(stderr, "===============\n");
  fprintf(stderr, "===============\n");
  exit(EXIT_FAILURE);
}

int main(int argc, char* argv[]) {
  setlinebuf(stdout);
  char* const argv0 = argv[0];
  argc--;
  argv++;
  if (argc < 1) usage(argv0, "must specify an input dir");
  struct stat filestat;
  int r = ::stat(argv[0], &filestat);
  if (r != 0) {
    fprintf(stderr, "Fail to stat file %s: %s\n", argv[0], strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (S_ISREG(filestat.st_mode)) {
    usage(argv0, "input path must be a dir");
  } else if (S_ISDIR(filestat.st_mode)) {
    process_dir(argv[0]);
  } else {
    fprintf(stderr, "Unexpected file type: %s\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  return 0;
}
