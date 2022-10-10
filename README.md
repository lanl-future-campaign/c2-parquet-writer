**Early experimental code for converting VPIC particle dataset to RAID-aligned Apache Parquet files**

[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

C2 parquet writer
================

```
XX              XXXXX XXX         XX XX           XX       XX XX XXX         XXX
XX             XXX XX XXXX        XX XX           XX       XX XX    XX     XX   XX
XX            XX   XX XX XX       XX XX           XX       XX XX      XX XX       XX
XX           XX    XX XX  XX      XX XX           XX       XX XX      XX XX       XX
XX          XX     XX XX   XX     XX XX           XX XXXXX XX XX      XX XX       XX
XX         XX      XX XX    XX    XX XX           XX       XX XX     XX  XX
XX        XX       XX XX     XX   XX XX           XX       XX XX    XX   XX
XX       XX XX XX XXX XX      XX  XX XX           XX XXXXX XX XX XXX     XX       XX
XX      XX         XX XX       XX XX XX           XX       XX XX         XX       XX
XX     XX          XX XX        X XX XX           XX       XX XX         XX       XX
XX    XX           XX XX          XX XX           XX       XX XX          XX     XX
XXXX XX            XX XX          XX XXXXXXXXXX   XX       XX XX            XXXXXX
```

C2 is developed under U.S. Government contract 89233218CNA000001 for Los Alamos National Laboratory (LANL), which is operated by Triad National Security, LLC for the U.S. Department of Energy/National Nuclear Security Administration. See the accompanying LICENSE.txt for further information.
Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. Please visit https://parquet.apache.org/ for more information.

# Software requirements

C2's custom parquet writer is written in C++ and relies on Apache Arrow for underlying writer implementation that writes data in Apache Parquet format. To install Apache Arrow, visit https://arrow.apache.org/install/ for instructions. On MacOS, Apache Arrow can be directly installed via Homebrew.

```bash
brew install apache-arrow
```

# Building

Use the following to build C2's custom parquet writer on a Mac.

```bash
git clone https://github.com/lanl-future-campaign/c2-parquet-writer.git
cd c2-parquet-writer
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DOPENSSL_ROOT_DIR=/usr/local/Cellar/openssl@1.1/1.1.1q ..
make
```

# Sample VPIC dataset

A sample VPIC dataset can be found at https://github.com/lanl-future-campaign/c2-vpic-sample-dataset. C2's custom parquet writer can be used to transform plain VPIC data into Apache Parquet.

```bash
c2-parquet-writer/build/writer -s 1 -j 8 c2-vpic-sample-dataset/particles /tmp
[FROM] c2-vpic-sample-dataset/particles/eparticle.192.0.bin [TO] /tmp/eparticle.192.0.bin.parquet [WHERE] 131072 particles were processed
[FROM] c2-vpic-sample-dataset/particles/eparticle.216.0.bin [TO] /tmp/eparticle.216.0.bin.parquet [WHERE] 131072 particles were processed
...
Done
```

Here we used `-j 8` to run 8 worker threads concurrently and `-s 1` to skip RAID alignment so that the generated Parquet file can be directly read by a standard Parquet reader without requiring a shim layer to transform file views.

```bash
/usr/local/bin/parquet meta /tmp/eparticle.0.0.bin.parquet

File path:  /tmp/eparticle.0.0.bin.parquet
Created by: parquet-cpp-arrow version 9.0.0
Properties: (none)
Schema:
message particle {
  required int64 ID (INTEGER(64,false));
  required float x;
  required float y;
  required float z;
  required float ke;
}


Row group 0:  count: 43520  24.01 B records  start: 4  total(compressed): 1020.245 kB total(uncompressed):1020.245 kB 
--------------------------------------------------------------------------------
    type      encodings count     avg size   nulls   min / max
ID  INT64     _         43520     8.00 B     0       "0" / "43519"
x   FLOAT     _         43520     4.00 B     0       "-5.0542088" / "5.7554708"
y   FLOAT     _         43520     4.00 B     0       "3.6795437E-4" / "15.999921"
z   FLOAT     _         43520     4.00 B     0       "1.2874603E-4" / "15.999313"
ke  FLOAT     _         43520     4.00 B     0       "5.4070908E-5" / "0.4867184"

Row group 1:  count: 43520  24.01 B records  start: 1045605  total(compressed): 1020.245 kB total(uncompressed):1020.245 kB 
--------------------------------------------------------------------------------
    type      encodings count     avg size   nulls   min / max
ID  INT64     _         43520     8.00 B     0       "43520" / "87039"
x   FLOAT     _         43520     4.00 B     0       "-5.6652737" / "5.755508"
y   FLOAT     _         43520     4.00 B     0       "7.5365603E-4" / "15.999394"
z   FLOAT     _         43520     4.00 B     0       "5.040169E-4" / "15.999879"
ke  FLOAT     _         43520     4.00 B     0       "6.2657266E-5" / "0.44745767"

Row group 2:  count: 43520  24.01 B records  start: 2091206  total(compressed): 1020.245 kB total(uncompressed):1020.245 kB 
--------------------------------------------------------------------------------
    type      encodings count     avg size   nulls   min / max
ID  INT64     _         43520     8.00 B     0       "87040" / "130559"
x   FLOAT     _         43520     4.00 B     0       "-6.7915874" / "5.364933"
y   FLOAT     _         43520     4.00 B     0       "0.0012398064" / "15.999388"
z   FLOAT     _         43520     4.00 B     0       "2.9325485E-4" / "15.999979"
ke  FLOAT     _         43520     4.00 B     0       "3.6562073E-5" / "0.4704048"

Row group 3:  count: 512  24.46 B records  start: 3136807  total(compressed): 12.230 kB total(uncompressed):12.230 kB 
--------------------------------------------------------------------------------
    type      encodings count     avg size   nulls   min / max
ID  INT64     _         512       8.09 B     0       "130560" / "131071"
x   FLOAT     _         512       4.09 B     0       "-3.3228176" / "3.0983238"
y   FLOAT     _         512       4.09 B     0       "0.046564244" / "15.985233"
z   FLOAT     _         512       4.09 B     0       "0.051451206" / "15.957799"
ke  FLOAT     _         512       4.09 B     0       "5.1077246E-4" / "0.28176078"
```

Thanks!


