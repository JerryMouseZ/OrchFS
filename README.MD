# [FAST'25] OrchFS (Version-0.2)

OrchFS is a research prototype file system, presented in FAST 2025 ([Rethinking the Request-to-IO Transformation Process of File Systems for Full Utilization of High-Bandwidth SSDs](https://www.usenix.org/conference/fast25/presentation/zhan)).

## Introduction:
The capacity and bandwidth of modern Solid-State Drives (SSDs) have been steadily increasing in recent years. Unfortunately, existing SSD file systems that transform user requests to memory-page aligned homogeneous block IOs have by and large failed to make full use of the superior write bandwidth of SSDs even for large writes. Our experimental analysis identifies three main root causes of this write inefficiency, namely, 1) SSD-page alignment cost, 2) page caching overhead, and 3) insufficient IO concurrency.

To fully exploit the potentials offered by modern SSDs, this paper proposes a heterogeneous-IO orchestrated file system with an alignment-based write-partition, or OrchFS, that leverages a small-size NVM (Non-Volatile Memory) to maximize SSD performance. OrchFS extends and improves the request-to-IO transformation functionality of file systems to proactively transform file-writes into SSD-page aligned SSD-IOs and/or remaining SSD-page unaligned NVM-IOs, and then to perform these IOs via their respective optimal data paths and in an explicit multi-threaded manner. To this end, OrchFS presents several novel enabling techniques, including heterogeneous-unit data layout, alignment-based file write partition, unified per-file mapping structure and embedded parallel IO engine. The experimental results show that OrchFS outperforms 1) EXT4 and F2FS on SSD, 2) NOVA, OdinFS and ArckFS on NVM, and 3) Strata, SPFS and PHFS on hybrid NVM-SSD by up to 29.76× and 6.79× in write and read performances, respectively.

Further details on the design and implementation of OrchFS can be found in the following paper. We encourage you to cite our paper as follows:
```
@inproceedings {zhan2025OrchFS,
author = {Yekang Zhan and Haichuan Hu and Xiangrui Yang and Qiang Cao and Hong Jiang and Shaohua Wang and Jie Yao},
title = {Rethinking the Request-to-IO Transformation Process of File Systems for Full Utilization of High-Bandwidth SSDs},
booktitle = {23rd USENIX Conference on File and Storage Technologies (FAST 25)},
year = {2025},
isbn = {978-1-939133-45-8},
address = {Santa Clara, CA},
pages = {69--86},
url = {https://www.usenix.org/conference/fast25/presentation/zhan},
publisher = {USENIX Association},
month = feb
}
```

## Structure:

```
root
|---- KernelFS      (OrchFS KernelFS module)
|---- LibFS     (OrchFS LibFS module)
|---- config        (OrchFS config module)
|---- util      (OrchFS other optimization module)
```
Before running OrchFS, it is necessary to ensure that there is a persistent memory (e.g., Intel Optane PM) and a high-bandwidth SSD available. The persistent memory require at least 32GiB of space and need to be configured in dax mode. We encourage the users to employ enterprise SSDs for evaluation.
Python 3.x is required on the server to run scripts that modify configuration files.

## Building OrchFS:
Assume current directory is a project root directory.

The production implementation uses one C++20 Runtime for KFS, client sessions,
SPDK completions, migration, allocation, and journal work. The old LibFS/KFS
I/O pools and blocking service bridge are no longer built.

**1. Change persistent-memory configuration**
```
python config_parameter.py [dax] [fb_flag]
```
- `[dax]`: The device file path of the dax-mode NVM. For example, `/dev/dax0.0`.
- `[fb_flag]`: This parameter can be omitted. If you will test the filebench, this parameter needs to be set to 'fb_mode_on'. Otherwise, this parameter does not need to be set.

The production SSD is selected by PCI BDF and namespace ID through the SPDK
environment described in `config/config-template.sh`; mounted devices must not
be used for formatting or destructive validation.

Here is an example for simple workload:
```
python config_parameter.py /dev/dax0.0
```

Here is an example for testing filebench:
```
python config_parameter.py /dev/dax0.0 fb_mode_on
```
Replace `[dax]` with the DAX device on the server. Every time the above two
workload modes switch, the script needs to be run.

**2. Compiling and Installing OrchFS**

Compiling requires the use of the cmake tool.
```
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
```

The build requires CMake 3.16 or newer and a C++20 compiler. By default it
builds the client, Runtime, mock integration tests, and hardware-independent
NVMe logic tests without requiring SPDK.

For the SPDK v26.01 build, runtime configuration, cooperative scheduling
contract, and destructive device-binding checklist, see
[docs/async-spdk.md](docs/async-spdk.md). The short form is:

```bash
cmake -S . -B build-spdk \
  -DORCHFS_BUILD_KFS=ON \
  -DSPDK_ROOT=/opt/orchfs/spdk \
  -DBUILD_TESTING=ON
cmake --build build-spdk -j
ctest --test-dir build-spdk --output-on-failure
```

`ORCHFS_BUILD_KFS=ON` always enables SPDK. Start `build-spdk/kfs_main` after
exporting the Runtime/SPDK configuration, and stop it with `SIGTERM` or
`SIGINT` so RPCs, migration, SPDK, and Runtime workers drain in order.

The staged 64 KiB/QD64 optimization measurements, p99 values, CPU profiles,
and final layer-gate assessment are recorded in
[docs/optimization-results-20260721.md](docs/optimization-results-20260721.md).

## Using OrchFS:

**1. Example of Micro-benchmark**
```
cd scripts
cd micro
sudo sh run_micro.sh
```
This test is used to test the performance of Sequential write and random write.
During runtime, it is necessary to ensure that all test files are in the directory `/Or`, otherwise these files will not be processed by OrchFS.

**2. Example of filebench**
```
sh compile_filebench.sh
cd scripts
cd filebench
sudo sh run_filebench.sh
```
This test is used to test the performance of `varmail` workload.

Before running Filebench, we must modify the path in the workload file. Specifically, Find 'set $dir=[path]' and modify it to 'set $dir=/Or'. 
Examples can be found in `./filebench/workloads/varmail.f` 


We will continue to maintain this repository.

TODO: 

- We are preparing more user-friendly test code samples and more automated testing scripts.
- We are enhancing the readability and modularity of the code to make it more developer-friendly.
- We will provide a method on how to write our your own test program to test OrchFS.
- Add comments and fix potential bugs.


If you have any questions and comments, please feel free to contact zhanyekang@foxmail.com. We look forward to your valuable insights.
