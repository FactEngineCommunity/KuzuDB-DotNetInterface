# KuzuDB .NET Interface - Maintainer Build Guide

This document is for contributors who need to regenerate the wrapper or rebuild the native DLLs. If you just want to use the library, see the root README.

## Quickstart (batch scripts)

Run these in order from the repo root:

```batch
.\refresh.bat
.\rebuild.bat
.\overwrite.bat
```

What each script does:
- `refresh.bat` regenerates the SWIG C# classes and `wrapperlib\kuzu_wrap.cpp`.
- `rebuild.bat` builds `kuzunet.dll` with CMake + Ninja (requires VS environment variables).
- `overwrite.bat` copies generated C# files and native DLLs into the .NET projects.

## Prerequisites

### 1. KuzuDB native libraries

Download the latest Windows release from:
https://github.com/kuzudb/kuzu/releases/

Extract `libkuzu-windows-x86_64.zip` and copy these files into `KuzuFiles`:
- `kuzu_shared.dll`
- `kuzu_shared.lib`
- `kuzu.h`
- `kuzu.hpp`

### 2. SWIG

Download and install SWIG:
https://www.swig.org/download.html

Update the path to `swig.exe` in `refresh.bat` (currently set to `p:\home\tools\swigwin\swig.exe`).

### 3. Visual Studio build tools

Install Visual Studio 2022 with the "Desktop Development with C++" workload. The build uses:
- CMake (from VS environment)
- Ninja (from VS environment)
- MSVC compiler

## Manual build (one-liner)

If you need to run the build without the batch files:

```powershell
cmd /c '"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64 && cd wrapperlib && cd build && cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release && ninja'
```
