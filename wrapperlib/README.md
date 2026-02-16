## Purpose
This folder builds the native `kuzunet.dll` wrapper. End users typically do not need this; use the prebuilt binaries from the repo releases instead.

## Requirements
- Visual Studio with the "Desktop Development with C++" workload
- CMake and Ninja available in the VS environment

## Build (x64 Native Tools Command Prompt)
```
mkdir build
cd build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja
```
