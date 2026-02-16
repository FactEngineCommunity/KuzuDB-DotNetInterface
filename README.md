# KuzuDB .NET Wrapper

Use the KuzuDB graph database from .NET without needing SWIG or C/C++ knowledge. This repo includes ready-to-use binaries and example projects, plus build scripts for maintainers.

## Quickstart (recommended: prebuilt binaries)

1. Download the latest release from this repo.
2. Copy these files next to your app executable (or into your output folder):
   - `KuzuDB.dll`
   - `kuzunet.dll`
   - `kuzu_shared.dll`
3. Add a reference to `KuzuDB.dll` in your .NET project.
4. Run one of the example projects to validate your setup:
   - C# example: `KuzuDB-net\ConsoleAppExample`
   - VB example: `KuzuDB-net\KuzuDB-TestAndExplore`

If your app fails to start due to missing native DLLs, see the troubleshooting section below.

## Using the library in your project

You have two options:

### Option A: Reference the binaries

1. Copy the three DLLs listed above into your app output folder.
2. Add `KuzuDB.dll` as a reference.
3. Build and run.

### Option B: Reference the source project

1. Open `KuzuDB-net\KuzuDB-TestAndExplorer.sln`.
2. Add the `KuzuDB-Net` project as a reference to your project.
3. Build the solution.

## Use the lib in VB or C#

C# Usage
```C#
using static kuzunet;
```

Visual Basic Usage
```VB
Imports kuzunet
```

See the example projects in `KuzuDB-net` for minimal working samples.

## Troubleshooting

- If you see a `DllNotFoundException`, make sure `kuzu_shared.dll` and `kuzunet.dll` are in the same folder as your executable.
- If you are using Any CPU, try building x64 to match the native libraries.
- If you use Option B, ensure the native DLLs are set to Copy to Output Directory.

## Folder structure (high level)

- `KuzuDB-net` - .NET projects (library and examples).
- `KuzuFiles` - native Kuzu files and generated C# wrapper classes.
- `SWIG-InputFile` - SWIG interface file (maintainers only).
- `wrapperlib` - C++ wrapper build output (maintainers only).

## Build from source (maintainers)

If you need to rebuild the wrapper or native DLLs, see [README-TOO.md](README-TOO.md) and the batch scripts in the repo root.
