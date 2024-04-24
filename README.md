# KuzuDB-net
.Net DLL Wrapper for the KuzuDB .lib

SWIG (www.swig.org) is used to automatically generate C# wrapper code to operate over the Kuzu database .lib file.

An example SWIG CLI command to generate the files is provided (SWIG-CLI-Command.txt) (above).

# Folders Are
- KuzuDB-Net
    - Contains a C# project to create the DLL
        - Wrapper files output from SWIG are included in this project, to generate the DLL.
    - Contains a VB.Net project to test the DLL

- KuzuFiles
    - Contains copies of the Kuzu files (.h, .hpp, .lib)

- SWIG-Input File
    - Contains the .i (input file) used in the SWIG command to generate the C# wrapper files

# Precompiled DLLs Are In:
- DotNetInterface/KuzuDB-net/KuzuDB-Net/bin/Debug/

# Test Program is in
- KuzuDB-DotNetInterface/KuzuDB-net/KuzuDB-TestAndExplore

# Collaboration Welcome
Want to chip in? Sing out to contribute to this project.

# Usage (VB.Net)
Imports kuzunet

Put the following files in your .exe directory:
kuzu_shared.dll
KuzuDB.dll
kuzunet.dll
libkuzu.lib
