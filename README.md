# KuzuDB-net
.NET Wrapper for KuzuDB

This project contains the necessary library files to use the Kùzu graph database (https://kuzudb.com/) in .NET applications.

You can either download the prebuilt libraries (see: Get the prebuilt libaries) or build them yourself. The project uses SWIG (www.swig.org) to automatically generate C# wrapper code and Visual Studio to build the library.

# Get the prebuilt libaries

The prebuilt libraries will be published in the releases of this repo. For information on the usage, look into the General Usage section or consult the C# and VB example projects in the KuzuDB-Net folder.

# Folder structure

- KuzuDB-Net
    - Contains a C# project to create the library DLL
    - Contains an example VB.Net project to test the DLL
    - Contains an example C# project to test the DLL

- KuzuFiles
    - Kuzu files (.h, .dll, .lib, etc.) should be added here (see: Building the libraries yourself)
    - the generated_classes subfolder will hold the SWIG generated C# wrapper classes

- SWIG-Input File
    - Contains the SWIG interface file for SWIG to generate the C# wrapper files and kuzu_wrap.cpp

- wrapperlib
    - Will contain the needed files to build kuzunet.dll

# Building the libraries yourself

The build process with the code from this repo was last tested with the following setup:
- Windows 11
- Visual Studio Community 22 with "Desktop Development with C++" package and .NET Framework 4.8
- Kuzu v.0.3.2
- SWIG version 4.2.1
- cmake 3.29.2
- ninja 1.12.0

Use the following steps to build your own version of the library files:

## 1. Get the newest version of Kuzu C/C++ libs (see last tested versions above)
- create KuzuFiles dir and generated_classes inside of it
- Download the c/c++ windows lib from https://github.com/kuzudb/kuzu/releases/
- put the files(.h, .dll, .lib, etc.) in KuzuFiles folder

## 2. Use SWIG to generate class files and wrapper
- Install swig (see last tested versions above).
- Generate C# classes and kuzu_wrap.cpp by using the provided SWIG interface file with the following command

```cmd
swig -c++ -csharp -IKuzuFiles/ -outdir "KuzuFiles/generated_classes/" -o wrapperlib/kuzu_wrap.cpp SWIG-InputFile/kuzu.i
```

## 3. Build kuzunet.dll
- check that you have Visual Studio with C++ dev package, as well as cmake and kuzu as described above
- build kuzunet.dll using cmake and ninja in the wrapperlib folder with following commands:

```cmd
cd wrapperlib
mkdir build
cd build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja
```

## 4. Build and test the lib files
In the KuzuDB-net folder is a KuzuDB-TestAndExplorer.sln file. It includes three things:
- The C# project to build the datbase .dll
- A small VB project to test the outcome
- A small C# project to test the outcome

For building the libary you need the KuzuDB-Net C# project
- open the sln file
- add all the files from KuzuFiles/generated_classes to the projects WrapperFiles folder (replace the existing ones)
- add kuzu_shared.dll (from KuzuFiles) and kuzunet.dll (from wrapperlib/build) to the KuzuNet-DB project (replace the existing ones)
- select Copy if newer for the added kuzu_shared.dll and kuzunet.dll

For testing your library you need to:
- open the .sln file
- make sure the KuzuDB-TestAndExplorer project has the KuzuDB-Net project as reference
- build the solution

# General Usage

To you use the library in your .NET project, you have two options:

## Option 1: Copy the downloaded or built lib files

Add the following files to your application directory (where your .exe file is):
kuzu_shared.dll
KuzuDB.dll
kuzunet.dll

An add KuzuDB.dll as reference to your project.

## Option 2: Add the lib building project as dependency

Use Visual Studio and add KuzuDB-Net project as project reference to your project (see example projects in KuzuDB-TestAndExplorer.sln).

## Use the lib in VB or C#

C# Usage
```C#
using static kuzunet;
```

Visual Basic Usage
```VB
Imports kuzunet
```
Check the examples in KuzuDB-TestAndExplorer.sln for minimal VB and C# examples.
