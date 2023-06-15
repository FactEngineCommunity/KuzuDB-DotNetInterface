## Required tools
Visual Studio with "Desktop Development with C++" package installed. Cmake and ninja generator. And all of them needed to be added to the PATH in order to have direct access from terminal.

1- We clone the repository with the command below:
```
git clone --recursive https://github.com/kuzudb/kuzu/tree/master
```

2- After cloning we are going to build it. But before the compilation of kuzu itself, as far as i understood, kuzu depends on Arrow and Parquet libraries which are not optional. So we either need to install them in someway or compile them. I wasn't able to find their binaries for windows so I've compiled them. Luckly, they have included them in "external/" directory.

Depending on your target architecture open "x86 Native Tools Command Prompt" or "x64 Native Tools Command Prompt". These shortcuts will become available after you've installed "Desktop Devel. with C++` package in Visual Studio. FYI, I've Visual Studio 2019. After opening command promt we start compilation of arrow and paraquet with the following commands.

```
cd external
mkdir build
cd build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja
```

After it finishes building, we change directory to the root of the repository, and create another build directory for the compilation of kuzu, and start building following these commands:

```
cd ..\..
mkdir build
cd build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja
```