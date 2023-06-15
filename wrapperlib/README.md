## Requirements
Visual Studio with "Desktop Development with C++" package installed. Cmake and ninja generator. And all of them needed to be added to the PATH in order to have direct access from terminal.

## How to create kuzucsharpwrapper.dll

We create a directory named "build" for to contain output files and enter that directory. After that
we configure the project with cmake and start compilation with ninja.

Commands to be executed in the "x64 Native Tools Command Prompt".
```
mkdir build
cd build
cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release
ninja
```
