## Requirements
Swig tool

1- Step is to create the source code of bindings with swig tool

```
 swig -c++ -csharp -IKuzuFiles/ -outdir "KuzuFiles/generated_classes/" -o wrapperlib/kuzu_wrap.cpp SWIG-InputFile/kuzu.i
```

After we execute the above command without any error it will generate the C# classes into csharclasses folder and kuzu_wrap.cpp into wrapperlib folder.

2- Now we will change the directory into wrapperlib and build the project and create the DLL for the C# bindings. After the build finishes successfully, there will be a dll file named "kuzucsharpwrapper.dll".

3- Now, we can create a csharpproject in visual studio, add all the classes in csharpclasses folder into our project, and add "kuzunet.dll" and "kuzu_shared.dll" to the project as existing files. Then modify the properties of both of dlls from solution manager and choose "Copy if newer" option for copy.

4- Now add your VB.net project to the solution, and then add the C# project as a dependency to the project. Please note that the module name in kuzu.i project is the name of the package in C#. 

```
Imports kuzunet
```

