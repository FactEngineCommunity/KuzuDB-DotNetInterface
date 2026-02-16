@REM STEP 1) refresh.bat
@REM STEP 2) rebuild.bat
@REM STEP 3) overwrite.bat ** YOU ARE HERE

@REM This copies the newly created CSharp classes to the .NET project

del kuzudb-net\kuzudb-net\wrapperfiles\*.* /q
copy KuzuFiles\generated_classes\*.* kuzudb-net\kuzudb-net\wrapperfiles\ /y
copy wrapperlib\build\kuzunet.dll kuzudb-net\kuzudb-net\ /y
copy KuzuFiles\kuzu_shared.dll kuzudb-net\kuzudb-net\ /y