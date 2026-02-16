@REM STEP 1) refresh.bat ** YOU ARE HERE
@REM STEP 2) rebuild.bat
@REM STEP 3) overwrite.bat

@REM Clear out old generated files before inserting new ones
@del KuzuFiles\generated_classes\*.* /Q

@REM TODO: Update this path to your local SWIG installation
@REM Get Swig from here: https://www.swig.org/download.html
"P:\Home\Tools\swigwin\swigwin-4.4.1\swig.exe" -c++ -csharp -IKuzuFiles/ -outdir "KuzuFiles/generated_classes/" -o wrapperlib/kuzu_wrap.cpp SWIG-InputFile/kuzu.i