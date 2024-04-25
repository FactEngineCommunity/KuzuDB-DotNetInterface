#define KUZU_EXPORTS
#define _WIN32

%module kuzunet
%{
/* Put header files here or function declarations like below */
#include "kuzu.h"
%}

%apply short { int16_t }
%apply int { int32_t }
%apply long long { int64_t }
%apply unsigned long long { uint64_t }

%include <windows.i>
%include "kuzu.h"
