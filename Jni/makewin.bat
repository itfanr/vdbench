

@echo off

rem
rem Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
rem

rem
rem Author: Henk Vandenbergh.
rem

rem
rem 32 and 64 bit Windows compile.
rem

@echo off

rem vcvarsall.bat adds to the path each time, and then adds itself again and again.....
rem This means of course at some point we'll get 'line too long' messages....
rem Just close the CMD prompt and try again
set oldpath=%path%

call "C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC\vcvarsall.bat" x86

set java="C:\Program Files (x86)\Java\jdk1.6.0_23"

set includes=/nologo /c /D WIN32 -I%java%\include\ -I%java%\include\win32\

cl.exe  %includes% snap.c
cl.exe  %includes% vdb.c
cl.exe  %includes% vdb_dv.c
cl.exe  %includes% vdbjni.c
cl.exe  %includes% vdbwin2k.c
cl.exe  %includes% win_cpu.c
cl.exe  %includes% win_pdh.c

link.exe /nologo /dll advapi32.lib Winmm.lib /out:c:\vdbench504\windows\vdbench32.dll *.obj

set path=%oldpath%




call "C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC\vcvarsall.bat" x64


cl.exe  %includes% snap.c
cl.exe  %includes% vdb.c
cl.exe  %includes% vdb_dv.c
cl.exe  %includes% vdbjni.c
cl.exe  %includes% vdbwin2k.c
cl.exe  %includes% win_cpu.c
cl.exe  %includes% win_pdh.c

link.exe /nologo /dll advapi32.lib Winmm.lib /out:c:\vdbench504\windows\vdbench64.dll *.obj

set path=%oldpath%




