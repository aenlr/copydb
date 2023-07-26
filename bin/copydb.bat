@echo off

setlocal
set ORGDIR=%CD%
cd %~dp0..
set BASEDIR=%CD%
cd %ORGDIR%
if exist "%BASEDIR%\target" (
    set CLASSPATH=%BASEDIR%\target\copydb.jar;%BASEDIR%\target\lib\*
) else (
    set CLASSPATH=%BASEDIR%\lib\*
)

if not "%JAVA%" == "" goto start
if exist "%JAVA_HOME%\bin\java.exe" set JAVA=%JAVA_HOME%\bin\java.exe
if "%JAVA%" == "" set JAVA=java

:start
%JAVA% %JAVA_OPTS% -cp %CLASSPATH% copydb.CopyDbCli %*
set ERROR_CODE=%ERRORLEVEL%
exit /B %ERROR_CODE%
