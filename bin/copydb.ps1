if (Test-Path env:JAVA) {
    $Java = $env:JAVA
} elseif (Test-Path env:JAVA_HOME) {
    $Java = Join-Path $env:JAVA_HOME "\bin\java.exe"
} else {
    $Java = 'java'
}

$BaseDir = Split-Path (Get-ChildItem $MyInvocation.MyCommand.Path).Directory.FullName
$ClassPath = "$BaseDir\lib\*"
if (Test-Path "$BaseDir\target\copydb.jar") {
    $ClassPath="$BaseDir\target\copydb.jar;$BaseDir\target\lib\*"
}

& $Java -cp $ClassPath copydb.CopyDbCli $args
