# Windows PowerShell: compile and run the tests
if (Test-Path out) { Remove-Item -Recurse -Force out }
New-Item -ItemType Directory out | Out-Null
javac -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
java -cp out scheduler.SchedulerTests
