#!/bin/sh
set -e
# On macOS the /usr/bin/javac stub fails without a real JDK, so prefer Homebrew OpenJDK when installed.
if [ -x /usr/local/opt/openjdk/bin/javac ]; then
    PATH="/usr/local/opt/openjdk/bin:$PATH"
fi
rm -rf out && mkdir out
javac -d out $(find src -name "*.java")
java -cp out scheduler.SchedulerTests
