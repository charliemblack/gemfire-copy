This sample copies data from one gemfire system to another over a TCP connection.   This is sometimes useful when we want to keep a system running and migrate the data to another system.

Data will only flow in one direction and for only the regions listed in the source command.   It is possible to run more then one source command if we are aggregating more then one system into a single GemFire system.

How to build

```
cd <project root dir>
./gradlew installApp
```

This creates a self contained directory that can be zipped up and copied to a target environment.
```
<project home>/build/install/gemfire-copy
```

There are two commands in the gemfire-copy/bin directory
* destination.sh - this will be the "client" to the destination grid
* source.sh - this will be the "client" to the source grid

To keep the project simple there are some assumptions in the code.
1. The destination process must be up and running before the source is started



Launching the destination client:
```
voltron:bin cblack$ ./destination.sh locator=localhost[10344] destinationPort=50505
```

Launching the source client:
```
voltron:bin cblack$ ./source.sh locator=localhost[10334] regions=TestData,OtherRegion destination=localhost[50505]
```
