This sample copies data from one gemfire system to another over a TCP connection.   This is sometimes useful when we want to keep a system running and migrate the data to another system.

Data will only flow in one direction and for only the regions listed in the source command.   It is possible to run more then one source command if we are aggregating more then one system into a single GemFire system.

The process uses a GemFire capability called CQ to receive the initial data set and then listen for any changes.   On the recieving side the process just does single puts.


How to build

```
cd <project root dir>
./gradlew installApp
```

This creates a self contained directory that can be zipped up and copied to a target environment.
```
<project home>/build/install/
```

There are two commands in the bin directory
* destination.sh - this will be the "client" to the destination grid
* source.sh - this will be the "client" to the source grid

To keep the project simple there are some assumptions in the code.
1. The destination process must be up and running before the source is started
2. Any domain objects implement serializable.
3. Any GemFire JSON objects are transmitted from the source to the destination as a string.    The destination will know that this string was originally a JSON Object and will insert it in the destination as GemFire JSON object.


Launching the destination client:
```
voltron:bin cblack$ ./destination.sh locator=localhost[10344] destinationPort=50505
```

Launching the source client:
```
voltron:bin cblack$ ./source.sh locator=localhost[10334] regions=TestData,OtherRegion destination=localhost[50505]
```

From the source side the application will output on occasion a time stamp and some region stats.   Using this data we can compare the stats with the destination to see where we are at with the copying process.

```
done with regionName = TestData
done with regionName = OtherRegion
*** Fri Dec 23 16:36:20 PST 2016
pdxRegions.keySetOnServer().size() = 1248
Region - TestData size = 1000
Region - PdxTypes size = 1248
Region - OtherRegion size = 10000
```
