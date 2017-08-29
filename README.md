This sample copies data from one gemfire system to another over a TCP connection.   This is sometimes useful when we want to keep a system running and migrate the data contained one GemFire system to another GemFire system.

Data will only flow in one direction and for only the regions listed in the source command.   It is possible to run more then one source command if we are aggregating more then one system into a single GemFire system.   

The source process uses a GemFire capability called CQ to receive the initial data set and then listen for any changes.   On the destination side the process just does single puts as data is sent from the source.

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
If we have authentication turned on we can pass in the credentials by appending the username and password.

```
voltron:bin cblack$ ./source.sh locator=localhost[10334] regions=TestData,OtherRegion destination=localhost[50505] username=someUser password=1234567
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

```
.
├── LICENSE.txt
├── README.md
├── build - this is where the build artifacts go
│   ├── install
│   │   └── gemfire-copy
│   │       ├── bin
│   │       │   ├── destination.sh - the script to run the destination app
│   │       │   ├── gemfire-copy  - just ignore this app gradle artifact
│   │       │   ├── gemfire-copy.bat - just ignore this app gradle artifact
│   │       │   └── source.sh - the script to run the source app
│   │       └── lib - where all of the dependacies go
├── build.gradle
├── config - Some config files to test the app with on a local machine
├── gradlew - gradle is cool
├── gradlew.bat - gradle is cool
├── scripts
│   ├── clear_data.sh - clears out the GemFire server data
│   ├── shutdown_gemfire.sh - shuts down the GemFire grids
│   ├── start_gemfire.sh - Starts up both GemFire systems site a and site b
│   ├── start_locator.sh - gets called by the start GemFire script to start a locator
│   └── start_server.sh - gets called by the start GemFire script to start a server instance
└── src
    ├── main
    │   ├── dist
    │   │   └── bin
    │   │       ├── destination.sh - the src for the destination script
    │   │       └── source.sh - the src for the source script
    │   ├── java
    │   │   └── io
    │   │       └── pivotal
    │   │           └── gemfire
    │   │               └── demo
    │   │                   ├── Action.java - the message container that is sent between the source and the destination process.
    │   │                   ├── Destination.java - the destination app code
    │   │                   ├── Source.java - the source app code
    │   │                   └── ToolBox.java - some helper functions
    │   └── resources
    │       └── gemfire.properties - Some common GemFire properties
    └── test
        └── resources
            └── spring
                └── spring-client.xml - At one point this was all spring so I left this in place incase I change my mind to put spring back in
```
