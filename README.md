
# kafkatail
Tail for kafka brokers with support for multiple topic selection.  
Building and running requires [Jdk 8+](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).

[![Build Status](https://api.travis-ci.org/gliviu/kafkatail.svg?branch=master)](https://travis-ci.org/gliviu/kafkatail)

## Install

**Binary release**  
Binaries are available at [GitHub Release](https://github.com/gliviu/kafkatail/releases) page.
Download, unpack and add `kafkatail-1.0.0/bin` directory to `PATH`.  
Run `kafkatail -v` to check installation.

**Nodejs package**  

NodeJs [kafkatail](https://www.npmjs.com/package/kafkatail) package can be installed using `npm install -g kafkatail`.  
Use `kafkatail -v` to check successful deployment.  
Upon first run it might download a local copy of JRE if `java` executable is not available. See [jdeploy](https://github.com/shannah/jdeploy) and [node-jre](https://www.npmjs.com/package/node-jre).

**Install from source**  
```bash
git clone https://github.com/gliviu/kafkatail
cd kafkatail
./gradlew build
unzip build/distributions/kafkatail-1.0.0.zip
kafkatail-1.0.0/bin/kafkatail -v   # or kafkatail.bat -v
```

## Usage
```
Usage: kafkatail [options] [--] [topic-pattern1 topic-pattern2 ...]             

Tails multiple kafka topics and prints records to standard output as they arrive.
With no options, consumes new records from all topics at localhost:9092.
Default display format is 'partition offset timestamp topic [key] value'.
Timestamp is marked as N/A if missing from record.

Options
-b host[:port]      Broker address (default: localhost:9092)
-x exclude-patterns Topics to be excluded
-h --help           Show help
-v --version        Show version
-s --since date     Print previous records starting from date
-u --until date     Consume records until date is reached
-a --amount amount  Consume certain amount of records starting with -s, then stop

Topic selection
Topic names are matched by substring.
kafkatail                        tail all topics at localhost:9092
kafkatail emp dep                tail topics containing emp and dep in their name
kafkatail -x emp,ord             tail all topics except those containing emp, ord
kafkatail -x hist,temp -- dep    tail all topics containing dep, excluding hist, temp

Offset range
Starting kafkatail with no options will print only new records.
It is possible to show previous records by using -s -u and -a options.
kafkatail -s 10m                                   tail starting from 10 minutes ago      
kafkatail -s 1d -u 2h topic1 topic2                tail since 1 day ago until 2 hours ago
kafkatail -s 2015-01-01T10:00 -u 2015-01-01T11:00  tail between specified interval        
kafkatail -s 10:00 -u 11:00                        tail between hours 10..11 today        
kafkatail -s 10:00 -a 30m                          tail between 10:00..10:30 today        
Formats accepted by -s and -u:
* absolute local date/time - 2015-01-01T18:00, 2015-01-01T18:00:33
* today local time - 18:00, 18:00:33
* relative past time - 10m (ten minutes ago)
  accepted units: day d, hour h, minute m, second s
Amount (-a) format: 10m (ten minutes).
Options -u and -a cannot be used together.
```

# Notes
* No consumer group is created on broker while tailing
* Offset range options (-s, -u, -a) do not work well with records that don't have a timestamp attached.  
  Use producer starting with [0.10.0.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/0.10.0.0) to benefit from these options. See [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message).
