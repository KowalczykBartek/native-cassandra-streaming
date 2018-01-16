[![](https://img.shields.io/badge/unicorn-approved-ff69b4.svg)](https://www.youtube.com/watch?v=9auOCbH5Ns4)
![][license img]

## wtf
This is ugly attemp to stream data from Apache Cassandra without external Java Driver (only Netty used - because Netty is cool !)

For now, it will perform just
```bash
SELECT * FROM SOMETABLE WHERE PARTITION=N;
```
and using page_state will try to stream entire data set from Cassandra (without driver of course).<br />

Main purpose of this project is learning Netty(because Netty is cool !) and learn how to deal with memorry inside JVM, actually I am able to achive ~0.005sec pauses for minor gc and because netty produce almost no garbage, there is no problem with old gen collection.
```bash
2018-01-16T22:26:50.736-0100: [GC (Allocation Failure) [PSYoungGen: 655360K->320K(720896K)] 809663K->154631K(1007616K), 0.0051639 secs] [Times: user=0.02 sys=0.00, real=0.01 secs] 

2018-01-16T22:27:34.790-0100: [GC (Allocation Failure) [PSYoungGen: 655680K->352K(720896K)] 809991K->154671K(1007616K), 0.0049471 secs] [Times: user=0.02 sys=0.00, real=0.00 secs] 
```

## how to run ?
Run Cassandra (expected localhost with port 9042 - so, default).<br />
You can use Setup.java to create keyspace and table - this class will also put into Cassandra some record,
next just run Server : <br />
```bash
./gradlew -PmainClass=com.directstreaming.poc.Server execute
```

and connect via plain TCP, you can use telnet or included StreamingClient

```bash
./gradlew -PmainClass=com.utils.client.StreamingClient execute
```
<br />

## links
https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
## links
https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec

## Powered by
<img src="http://normanmaurer.me/presentations/2014-netflix-netty/images/netty_logo.png" height="75" width="150">

[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg