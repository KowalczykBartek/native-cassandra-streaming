[![](https://img.shields.io/badge/unicorn-approved-ff69b4.svg)](https://www.youtube.com/watch?v=9auOCbH5Ns4)
![][license img]

## wtf
This is ugly attemp to stream data from Apache Cassandra without external Java Driver (only Netty used - because Netty is cool !)

For now, it will perform 
```bash
SELECT * FROM SOMETABLE WHERE PARTITION=N;
```
and using page_state will try to stream all data (without driver of course).

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

## Powered by
<img src="http://normanmaurer.me/presentations/2014-netflix-netty/images/netty_logo.png" height="75" width="150">

[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg