#!/bin/sh
ZKHOME="/Users/igor/opt/github.com/apache/zookeeper/apache-zookeeper-3.5.6"
$ZKHOME/bin/zkServer.sh start-foreground ./z2.cfg
