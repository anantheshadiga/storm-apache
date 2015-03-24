# Storm Ganglia JMX Monitoring
This project provides the libraries and sample configuration for publishing JVM metrics from Storm to Ganglia.

## Overview
The `lib/jmxetric-1.0.4.jar` provides a java agent implementation that can be used to push JVM metrics to ganglia. The `lib/gmetric4j-1.0.3` and `lib/oncrpc-1.0.7` are dependencies that should be placed in the same directory as the jmxetric jar.

The `conf/jmxetric-conf.xml` is a basic configuration file that tells jmxetric which JMX MBean attributes to publish to ganglia.

## Installation
The following installation steps should be performed on every Nimbus and Supervisor node in the Storm cluster.

Beging by copying the `lib` and `conf` directories to a directory such as `/var/lib/storm-jmxetric`. We will refer to this directory as `$JMXETRIC_HOME`.

## Configuration
First, configure Storm to start the nimbus, supervisor, and worker processes with the jmxetric agent enabled. Edit the `$STORM_HOME/conf/storm.yaml` and update the following sections accordingly. Replace `$JMXETRIC_HOME` with the absolute path of the directory containing the jmxetric files.

Replace `ganglia` with the hostname of the `gmond` instance that is listening for metrics (usually the `gmond` instance running on the `gmetad` server).

The config below assumes ganglia is configured in unicast mode. If the ganglia environment is configured to use multicast, replace `mode=unicast` to `mode=multicast`.

```
supervisor.childopts: "-javaagent:$JMXETRIC_HOME/lib/jmxetric-1.0.4.jar=host=ganglia,port=8649,wireformat31x=true,mode=unicast,config=$JMXETRIC_HOME/conf/jmxetric-conf.xml,process=Supervisor_JVM -Xmx256m"

nimbus.childopts: "-javaagent:$JMXETRIC_HOME/lib/jmxetric-1.0.4.jar=host=ganglia,port=8649,wireformat31x=true,mode=unicast,config=$JMXETRIC_HOME/conf/jmxetric-conf.xml,process=Nimbus_JVM -Xmx1024m"

worker.childopts: "-javaagent:$JMXETRIC_HOME/lib/jmxetric-1.0.4.jar=host=ganglia,port=8649,wireformat31x=true,mode=unicast,config=$JMXETRIC_HOME/conf/jmxetric-conf.xml,process=Worker_%ID%_JVM -Xmx768m"
```

### Publishing Additional JMX Metrics
Every MBean attribute you want to be published to ganglia must be defined in the $JMXETRIC_HOME/conf/jmxetric-conf.xml file.

For example, the following XML will cause the `ThreadCount` and `DaemonThreadCount` attributes of the `java.lang:type=Threading` MBean to be published to ganglia:

```xml
<mbean name="java.lang:type=Threading" pname="Threading" >
    <attribute name="ThreadCount" type="int16" />
    <attribute name="DaemonThreadCount" type="int16" />
</mbean>
```
