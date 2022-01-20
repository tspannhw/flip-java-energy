# Overview

Using Java to send data to Pulsar to InfluxDB v2.1.1 - InfluxCloud

Energy Data

# Prerequisites

- Java 1.8 or higher version
- Java Client: 2.9.1

# Details

## Create a Standaone Apache Pulsar 2.9.1 Cluster or Use StreamNative Cloud

## Download InfluxDB Connector for 2.9.1

https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.9.1/connectors/pulsar-io-influxdb-2.9.1.nar

## Create a InfluxDB Cloud Account

## Create a Bucket

pulsar

## Create A Token

https://us-Location.aws.cloud2.influxdata.com/notebook/from/bucket/pulsar

## Create a Pulsar IO Connector for InfluxDB v2 Sink

## Config conf/influxcloud.yml

### Note:   preceision MS and Log Level BASIC are needed or things won't work

````
configs:
    influxdbUrl: "https://us-east-1-1.aws.cloud2.influxdata.com"
    organization: "tim.spann@streamnative.io"
    bucket: "pulsar"
    token: "2THISISVERYLONGGENERATEinCLOUDog=="
    logLevel: "BASIC"
    precision: "ms"

````

## Deploy the Connector (Let's stop and delete if we already have one)

````
bin/pulsar-admin sink stop --name influxdb-sink-jetson --namespace default --tenant public

bin/pulsar-admin sinks delete --tenant public --namespace default --name influxdb-sink-jetson

bin/pulsar-admin sinks create --archive ./connectors/pulsar-io-influxdb-2.9.1.nar --tenant public --namespace default --name influxdb-sink-jetson --sink-config-file conf/influxcloud.yml --inputs persistent://public/default/energy-influx --parallelism 1

bin/pulsar-admin sinks get --tenant public --namespace default --name influxdb-sink-jetson

bin/pulsar-admin sinks status --tenant public --namespace default --name influxdb-sink-jetson

````

## Test Consume Messages

````

bin/pulsar-client consume "persistent://public/default/energy-influx" -s "influxr" -n 0

````

## Check the Sink

````
bin/pulsar-admin sinks get --tenant public --namespace default --name influxdb-sink-jetson
````

## Get Sink Counts and Status

````
bin/pulsar-admin sinks status --tenant public --namespace default --name influxdb-sink-jetson
````

## Run Java 

````
java -jar /opt/demo/energy/EnergyProducer-1.0-jar-with-dependencies.jar --serviceUrl pulsar://pulsar1:6650 --topic 'persistent://public/default/energy-influx'
````

