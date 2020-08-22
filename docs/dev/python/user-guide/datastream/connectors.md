---
title: "Connectors"
nav-parent_id: python_datastream_api
nav-pos: 30
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->


This page describes how to use connectors in Python DataStream API and highlights the different parts between using connectors in Python vs Java/Scala. 

* This will be replaced by the TOC
{:toc}

<span class="label label-info">Note</span>For general connector information and common configuration, please refer to the corresponding [Java/Scala documentation]({{ site.baseurl }}/dev/connectors/index.html). 

## Download connector and format jars

For both connectors and formats, implementations are available as jars that need to be specified as job [dependencies]({{ site.baseurl }}/dev/python/user-guide/table/dependency_management.html).

{% highlight python %}

env.add_user_jars(["file:///my/jar/path/connector.jar", "file:///my/jar/path/json.jar"])

{% endhighlight %}

## Predefined Sources and Sinks

A few basic data sources and sinks are built into Flink and are always available. 
Currently, in Python DataStream API, the predefined sources only include the `from_collection()` and the predefined sinks only include the `print()`.

{% highlight python %}

env = StreamExecutionEnvironment.get_execution_environment()
env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')]).print()

{% endhighlight %}

A lot for predefined sources and sinks in Java/Scala are not supported in Python DataStream due to the missing of fault-tolerant or rarely used. 
You should also keep in mind that the `from_collection()` and `print()` are also not recommended to be used in production. 

## Bundled Connectors

Connectors provide code for interfacing with various third-party systems. Currently these systems are supported:

 * [Apache Kafka]({% link dev/connectors/kafka.md %}) (source/sink)
 * [Streaming FileSystem]({% link dev/connectors/streamfile_sink.md %}) (sink)
 * [JDBC]({% link dev/connectors/jdbc.md %}) (sink)
 
 
## Read and write with Table API connector

Since DataStream can be converted to/from a Table, we can also use Table API connectors to read or write data, thus we can make use of all Table API connectors.

Currently connectors supported in Table API include:

 * [Apache Kafka]({% link dev/table/connectors/kafka.md %}) (source/sink)
 * [JDBC]({% link dev/table/connectors/jdbc.md %}) (source/sink)
 * [Elasticsearch]({% link dev/table/connectors/elasticsearch.md %}) (sink)
 * [FileSystem]({% link dev/table/connectors/filesystem.md %}) (source/sink)
 * [HBase]({% link dev/table/connectors/hbase.md %}) (source/sink)
 * [DataGen]({% link dev/table/connectors/datagen.md %}) (source)
 * [Print]({% link dev/table/connectors/print.md %}) (sink)
 * [BlackHole]({% link dev/table/connectors/blackhole.md %}) (sink)
 
Take kafka as an example, the code looks like:

{% highlight python %}

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(stream_execution_environment=env)
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
# specify connector and format jars
t_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")

source_ddl = """
        CREATE TABLE source_table(
            a VARCHAR,
            b INT
        ) WITH (
          'connector.type' = 'kafka',
          'connector.version' = 'universal',
          'connector.topic' = 'source_topic',
          'connector.properties.bootstrap.servers' = 'kafka:9092',
          'connector.properties.group.id' = 'test_3',
          'connector.startup-mode' = 'latest-offset',
          'format.type' = 'json'
        )
        """

sink_ddl = """
        CREATE TABLE sink_table(
            a VARCHAR
        ) WITH (
          'connector.type' = 'kafka',
          'connector.version' = 'universal',
          'connector.topic' = 'sink_topic',
          'connector.properties.bootstrap.servers' = 'kafka:9092',
          'format.type' = 'json'
        )
        """

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

# read from Table API connector
source_table = t_env.from_path("source_table")
# convert the source Table into a DataStream for further processing
ds = t_env.to_append_stream(source_table, type_info=Types.ROW([Types.STRING(), Types.INT()]))\
    .map(lambda record: record[0], output_type=Types.STRING)

# convert the DataStream into a Table and writes the Table with Table API connector
t_env.from_data_stream(ds, ['a']).insert_into("sink_table")
t_env.execute()

{% endhighlight %}

The example, read and write data using Kafka Table connector. 
A Table can be converted to a DataStream using `StreamTableEnvironment.to_append_stream()` 
and a DataStream can be converted to a Table using `StreamTableEnvironment.from_data_stream()`.
