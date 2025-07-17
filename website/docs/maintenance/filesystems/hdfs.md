---
title: HDFS
sidebar_position: 2
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

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# HDFS
[HDFS (Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) is the primary storage system used by Hadoop applications. Fluss
supports HDFS as a remote storage.


## Configurations setup

To enabled HDFS as remote storage, you need to define the hdfs path as remote storage in Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: hdfs://namenode:50010/path/to/remote/storage
```

To allow for easy adoption, you can use the same configuration keys in Fluss' server.yaml as in Hadoop's `core-site.xml`.
You can see the configuration keys in Hadoop's [`core-site.xml`](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml).

#### Hadoop Environment Configuration

To use the machine hadoop environment, instead of Fluss' embedded Hadoop, follow these steps:

**Step 1: Set Hadoop Classpath**
```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

**Step 2: Add the following to your configuration file**
```yaml
plugin.classloader.parent-first-patterns.default: java.,com.alibaba.fluss.,javax.annotation.,org.slf4j,org.apache.log4j,org.apache.logging,org.apache.commons.logging,ch.qos.logback,hdfs-site,core-site,org.apache.hadoop.,META-INF
```






