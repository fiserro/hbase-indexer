#! /usr/bin/env bash
#
#/**
# * Copyright 2013 NGDATA nv
# *
# * Based on HBase's hbase script
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

if [ $# -eq 0 ]
  then
    echo "Pass 2 arguments: <table_name> <morphline_conf>"
    exit 1
fi

TEMP_FILE=/tmp/indexer_conf.xml
./bin/zkmorph.sh $1 $2 $TEMP_FILE

cp=$(find `pwd` -name '*.jar' | tr '\n', ',')
cp=$cp$(hbase mapredcp 2>&1 | tail -1 | tr ':' ',')
echo $cp
echo -------------------------------------------------
HADOOP_CLASSPATH=$(hbase mapredcp 2>&1 | tail -1):/opt/hbase/conf:`pwd`/lib/* yarn jar `pwd`/tools/hbase-indexer-mr-*-job.jar \
-Dmapreduce.task.timeout=3000000 \
-Dhbase.client.scanner.caching=400 \
-Dmapreduce.map.cpu.vcores=8 \
-Dmapreduce.map.memory.mb=6000 \
-Dhbase.zookeeper.quorum=c-sencha-s01,c-sencha-s02,c-sencha-s03:2181 \
--libjars ${cp} \
--region-split -1 \
--log4j `pwd`/conf/log4j.properties \
--hbase-indexer-file $TEMP_FILE \
--collection twitter_v1 \
--reducers 0 \
--zk-host solr1,solr2,solr3:2181/solr \

#--hbase-start-time 1430926300000 \
#--hbase-end-time   1432049845000 \
#--hbase-start-row "\x40\x00\x00\x00\x00\x00\x00\x00" \
#--hbase-end-row   "\x80\x00\x00\x00\x00\x00\x00\x00" \
#-Dmapreduce.map.cpu.vcores=8 \
#-Dmapreduce.map.memory.mb=6000 \
