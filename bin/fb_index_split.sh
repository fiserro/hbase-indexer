#!/usr/bin/env bash

if [ $# -lt 3 ]
  then
    echo "Pass 3 arguments: <zk> <index_window_table_name> <command> [<command options>]"
    echo "<zk>  e.g. c-sencha-s01,c-sencha-s02,c-sencha-s03"
    echo "<index_window_table_name>  e.g. fb_split_windows"
    echo "<command> split or addwindow"
    echo "e.g.:"
    echo "$0 localhost fb_split_windows split --dry-run true"
    echo "$0 localhost fb_split_windows addwindow --window facebook_v2_00000,facebook_v2_00001"
    exit 1
fi

#export HADOOP_USER_CLASSPATH_FIRST=true; export HADOOP_CLASSPATH=/opt/hbase/conf:`pwd`/lib/*:$(hbase mapredcp 2>&1 | tail -1)
cp=$(find `pwd` -name '*.jar' | tr '\n', ',')
./bin/hbase-indexer split-indexes --zookeeper $1:2181 --split-windows-table $2 --command $3 ${@:4}./bin/hbase-indexer split-indexes --zookeeper $1:2181 --split-windows-table $2 --command $3 ${@:4} \
--reindex-args "-Dmapreduce.user.classpath.first=true -Dmapreduce.job.user.classpath.first=true -Dyarn.resourcemanager.address=c-sencha-s02:8032 -Dmapreduce.task.timeout=3000000 -Dhbase.client.scanner.caching=5000 -Dmapreduce.map.speculative=false -Dmapreduce.map.cpu.vcores=1 --libjars ${cp}"