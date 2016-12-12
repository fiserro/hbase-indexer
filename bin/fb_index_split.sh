#!/usr/bin/env bash

if [ $# -lt 2 ]
  then
    echo "Pass 2 arguments: <zk> <index_window_table_name>"
    echo "<zk>  e.g. c-sencha-s01,c-sencha-s02,c-sencha-s03"
    echo "<index_window_table_name>  e.g. fb_split_windows"
    exit 1
fi

./bin/hbase-indexer split-indexes --zookeeper $1:2181 --split-windows-table $2