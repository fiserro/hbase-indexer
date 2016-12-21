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

./bin/hbase-indexer split-indexes --zookeeper $1:2181 --split-windows-table $2 --command $3 ${@:4}
