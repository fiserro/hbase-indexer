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
    echo "Pass 3 arguments: <table_name> <inexer_name> <morphline_conf>"
    exit 1
fi

TEMP_FILE=/tmp/indexer_conf.xml

./bin/zkmorph.sh $1 $3 $TEMP_FILE
./bin/hbase-indexer update-indexer -n $2 -c $TEMP_FILE -z c-sencha-s01,c-sencha-s02,c-sencha-s03

