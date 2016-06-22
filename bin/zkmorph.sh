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

if [ $# -eq 0 ] ; then
	echo "Pass 3 arguments: <table> <morphline> <outputfile> [<read-row>(default 'never')]"
	exit 1
fi

READ_ROW="never"
if [ $# -gt 3 ] && [ "$4" == "dynamic" ] ; then
	echo "Using dynamic read-row mode"
	READ_ROW="dynamic"
fi

echo "writing to "$3

echo "<indexer table=\""$1"\" mapper=\"com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper\" read-row=\"$READ_ROW\">" > $3
echo "  <param name=\"morphlineString\" value=\"" >> $3
cat $2 | sed 's/\&/\&#38;/g;s/\"/\&quot;/g;s/</\&lt;/g;s/>/\&gt;/g;s/\t/\&#9;/g' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/\&#10;/g' >> $3
echo "\"/>" >> $3
echo "</indexer>" >> $3
