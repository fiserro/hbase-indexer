#!/bin/sh
#
# Copyright 2013 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#!/bin/bash -eu

#mvn clean package -DskipTests

scp ./hbase-sep/hbase-sep-impl-common/target/hbase-sep-impl-common-1.7-aplpha-2-SNAPSHOT.jar hadoop1.dev1.nag.ccl:/tmp/
scp ./hbase-sep/hbase-sep-impl/target/hbase-sep-impl-1.7-aplpha-2-SNAPSHOT.jar hadoop1.dev1.nag.ccl:/tmp/
scp ./hbase-sep/hbase-sep-api/target/hbase-sep-api-1.7-aplpha-2-SNAPSHOT.jar hadoop1.dev1.nag.ccl:/tmp/
scp ./hbase-sep/hbase-sep-tools/target/hbase-sep-tools-1.7-aplpha-2-SNAPSHOT.jar hadoop1.dev1.nag.ccl:/tmp/

ssh hadoop1.dev1.nag.ccl 'sudo service hbase-regionserver stop'
ssh hadoop1.dev1.nag.ccl 'sudo service hbase-indexer stop'
ssh hadoop1.dev1.nag.ccl 'sudo mv /tmp/hbase-sep-* /opt/hbase-indexer/lib/'

sleep 5

ssh hadoop1.dev1.nag.ccl 'sudo service hbase-indexer start'
ssh hadoop1.dev1.nag.ccl 'sudo service hbase-regionserver start'