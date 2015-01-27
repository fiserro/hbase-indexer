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

VERSION=1.6
SBKS_VERSION=1

if [ ! -z $( grep -R hbase-indexer-$VERSION~sbks~$SBKS_VERSION) ]; then
	echo "hbase-indexer-$VERSION~sbks~$SBKS_VERSION already exists, please bump sbks version"
	exit 1
fi

cleanup() {
	rm -rf hbase-indexer-dist/target/opt
	rm -f hbase-indexer-dist/target/hbase-indexer*.tar.gz
}
cleanup

mvn package -Pdist -DskipTests -Dhbase.api=0.98

cd hbase-indexer-dist/target/

mkdir -p opt/hbase-indexer
# tar -xzf hbase-$VERSION-hadoop2-bin.tar.gz -C opt/hbase --strip=1
cp -p -R hbase-indexer-*/hbase-indexer-*/* opt/hbase-indexer
# rm -rf opt/hbase/docs
# sed -i ''  '/hbase.security.log.file=.*/d' opt/hbase/conf/log4j.properties
# sed -i ''  '/hbase.log.dir=.*/d' opt/hbase/conf/log4j.properties

fpm -s dir -t deb --config-files opt/hbase-indexer/conf/ -n hbase-indexer -v $VERSION~sbks~$SBKS_VERSION -a amd64 -C . opt/hbase-indexer

cleanup
