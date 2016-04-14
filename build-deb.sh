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

VERSION=1.7
SBKS_VERSION=18
filename=hbase-indexer_$VERSION~sbks~$SBKS_VERSION
if [ ! -z $( find -name "$filename"_* ) ]; then
	echo "$filename already exists, please bump sbks version"
	exit 1
fi

cleanup() {
	rm -rf hbase-indexer-dist/target/opt
	rm -f hbase-indexer-dist/target/hbase-indexer*.tar.gz
}
cleanup

mvn clean package -Pdist -DskipTests

rc=$?; if [ $rc != 0 ]; then exit $rc; fi

cd hbase-indexer-dist/target/

mkdir -p opt/hbase-indexer
echo "rm -f /opt/hbase-indexer/lib/*" > preinst
# tar -xzf hbase-$VERSION-hadoop2-bin.tar.gz -C opt/hbase --strip=1
cp -p -R hbase-indexer-*/hbase-indexer-*/* opt/hbase-indexer
# rm -rf opt/hbase/docs
# sed -i ''  '/hbase.security.log.file=.*/d' opt/hbase/conf/log4j.properties
# sed -i ''  '/hbase.log.dir=.*/d' opt/hbase/conf/log4j.properties

#../../nxfetch.sh -i com.socialbakers.protobuf:sbks-protos:0.1-protobuf-2.5.0-SNAPSHOT > opt/hbase-indexer/lib/sbks-protos-0.1.jar
#../../nxfetch.sh -i com.sematext:kite-morphlines-elasticsearch:0.1.1-SBKS-FORK > opt/hbase-indexer/lib/kite-morphlines-elasticsearch-0.1.1.jar

fpm --before-install preinst -s dir -t deb --config-files opt/hbase-indexer/conf/ -n hbase-indexer -v $VERSION~sbks~$SBKS_VERSION -a amd64 -C . opt/hbase-indexer

cleanup
