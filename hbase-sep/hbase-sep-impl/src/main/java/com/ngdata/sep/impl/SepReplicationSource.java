/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import static com.ngdata.sep.impl.SepModelImpl.toExternalSubscriptionName;

import java.io.IOException;
import java.util.ServiceLoader;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;

import com.ngdata.sep.WALEditFilter;
import com.ngdata.sep.WALEditFilterProvider;

/**
 * Custom replication source for distributing Side-Effect Processor (SEP) events to listeners
 * elsewhere on the cluster, as well as remaining compatible with stock HBase replication servers.
 * <p>
 * When a {@code SepReplicationSource} is replicating to a standard replication sink, all replication entries are
 * delegated to the stock HBase {@code ReplicationSource}. When a {@code SepReplicationSource} is replicating to a
 * custom SEP handler, the replication entries will be filtered to only send the minimal necessary information for the
 * remote SEP event processor.
 */
public class SepReplicationSource extends ReplicationSource {

	private WALEditFilter walEditFilter;
	private final Log log = LogFactory.getLog(getClass());

	@Override
	public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager, ReplicationQueues replicationQueues,
			ReplicationPeers replicationPeers, Stoppable stopper, String peerClusterZnode, UUID clusterId,
			ReplicationEndpoint replicationEndpoint, MetricsSource metrics) throws IOException {
		super.init(conf, fs, manager, replicationQueues, replicationPeers, stopper, peerClusterZnode, clusterId, replicationEndpoint,
				metrics);
		log.debug("init on cluster " + getPeerClusterId() + " on node " + getPeerClusterZnode());
		setWALEditFilter(loadEditFilter(getPeerClusterId(), ServiceLoader.load(WALEditFilterProvider.class)));
	}

	// @Override
	// protected void removeNonReplicableEdits(HLog.Entry edit) {
	// super.removeNonReplicableEdits(edit);
	// if (walEditFilter != null) {
	// walEditFilter.apply(edit);
	// }
	// }

	WALEditFilter loadEditFilter(String peerClusterId, Iterable<WALEditFilterProvider> editFilterProviders) {
		for (WALEditFilterProvider editFilterProvider : editFilterProviders) {
			WALEditFilter editFilter = editFilterProvider.getWALEditFilter(toExternalSubscriptionName(peerClusterId));
			if (editFilter != null) {
				log.debug("Loaded WALEditFilter " + editFilter);
				return editFilter;
			}
		}
		log.debug("No custom WALEditFilter loaded");
		return null;
	}

	void setWALEditFilter(WALEditFilter walEditFilter) {
		this.walEditFilter = walEditFilter;
	}

}
