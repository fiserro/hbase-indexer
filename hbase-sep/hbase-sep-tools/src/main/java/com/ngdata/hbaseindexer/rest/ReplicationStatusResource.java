/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.google.common.base.Charsets;
import com.ngdata.sep.tools.monitoring.ReplicationStatus;
import com.ngdata.sep.tools.monitoring.ReplicationStatusReport;
import com.ngdata.sep.tools.monitoring.ReplicationStatusRetriever;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.sun.jersey.json.impl.provider.entity.JSONArrayProvider;


@Path("replication-status")
public class ReplicationStatusResource {

    @GET
    @Produces("application/json")
    public Response get(@Context UriInfo uriInfo) throws Exception {
    	Configuration conf = new Configuration();
        conf.addResource("hbase-default.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hbase-indexer-default.xml");
        conf.addResource("hbase-indexer-site.xml");
        String zkConnString = conf.get("hbase.zookeeper.quorum");
        ZooKeeperItf zk = ZkUtil.connect(zkConnString, 30000);
        ReplicationStatusRetriever retriever = new ReplicationStatusRetriever(zk, 60010);
        ReplicationStatus replicationStatus = retriever.collectStatusFromZooKeepeer();
        try {
        	retriever.addStatusFromJmx(replicationStatus);
		} catch (Exception e) {
		}

        String json = ReplicationStatusReport.toJson(replicationStatus);
        
        Closer.close(zk);
        return Response.ok(json, new MediaType("application", "json")).build();
    }
	
}
