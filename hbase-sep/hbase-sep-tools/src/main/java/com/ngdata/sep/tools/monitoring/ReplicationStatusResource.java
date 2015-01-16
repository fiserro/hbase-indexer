package com.ngdata.sep.tools.monitoring;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;


@Path("indexer")
public class ReplicationStatusResource {

    @GET
    @Path("replication-status")
    @Produces("application/json")
    public ReplicationStatus get(@Context UriInfo uriInfo) throws Exception {
        ZooKeeperItf zk = ZkUtil.connect("localhost", 30000);
        ReplicationStatusRetriever retriever = new ReplicationStatusRetriever(zk, 60010);
        ReplicationStatus replicationStatus = retriever.collectStatusFromZooKeepeer();
        retriever.addStatusFromJmx(replicationStatus);
//        ReplicationStatusReport.printReport(replicationStatus, System.out);
        Closer.close(zk);
        return replicationStatus;
    }
	
}
