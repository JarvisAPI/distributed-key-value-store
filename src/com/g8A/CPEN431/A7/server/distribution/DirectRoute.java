package com.g8A.CPEN431.A7.server.distribution;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.Server;
import com.google.protobuf.ByteString;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 * Updated to Singleton pattern
 */
public class DirectRoute implements RouteStrategy {    
	private Map<Integer, AddressHolder> nodeIdMap = new HashMap<Integer, AddressHolder>();
	private int mSelfNodeId = -1;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        try {
            NodeTable nodeTable = NodeTable.makeInstance();
            AddressHolder[] ipaddrs = nodeTable.getIPaddrs();
        	for(int i = 0; i < ipaddrs.length; i++) {
        		AddressHolder curAddr = ipaddrs[i];
        		String hostname = curAddr.address.getHostName();
        		ByteString hostnameAndPort = Util.concatHostnameAndPort(hostname, curAddr.port);
    
				int nodeId = HashEntity.getInstance().addNode(hostnameAndPort);
				if (nodeTable.getSelfHostname().equals(hostname) && Server.getInstance().PORT == curAddr.port) {
				    mSelfNodeId = nodeId;
				}
				System.out.println("NodeId: " + nodeId + ", hostname: " + hostname + ", port: " + curAddr.port);
				nodeIdMap.put(nodeId, curAddr);
        	}
        	if (mSelfNodeId == -1) {
        	    System.err.println("[ERROR]: Self node id not set! Exiting...");
        	    System.exit(1);
        	}
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            System.err.println("[ERROR]: Unable to resolve host");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR]: Failure when contructing node id table");
            System.exit(1);
        }
    }
    
    public int getSelfNodeId() {
        return mSelfNodeId;
    }
    
    public AddressHolder getLocalAddress() {
    	return nodeIdMap.get(mSelfNodeId);
    }
    
    @Override
    /**
     * Get the address and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to, null if value is 
     */
    public AddressHolder getRoute(int nodeId) {
        return nodeIdMap.get(nodeId);
    }
    
    public static synchronized DirectRoute getInstance() {
        if (mDirectRoute == null) {
            mDirectRoute = new DirectRoute();
        }
        return mDirectRoute;
    }
    
    /**
     * Get the index into the node table array given a nodeId.
     * @param nodeId the id of the node to get index of.
     * @return the index into the node address array, or -1 if index does not exit.
     */
    public int getNodeIdx(int nodeId) {
        AddressHolder holder = nodeIdMap.get(nodeId);
        AddressHolder[] ipaddrs = NodeTable.getInstance().getIPaddrs();
        for (int i = 0; i < ipaddrs.length; i++) {
            if (ipaddrs[i] == holder) {
                return i;
            }
        }
        return -1;
    }
}
