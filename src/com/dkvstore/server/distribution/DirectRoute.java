package com.dkvstore.server.distribution;

import java.util.HashMap;
import java.util.Map;

import com.dkvstore.Util;
import com.dkvstore.server.ReactorServer;
import com.google.protobuf.ByteString;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 * Updated to Singleton pattern
 */
public class DirectRoute implements RouteStrategy {    
	private Map<Integer, AddressHolder> nodeIdMap = new HashMap<>();
	private int mSelfNodeId = -1;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        try {
            NodeTable nodeTable = NodeTable.getInstance();
            String hostname = nodeTable.getSelfHostname();
            int port = ReactorServer.KEY_VALUE_PORT;
            ByteString hostnameAndPort = Util.concatHostnameAndPort(hostname, port);
            mSelfNodeId = HashEntity.getInstance().addNode(hostnameAndPort);
            System.out.println("NodeId: " + mSelfNodeId + ", hostname: " + hostname + ", port: " + port);
            AddressHolder selfAddressHolder = nodeTable.getSelfAddressHolder();
            if (selfAddressHolder == null) {
                System.err.println("[ERROR]: Self address holder is null, exiting...");
                System.exit(1);
            }
            if (selfAddressHolder.port != port || selfAddressHolder.epidemicPort != EpidemicProtocol.EPIDEMIC_SRC_PORT) {
                System.err.println("[ERROR]: Port or epidemic port mismatch in node list and options, exiting...");
                System.exit(1);
            }
            nodeIdMap.put(mSelfNodeId, selfAddressHolder);
        	if (mSelfNodeId == -1) {
        	    System.err.println("[ERROR]: Self node id not set! Exiting...");
        	    System.exit(1);
        	}
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
    public synchronized AddressHolder getRoute(int nodeId) {
        return nodeIdMap.get(nodeId);
    }
    
    public static synchronized DirectRoute getInstance() {
        if (mDirectRoute == null) {
            mDirectRoute = new DirectRoute();
        }
        return mDirectRoute;
    }
    
    /**
     * Add a node mapping of the nodeId to the node.
     * @param nodeId the nodeId key.
     * @param node the node to map to.
     */
    public synchronized void addNode(int nodeId, AddressHolder node) {
        nodeIdMap.put(nodeId, node);
    }
    
    public synchronized void removeNode(int nodeId) {
        nodeIdMap.remove(nodeId);
    }
}
