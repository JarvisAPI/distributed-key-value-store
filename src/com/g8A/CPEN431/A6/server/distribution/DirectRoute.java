package com.g8A.CPEN431.A6.server.distribution;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.g8A.CPEN431.A6.protocol.Util;
import com.google.protobuf.ByteString;

/**
 * This strategy for routing directly routes to the destination node.
 * Number of hops: O(1)
 * Amount of state (number of host/port stored): O(N)
 * Updated to Singleton pattern
 */
public class DirectRoute implements RouteStrategy {    
	
	// For now the host name and ports of the nodes are hard coded
	private final int DEFAULT_PORT = 8082;
	private AddressHolder[] ipaddrs;
	private Map<Integer, AddressHolder> nodeIdMap = new HashMap<Integer, AddressHolder>();
	private final String[] nodeHostnames = {
	        "", // Empty slot for hostname of this machine
	        "pl1.eng.monash.edu.au", "planetlab-4.eecs.cwru.edu",
	        "planetlab1.cs.ubc.ca", "planetlab2.cs.ubc.ca"
	        };
	private String mSelfHostname;
	private int mSelfNodeId;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        try {
            mSelfHostname = InetAddress.getLocalHost().getHostName();
            nodeHostnames[0] = mSelfHostname;
        } catch (UnknownHostException e1) {
            System.err.println("[ERROR] Cannot get hostname of node, it will not be able to have a nodeId, exiting...");
            System.exit(1);
        }
    	Arrays.sort(nodeHostnames);
    	ipaddrs = new AddressHolder[nodeHostnames.length];

    	for(int i = 0; i < ipaddrs.length; i++) {
            ipaddrs[i] = new AddressHolder(nodeHostnames[i], DEFAULT_PORT);
    		AddressHolder curAddr = ipaddrs[i];
    		ByteString hostnameAndPort = Util.concatHostnameAndPort(curAddr.hostname, curAddr.port);
    		try {
				int nodeId = HashEntity.getInstance().addNode(hostnameAndPort);
				if (mSelfHostname.equals(curAddr.hostname)) {
				    mSelfNodeId = nodeId;
				}
				nodeIdMap.put(nodeId, curAddr);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
    	}
    }
    
    public int getSelfNodeId() {
        return mSelfNodeId;
    }
    
    @Override
    /**
     * Get the addess and port of the node to route to, given the nodeId
     * as the id of the final destination node.
     * @param nodeId the id of the final destination node
     * @return hostname and port of node to route to, null if value is 
     */
    public AddressHolder getRoute(int nodeId) {
    	return nodeIdMap.get(nodeId);
    }
    
    public static synchronized void makeInstance() {
        if (mDirectRoute == null) {
            mDirectRoute = new DirectRoute();
        }
    }
    
    public static DirectRoute getInstance() {
        return mDirectRoute;
    }
    
    
}
