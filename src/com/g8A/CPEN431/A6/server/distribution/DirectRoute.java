package com.g8A.CPEN431.A6.server.distribution;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.g8A.CPEN431.A6.protocol.Util;
import com.g8A.CPEN431.A6.server.Server;
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
	        "127.0.0.1"//, "127.0.0.1"
	        };
	private int mSelfNodeId;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        String selfHostname = "127.0.0.1";
        /*
        try {
            selfHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            System.err.println("[ERROR] Cannot get hostname of node, it will not be able to have a nodeId, exiting...");
            System.exit(1);
        }*/
        nodeHostnames[0] = selfHostname;
    	Arrays.sort(nodeHostnames);
    	ipaddrs = new AddressHolder[nodeHostnames.length];
        try {
        	for(int i = 0; i < ipaddrs.length; i++) {
                ipaddrs[i] = new AddressHolder(InetAddress.getByName(nodeHostnames[i]), DEFAULT_PORT + i);
        		AddressHolder curAddr = ipaddrs[i];
        		ByteString hostnameAndPort = Util.concatHostnameAndPort(nodeHostnames[i], curAddr.port);
    
				int nodeId = HashEntity.getInstance().addNode(hostnameAndPort);
				if (selfHostname.equals(nodeHostnames[i]) && Server.getInstance().PORT == curAddr.port) {
				    mSelfNodeId = nodeId;
				}
				System.out.println("NodeId: " + nodeId + ", hostname: " + nodeHostnames[i] + ", port: " + curAddr.port);
				nodeIdMap.put(nodeId, curAddr);
        	}
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.err.println("[ERROR]: Unable to get nodeId");
            System.exit(1);
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            System.err.println("[ERROR]: Unable to resolve host");
            System.exit(1);
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
    
    public static synchronized DirectRoute getInstance() {
        if (mDirectRoute == null) {
            mDirectRoute = new DirectRoute();
        }
        return mDirectRoute;
    }
    
    
}
