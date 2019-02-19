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
	        "127.0.0.1:8082", // Empty slot for hostname of this machine
	        "127.0.0.1:8083", "127.0.0.1:8084"
	        };
	private int mSelfNodeId = -1;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        try {
            String selfHostname = "127.0.0.1";
            /*
            try {
                selfHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e1) {
                System.err.println("[ERROR] Cannot get hostname of node, it will not be able to have a nodeId, exiting...");
                System.exit(1);
            }*/
            if (nodeHostnames[0].isEmpty()) {
                nodeHostnames[0] = selfHostname + ":" + Server.getInstance().PORT;
            }
            for (int i = 0; i < nodeHostnames.length; i++) {
                String[] nodeHostAndPort = nodeHostnames[i].split(":");
                if (nodeHostAndPort.length <= 1) {
                    nodeHostnames[i] += ":" + Integer.valueOf(DEFAULT_PORT);
                }
            }
        	Arrays.sort(nodeHostnames);
        	ipaddrs = new AddressHolder[nodeHostnames.length];
        	for(int i = 0; i < ipaddrs.length; i++) {
        	    String[] nodeHostAndPort = nodeHostnames[i].split(":");
        	    int port = Integer.parseInt(nodeHostAndPort[1]);
                ipaddrs[i] = new AddressHolder(InetAddress.getByName(nodeHostAndPort[0]), port);
        		AddressHolder curAddr = ipaddrs[i];
        		ByteString hostnameAndPort = Util.concatHostnameAndPort(nodeHostAndPort[0], curAddr.port);
    
				int nodeId = HashEntity.getInstance().addNode(hostnameAndPort);
				if (selfHostname.equals(nodeHostAndPort[0]) && Server.getInstance().PORT == curAddr.port) {
				    mSelfNodeId = nodeId;
				}
				System.out.println("NodeId: " + nodeId + ", hostname: " + nodeHostAndPort[0] + ", port: " + curAddr.port);
				nodeIdMap.put(nodeId, curAddr);
        	}
        	if (mSelfNodeId == -1) {
        	    System.err.println("[ERROR]: Self node id not set! Exiting...");
        	    System.exit(1);
        	}
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.err.println("[ERROR]: Unable to get node id");
            System.exit(1);
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
