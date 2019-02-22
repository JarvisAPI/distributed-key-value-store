package com.g8A.CPEN431.A7.server.distribution;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
	
	private static AddressHolder[] ipaddrs;
	private Map<Integer, AddressHolder> nodeIdMap = new HashMap<Integer, AddressHolder>();
	// Node hostnames format: ip_address + ":" + port
	private static String[] nodeHostnames = {};
	private int mSelfNodeId = -1;
    
    private static DirectRoute mDirectRoute;
    
    private DirectRoute() {
        try {
            String selfHostname = InetAddress.getLocalHost().getHostName();
            if (selfHostname.endsWith(".local")) {
                selfHostname = "127.0.0.1";
            }
            System.out.println("Self Hostname: " + selfHostname);
            if (nodeHostnames.length == 0) {
                System.out.println("[INFO]: No other nodes specified, using default single node setup");
                nodeHostnames = new String[] {selfHostname + ":" + Server.getInstance().PORT};
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
    
    public static void parseNodeListFile(String nodeListFile) throws Exception {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(nodeListFile));
            List<String> nodeList = new ArrayList<>();
            String entry = reader.readLine();
            while (entry != null) {
                String[] hostAndPort = entry.split(":");
                if (hostAndPort.length < 2) {
                    System.err.println("[ERRPR]: entry: " + entry + ", format is in correct");
                }
                else {
                    if (hostAndPort[0].equals("localhost")) {
                        hostAndPort[0] = "127.0.0.1";
                    }
                    nodeList.add(hostAndPort[0] + ":" + hostAndPort[1]);
                }
                entry = reader.readLine();
            }
            nodeHostnames = new String[nodeList.size()];
            nodeList.toArray(nodeHostnames);
        } catch(FileNotFoundException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
   
    public static AddressHolder getRandomNode() {
        return ipaddrs[Util.rand.nextInt(ipaddrs.length)];
    }
    
    public static int getNumberOfNodes() {
        return ipaddrs.length;
    }
}
