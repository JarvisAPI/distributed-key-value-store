package com.g8A.CPEN431.A7.server.distribution;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.Server;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;

/**
 * This class contains information about all the nodes that can join the network and
 * all the nodes that are currently in the network.
 *
 */
public class NodeTable {
    // Node hostnames format: ip_address + ":" + port
    private static String[] nodeHostnames = {};
    private AddressHolder[] ipaddrs;
    private static NodeTable mNodeTable;
    private List<Integer> mCurrentAliveNodes; 
    private String mSelfHostname;
    
    private NodeTable() throws UnknownHostException {
        mCurrentAliveNodes = new ArrayList<>();
        mSelfHostname = InetAddress.getLocalHost().getHostName();
        if (mSelfHostname.endsWith(".local")) {
            mSelfHostname = "127.0.0.1";
        }
        System.out.println("Self Hostname: " + mSelfHostname);
        if (nodeHostnames.length == 0) {
            System.out.println("[INFO]: No other nodes specified, using default single node setup");
            nodeHostnames = new String[] {mSelfHostname + ":" + Server.getInstance().PORT};
        }
        Arrays.sort(nodeHostnames);
        ipaddrs = new AddressHolder[nodeHostnames.length];
        for(int i = 0; i < ipaddrs.length; i++) {
            String[] nodeHostAndPort = nodeHostnames[i].split(":");
            int port = Integer.parseInt(nodeHostAndPort[1]);
            ipaddrs[i] = new AddressHolder(InetAddress.getByName(nodeHostAndPort[0]), port);
        }
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
    
    /**
     * 
     * @return random node that is alive in the network,
     *         null if no other nodes are alive.
     */
    public AddressHolder getRandomNode() {
        int randCurIdx = Util.rand.nextInt(mCurrentAliveNodes.size());
        if (randCurIdx > 0) {
            int nodeIdx = mCurrentAliveNodes.get(randCurIdx);
            return ipaddrs[nodeIdx];
        }
        return null;
    }
    
    /**
     * Add a node index to the list of alive nodes.
     * @param nodeIdx the node index to add.
     */
    public void addAliveNode(int nodeIdx) {
        if (!mCurrentAliveNodes.contains(nodeIdx)) {
            mCurrentAliveNodes.add(nodeIdx);
        }
    }
    
    /**
     * Remove a node index from the list of alive nodes.
     * @param nodeIdx the node index to remove.
     */
    public void removeAliveNode(int nodeIdx) {
        mCurrentAliveNodes.remove((Object)nodeIdx);
    }
    
    /**
     * 
     * @return the number of nodes that can participate in the network.
     */
    public int getNumberOfNodes() {
        return ipaddrs.length;
    }
    
    public AddressHolder[] getIPaddrs() {
        return ipaddrs;
    }
    
    public String getSelfHostname() {
        return mSelfHostname;
    }
    
    public static NodeTable makeInstance() throws UnknownHostException {
        if (mNodeTable == null) {
            mNodeTable = new NodeTable();
        }
        return mNodeTable;
    }
    
    public static NodeTable getInstance() {
        return mNodeTable;
    }
    
}
