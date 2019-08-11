package com.dkvstore.server.distribution;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.dkvstore.Util;
import com.dkvstore.server.ReactorServer;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;

/**
 * This class contains information about all the nodes that can join the network and
 * all the nodes that are currently in the network.
 *
 */
public class NodeTable {
    // Node hostnames format: ip_address + ":" + port + ":" epidemicPort
    private static String[] nodeHostnames = {};
    private AddressHolder[] ipaddrs;
    private static NodeTable mNodeTable;
    private List<Integer> mCurrentAliveNodes; 
    private String mSelfHostname;
    
    private NodeTable(String selfHostname) throws UnknownHostException {
        mCurrentAliveNodes = new ArrayList<>();
        if (selfHostname == null) {
            mSelfHostname = InetAddress.getLocalHost().getHostName();
        }
        else {
            mSelfHostname = selfHostname;
        }
        System.out.println("[INFO]: Self Hostname: " + mSelfHostname);
        if (nodeHostnames.length == 0) {
            System.out.println("[INFO]: No other nodes specified, using default single node setup");
            nodeHostnames = new String[] {mSelfHostname + ":" + ReactorServer.KEY_VALUE_PORT};
        }
        Arrays.sort(nodeHostnames);
        ipaddrs = new AddressHolder[nodeHostnames.length];
        for(int i = 0; i < ipaddrs.length; i++) {
            String[] nodeHostAndPort = nodeHostnames[i].split(":");
            int port = Integer.parseInt(nodeHostAndPort[1]);
            int epidemicPort;
            if (nodeHostAndPort.length > 2) {
                epidemicPort = Integer.parseInt(nodeHostAndPort[2]);
            }
            else {
                epidemicPort = EpidemicProtocol.EPIDEMIC_SRC_PORT;
            }
            System.out.println(String.format("[INFO]: Ipaddr entry %d, hostname: %s, port: %d, epidemicPort: %d", i, nodeHostAndPort[0], port, epidemicPort));
            ipaddrs[i] = new AddressHolder(InetAddress.getByName(nodeHostAndPort[0]), nodeHostAndPort[0], port, epidemicPort);
        }
        
        
        // Initially assume all other nodes are alive.
        int selfNodeIdx = getSelfNodeIdx();
        System.out.println("[INFO]: Self Node Idx: " + selfNodeIdx);
        for (int i = 0; i < ipaddrs.length; i++) {
            if (i != selfNodeIdx) {
                mCurrentAliveNodes.add(i);
            }
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
                    String nodeEntry = hostAndPort[0] + ":" + hostAndPort[1];
                    if (hostAndPort.length > 2) {
                        nodeEntry += ":" + hostAndPort[2];
                    }
                    nodeList.add(nodeEntry);
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
        if (mCurrentAliveNodes.size() > 0) {
            int randCurIdx = Util.rand.nextInt(mCurrentAliveNodes.size());
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
    
    public AddressHolder getSelfAddressHolder() {
        for (int i = 0; i < ipaddrs.length; i++) {
            String hostname = ipaddrs[i].hostname;
            if (hostname.equals("localhost")) {
                hostname = "127.0.0.1";
            }
            if (mSelfHostname.equals(hostname) &&
                    ReactorServer.KEY_VALUE_PORT == ipaddrs[i].port) {
                return ipaddrs[i];
            }
        }
        return null;
    }
    
    /**
     * 
     * @return the node index of the current node or -1 if it doesn't exist.
     */
    public int getSelfNodeIdx() {
        for (int i = 0; i < ipaddrs.length; i++) {
            String hostname = ipaddrs[i].hostname;
            if (hostname.equals("localhost")) {
                hostname = "127.0.0.1";
            }
            if (mSelfHostname.equals(hostname) &&
                    ReactorServer.KEY_VALUE_PORT == ipaddrs[i].port) {
                return i;
            }
        }
        return -1;
    }
    
    public static NodeTable makeInstance(boolean isLocal) throws UnknownHostException {
        if (mNodeTable == null) {  
            mNodeTable = new NodeTable(isLocal ? "127.0.0.1" : null);
        }
        return mNodeTable;
    }
    
    public static NodeTable getInstance() {
        return mNodeTable;
    }
    
}
