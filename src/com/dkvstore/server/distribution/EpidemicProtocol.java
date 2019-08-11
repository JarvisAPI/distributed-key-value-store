package com.dkvstore.server.distribution;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Util;
import com.dkvstore.server.MembershipService;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

/**
 * Implements the epidemic protocol, to periodically push updates to
 * other servers
 */
public class EpidemicProtocol {
    // The minimum push interval is the fastest rate at which epidemic protocol
    // will periodically contact another node, however when the system is running
    // the rate might actually be slower due to other operations. In milliseconds.
    public static int MIN_PUSH_INTERVAL = 2000;
    public static int EPIDEMIC_SRC_PORT = 50222; // Source port to receive and send
    // If the timestamp counter - timestamp of system image is greater than this
    // value then the node is assumed to have failed.
    public static long NODE_HAS_FAILED_MARK = 16;
    public static int NODE_ALIVE_ROUND_LIMIT = 1;
    private static class SystemImage {
        // timestamp recorded by current node, currently it is counter
        private long timestamp;
        private long lastMsgTimestamp; // timestamp send from originating node.
        // the number of rounds since declared failure, 0 if it has not yet failed
        private int failedRoundCounter = 0;
        // the number of rounds since declared alive, 0 if it has not yet been declared alive.
        // when this counter is greater than NODE_ALIVE_ROUND_LIMIT then the node will be added to
        // the hash ring.
        private int aliveRoundCounter = 0;
        private int resetAliveCounter = 0;
    }
    
    // Format: [msgType: 1 byte][nodeIdx: 4 bytes][msgTimestamp: 8 bytes]
    private static final int PROTOCOL_FORMAT_SIZE = 13;
    
    private static EpidemicProtocol mEpidemicProtocol;
    
    private static volatile SystemImage[] mSysImages;
    private static volatile int mSysImageSize; // The number of valid entries in mSysImages
    private DatagramChannel mChannel;
    
    private static final byte MSG_TYPE_STATUS_UPDATE = 1;
    private int mNodeIdx;
    // Approximately every MIN_PUSH_INTERVAL the timestamp counter is incremented
    private volatile long mTimestampCounter;
    private long mMsgTimestampCounter;
    private Inet4Address mSelfAddr;
    
    private EpidemicProtocol(DatagramChannel channel) throws SocketException {
        mChannel = channel;
        
        mSysImageSize = 0;
        mSysImages = new SystemImage[NodeTable.getInstance().getNumberOfNodes()];

        mNodeIdx = NodeTable.getInstance().getSelfNodeIdx();
        mSysImages[mNodeIdx] = new SystemImage();
        mTimestampCounter = 0;
        mSysImages[mNodeIdx].timestamp = mTimestampCounter;
        mMsgTimestampCounter = System.currentTimeMillis();
        mSysImages[mNodeIdx].lastMsgTimestamp = mMsgTimestampCounter;
        mSysImageSize++;
        
        try {
            mSelfAddr = (Inet4Address) InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            System.err.println("[WARNING]: Cannot get self address to construct unique message ID, falling back to use loopback address");
            mSelfAddr = (Inet4Address) InetAddress.getLoopbackAddress();
        }
    }
    
    public static EpidemicProtocol makeInstance(DatagramChannel channel) throws SocketException {
        if (mEpidemicProtocol == null) {
            mEpidemicProtocol = new EpidemicProtocol(channel);
        }
        return mEpidemicProtocol;
    }
    
    public static EpidemicProtocol getInstance() {
        return mEpidemicProtocol;
    }
    
    public int getAliveMembers() {
        return mSysImageSize;
    }
    
    public void start() {
        Util.scheduler.scheduleAtFixedRate(new EpidemicSendTask(), MIN_PUSH_INTERVAL, MIN_PUSH_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    private class EpidemicSendTask implements Runnable {

        @Override
        public void run() {
            try {
                NetworkMessage msg = new NetworkMessage();
    
                AddressHolder node = NodeTable.getInstance().getRandomNode();
                if (node != null) {
                    msg.setIdString(ByteString.copyFrom(Util.getUniqueId(mSelfAddr, node.epidemicPort)));
                    synchronized(mSysImages) {
                        byte[] data = new byte[PROTOCOL_FORMAT_SIZE * mSysImageSize];
                        int j = 0;
                        for (int i = 0; i < mSysImages.length; i++) {
                            if (mSysImages[i] != null && mSysImages[i].failedRoundCounter == 0) {
                                data[j++] = MSG_TYPE_STATUS_UPDATE;
                                Util.intToBytes(i, data, j);
                                j += 4; // size of int
                                if (i == mNodeIdx) {
                                    mSysImages[i].lastMsgTimestamp = mMsgTimestampCounter;
                                }
                                Util.longToBytes(mSysImages[i].lastMsgTimestamp, data, j);
                                
                                j += 8; // size of long
                            }
                        }
                        msg.setPayload(data);
                    }
                    mChannel.send(ByteBuffer.wrap(msg.getDataBytes()), new InetSocketAddress(node.address, node.epidemicPort));
                }
                
                synchronized(mSysImages) {
                    // Check if nodes should be marked dead.
                    for (int i = 0; i < mSysImages.length; i++) {
                        if (mNodeIdx != i && mSysImages[i] != null) {
                            if (mTimestampCounter - mSysImages[i].timestamp > NODE_HAS_FAILED_MARK) {
                                // Node deemed to have failed.
                                if (mSysImages[i].failedRoundCounter > NODE_HAS_FAILED_MARK / 2) {
                                    
                                    if (mSysImages[i].aliveRoundCounter > 0)  {
                                        mSysImages[i].resetAliveCounter++;
                                        if (mSysImages[i].resetAliveCounter > NODE_HAS_FAILED_MARK) {
                                            mSysImages[i].resetAliveCounter = 0;
                                            mSysImages[i].aliveRoundCounter = 0;
                                            
                                        }
                                    }
                                    continue;
                                }
                                if (mSysImages[i].failedRoundCounter == NODE_HAS_FAILED_MARK / 2) {
                                    // Node has failed long enough to be deemed completely gone, so we remove it
                                    // now, but not before since it could be deemed failed due to network slowdown,
                                    // this will cut down potential migration costs.
                                    AddressHolder failedNode = NodeTable.getInstance().getIPaddrs()[i];
                                    NodeTable.getInstance().removeAliveNode(i);
                                    System.out.println(String.format("[INFO]: Node idx: %d removed from hash ring", i));
                                    MembershipService.OnNodeLeft(failedNode);
                                }
                                if (mSysImages[i].failedRoundCounter == 0) {
                                    // Node just failed.
                                    System.out.println(String.format("[INFO]: Node idx: %d just failed", i));
                                    mSysImageSize--;
                                }
                                mSysImages[i].failedRoundCounter++;
                            }
                        }
                    }
                    mMsgTimestampCounter++;
                    mTimestampCounter++;
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class EpidemicReceiveTask implements Runnable {
        private byte[] mDataBytes;
        
        public EpidemicReceiveTask(byte[] dataBytes) {
            mDataBytes = dataBytes;
        }
         
        @Override
        public void run() {
            try {
                NetworkMessage msg = NetworkMessage.contructMessage(mDataBytes);
                byte[] payload = msg.getPayload();
                
                if (payload.length % PROTOCOL_FORMAT_SIZE != 0) {
                    System.err.println("[WARNING]: Epidemic protocol: received wrong data length, ignoring received request");
                }
                else {
                    int nodeIdx;
                    long msgTimestamp;
                    synchronized(mSysImages) {
                        for (int i = 0; i < payload.length; ) {
                            if (payload[i] == MSG_TYPE_STATUS_UPDATE) {
                                i++;
                                nodeIdx = Util.intFromBytes(payload, i);
                                if (nodeIdx >= 0 && nodeIdx < mSysImages.length) {
                                    if (nodeIdx != mEpidemicProtocol.mNodeIdx) {
                                        msgTimestamp = Util.longFromBytes(payload, i + 4);
                                        if (mSysImages[nodeIdx] == null) {
                                            // Node joined
                                            mSysImages[nodeIdx] = new SystemImage();
                                            mSysImages[nodeIdx].lastMsgTimestamp = msgTimestamp;
                                            mSysImages[nodeIdx].timestamp = mEpidemicProtocol.mTimestampCounter;
                                            mSysImages[nodeIdx].failedRoundCounter = 0;
                                            
                                            mSysImageSize++;
                                            MembershipService.OnNodeJoin(NodeTable.getInstance().getIPaddrs()[nodeIdx]);
                                            NodeTable.getInstance().addAliveNode(nodeIdx);
                                            System.out.println(String.format("[INFO]: Node idx: %d joining", nodeIdx));
                                        }
                                        else if (mSysImages[nodeIdx].lastMsgTimestamp - msgTimestamp < 0) {
                                            // Node update
                                            mSysImages[nodeIdx].lastMsgTimestamp = msgTimestamp;
                                            mSysImages[nodeIdx].timestamp = mEpidemicProtocol.mTimestampCounter;
                                            
                                            if (mSysImages[nodeIdx].failedRoundCounter > 0) {
                                                // An assumed failed node should rejoin
                                                if (mSysImages[nodeIdx].failedRoundCounter > NODE_HAS_FAILED_MARK / 2) {
                                                    mSysImages[nodeIdx].aliveRoundCounter++;
                                                    
                                                    if (mSysImages[nodeIdx].aliveRoundCounter > NODE_ALIVE_ROUND_LIMIT) {
                                                        mSysImageSize++;
                                                        mSysImages[nodeIdx].failedRoundCounter = 0;
                                                        mSysImages[nodeIdx].aliveRoundCounter = 0;
                                                        
                                                        System.out.println(String.format("[INFO]: Node idx: %d rejoining, adding to hash ring", nodeIdx));
                                                        MembershipService.OnNodeJoin(NodeTable.getInstance().getIPaddrs()[nodeIdx]);
                                                        NodeTable.getInstance().addAliveNode(nodeIdx);
                                                    }
                                                }
                                                else {
                                                    mSysImageSize++;
                                                    mSysImages[nodeIdx].failedRoundCounter = 0;
                                                    System.out.println(String.format("[INFO]: Node idx: %d rejoining", nodeIdx));
                                                }
                                            }
                                        }
                                    }
                                }
                                else {
                                    System.err.println(String.format("[WARNING]: Epidemic protocol: bad nodeIdx: %d", nodeIdx));
                                }
                                i += PROTOCOL_FORMAT_SIZE - 1;
                            }
                            else {
                                System.err.println("[WARNING]: Epidemic protocol: unrecognized message type, corrupted message ignoring...");
                                break;
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
