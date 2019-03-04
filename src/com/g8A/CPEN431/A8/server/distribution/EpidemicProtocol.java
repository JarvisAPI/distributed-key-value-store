package com.g8A.CPEN431.A8.server.distribution;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.protocol.Util;
import com.g8A.CPEN431.A8.server.MembershipService;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

/**
 * Implements the epidemic protocol, to periodically push updates to
 * other servers
 */
public class EpidemicProtocol implements Runnable {
    // The minimum push interval is the fastest rate at which epidemic protocol
    // will periodically contact another node, however when the system is running
    // the rate might actually be slower due to other operations. In milliseconds.
    public static int MIN_PUSH_INTERVAL = 4000;
    public static int EPIDEMIC_SRC_PORT = 50222; // Source port to receive and send
    // If the timestamp counter - timestamp of system image is greater than this
    // value then the node is assumed to have failed.
    public static long NODE_HAS_FAILED_MARK = 8;
    private static class SystemImage {
        // timestamp recorded by current node, currently it is counter
        private long timestamp;
        private long lastMsgTimestamp; // timestamp send from originating node.
        // the number of rounds since declared failure, 0 if it has not yet failed
        private int failedRoundCounter = 0;
    }
    
    // Format: [msgType: 1 byte][nodeIdx: 4 bytes][msgTimestamp: 8 bytes]
    private static final int PROTOCOL_FORMAT_SIZE = 13;
    
    private static EpidemicProtocol mEpidemicProtocol;
    
    private volatile SystemImage[] mSysImages;
    private volatile int mSysImageSize; // The number of valid entries in mSysImages
    private DatagramSocket mSocket;
    
    private static final byte MSG_TYPE_STATUS_UPDATE = 1;
    private int mNodeIdx;
    // Approximately every MIN_PUSH_INTERVAL the timestamp counter is incremented
    private volatile long mTimestampCounter;
    private long mMsgTimestampCounter;
    
    private EpidemicProtocol() throws SocketException {
        mSocket = new DatagramSocket(EPIDEMIC_SRC_PORT);
        
        mSysImageSize = 0;
        mSysImages = new SystemImage[NodeTable.getInstance().getNumberOfNodes()];

        mNodeIdx = NodeTable.getInstance().getSelfNodeIdx();
        mSysImages[mNodeIdx] = new SystemImage();
        mTimestampCounter = 0;
        mSysImages[mNodeIdx].timestamp = mTimestampCounter;
        mMsgTimestampCounter = System.currentTimeMillis();
        mSysImages[mNodeIdx].lastMsgTimestamp = mMsgTimestampCounter;
        mSysImageSize++;
    }
    
    public static EpidemicProtocol makeInstance() throws SocketException {
        if (mEpidemicProtocol == null) {
            mEpidemicProtocol = new EpidemicProtocol();
        }
        return mEpidemicProtocol;
    }
    
    public static EpidemicProtocol getInstance() {
        return mEpidemicProtocol;
    }
    
    public int getAliveMembers() {
        return mSysImageSize;
    }
    
    @Override
    public void run() {
        AddressHolder node;
        Inet4Address selfAddr;
        try {
            selfAddr = (Inet4Address) InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            System.err.println("[WARNING]: Cannot get self address to construct unique message ID, falling back to use loopback address");
            selfAddr = (Inet4Address) InetAddress.getLoopbackAddress();
        }
        NetworkMessage msg = new NetworkMessage();
        DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        
        new EpidemicReceiveThread().start();
        
        while (true) {
            try {
                node = NodeTable.getInstance().getRandomNode();
                if (node != null) {
                    msg.setIdString(ByteString.copyFrom(Util.getUniqueId(selfAddr, node.epidemicPort)));
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
                    packet.setData(msg.getDataBytes());
                    packet.setAddress(node.address);
                    packet.setPort(node.epidemicPort);
                    mSocket.send(packet);
                }

                Thread.sleep(MIN_PUSH_INTERVAL);
                
                synchronized(mSysImages) {
                    // Check if nodes should be marked dead.
                    for (int i = 0; i < mSysImages.length; i++) {
                        if (mNodeIdx != i && mSysImages[i] != null) {
                            if (mTimestampCounter - mSysImages[i].timestamp > NODE_HAS_FAILED_MARK) {
                                // Node deemed to have failed.
                                if (mSysImages[i].failedRoundCounter > NODE_HAS_FAILED_MARK) {
                                    mSysImages[i] = null;
                                    continue;
                                }
                                if (mSysImages[i].failedRoundCounter == 0) {
                                    // Node just failed.
                                    AddressHolder failedNode = NodeTable.getInstance().getIPaddrs()[i];
                                    NodeTable.getInstance().removeAliveNode(i);
                                    mSysImageSize--;
                                    System.out.println(String.format("[INFO]: Node idx: %d leaving", i));
                                    MembershipService.OnNodeLeft(failedNode);
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
    
    private class EpidemicReceiveThread extends Thread {
         
        @Override
        public void run() {
            NetworkMessage msg = new NetworkMessage();
            byte[] buf = NetworkMessage.getMaxDataBuffer();
            DatagramPacket receivePacket = new DatagramPacket(buf, buf.length);
            
            while (true) {
                try {
                    mSocket.receive(receivePacket);
                    NetworkMessage.setMessage(msg, Arrays.copyOf(receivePacket.getData(), receivePacket.getLength()));
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
                                        if (nodeIdx != mNodeIdx) {
                                            msgTimestamp = Util.longFromBytes(payload, i + 4);
                                            if (mSysImages[nodeIdx] == null) {
                                                // Node joined
                                                mSysImages[nodeIdx] = new SystemImage();
                                                mSysImages[nodeIdx].lastMsgTimestamp = msgTimestamp;
                                                mSysImages[nodeIdx].timestamp = mTimestampCounter;
                                                mSysImages[nodeIdx].failedRoundCounter = 0;
                                                
                                                mSysImageSize++;
                                                MembershipService.OnNodeJoin(NodeTable.getInstance().getIPaddrs()[nodeIdx]);
                                                NodeTable.getInstance().addAliveNode(nodeIdx);
                                                System.out.println(String.format("[INFO]: Node idx: %d joining", nodeIdx));
                                            }
                                            else if (mSysImages[nodeIdx].lastMsgTimestamp - msgTimestamp < 0) {
                                                // Node update
                                                mSysImages[nodeIdx].lastMsgTimestamp = msgTimestamp;
                                                mSysImages[nodeIdx].timestamp = mTimestampCounter;
                                                if (mSysImages[nodeIdx].failedRoundCounter > 0) {
                                                    // A failed node should rejoin
                                                    mSysImages[nodeIdx].failedRoundCounter = 0;
                                                    MembershipService.OnNodeJoin(NodeTable.getInstance().getIPaddrs()[nodeIdx]);
                                                    mSysImageSize++;
                                                    NodeTable.getInstance().addAliveNode(nodeIdx);
                                                    System.out.println(String.format("[INFO]: Node idx: %d joining", nodeIdx));
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

}
