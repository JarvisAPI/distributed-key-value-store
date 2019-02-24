package com.g8A.CPEN431.A7.server.distribution;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.MembershipService;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

/**
 * Implements the epidemic protocol, to periodically push updates to
 * other servers
 */
public class EpidemicProtocol implements Runnable {
    // The minimum push interval is the fastest rate at which epidemic protocol
    // will periodically contact another node, however when the system is running
    // the rate might actually be slower due to other operations. In milliseconds.
    private static final int MIN_PUSH_INTERVAL = 4000;
    private static final int TOLERANCE = 100;
    public static int EPIDEMIC_SRC_PORT = 50222; // Source port to receive and send
    // If the timestamp counter - timestamp of system image is greater than this
    // value then the node is assumed to have failed.
    private static final long NODE_HAS_FAILED_MARK = 5;
    private static class SystemImage {
        // timestamp recorded by current node, currently it is counter
        private long timestamp;
        private long lastMsgTimestamp; // timestamp send from originating node.
    }
    
    // Format: [msgType: 1 byte][nodeIdx: 4 bytes][msgTimestamp: 8 bytes]
    private static final int PROTOCOL_FORMAT_SIZE = 13;
    
    private SystemImage[] mSysImages;
    private int mSysImageSize; // The number of valid entries in mSysImages
    private DatagramSocket mSocket;
    
    private static final byte MSG_TYPE_STATUS_UPDATE = 1;
    private int mNodeIdx;
    // Approximately every MIN_PUSH_INTERVAL the timestamp counter is incremented
    private long mTimestampCounter;
    private long mMsgTimestampCounter;
    
    public EpidemicProtocol() throws SocketException {
        mSocket = new DatagramSocket(EPIDEMIC_SRC_PORT);
        mSocket.setSoTimeout(MIN_PUSH_INTERVAL);
        
        mSysImageSize = 0;
        mSysImages = new SystemImage[NodeTable.getInstance().getNumberOfNodes()];

        mNodeIdx = NodeTable.getInstance().getSelfNodeIdx();
        mSysImages[mNodeIdx] = new SystemImage();
        mSysImages[mNodeIdx].timestamp = System.nanoTime();
        mTimestampCounter = 0;
        mMsgTimestampCounter = System.currentTimeMillis();
        mSysImages[mNodeIdx].lastMsgTimestamp = mMsgTimestampCounter;
        mSysImageSize++;
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
        byte[] buf = NetworkMessage.getMaxDataBuffer();
        DatagramPacket receivePacket = new DatagramPacket(buf, buf.length);
        
        long elapsedTime;
        while (true) {
            try {
                node = NodeTable.getInstance().getRandomNode();
                if (node != null) {
                    //System.out.println(String.format("[INFO]: Chosen node: host: %s, port: %d, epidemicPort: %d",
                    //        node.address.getHostName(), node.port, node.epidemicPort));
                    msg.setIdString(ByteString.copyFrom(Util.getUniqueId(selfAddr, node.epidemicPort)));
                    byte[] data = new byte[PROTOCOL_FORMAT_SIZE * mSysImageSize];
                    int j = 0;
                    for (int i = 0; i < mSysImages.length; i++) {
                        if (mSysImages[i] != null) {
                            data[j++] = MSG_TYPE_STATUS_UPDATE;
                            Util.intToBytes(i, data, j);
                            j += 4; // size of int
                            if (i == mNodeIdx) {
                                mMsgTimestampCounter++;
                                mSysImages[i].lastMsgTimestamp = mMsgTimestampCounter;
                            }
                            Util.longToBytes(mSysImages[i].lastMsgTimestamp, data, j);
                            
                            j += 8; // size of long
                        }
                    }
                    
                    msg.setPayload(data);
                    packet.setData(msg.getDataBytes());
                    packet.setAddress(node.address);
                    packet.setPort(node.epidemicPort);
                    mSocket.send(packet);
                }
                
                elapsedTime = System.nanoTime();
                try {
                    mSocket.receive(receivePacket);
                    //System.out.println("[INFO]: Received message");
                    NetworkMessage.setMessage(msg, Arrays.copyOf(receivePacket.getData(), receivePacket.getLength()));
                    byte[] payload = msg.getPayload();
                    
                    if (payload.length % PROTOCOL_FORMAT_SIZE != 0) {
                        System.err.println("[WARNING]: Epidemic protocol: received wrong data length, ignoring received request");
                    }
                    else {
                        int nodeIdx;
                        long msgTimestamp;
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
                                            mSysImages[nodeIdx].timestamp = 0;
                                            mSysImageSize++;
                                            NodeTable.getInstance().addAliveNode(nodeIdx);
                                            System.out.println(String.format("[INFO]: nodeIdx %d joining", nodeIdx));
                                            MembershipService.OnNodeJoin(NodeTable.getInstance().getIPaddrs()[nodeIdx]);
                                        }
                                        else if (mSysImages[nodeIdx].lastMsgTimestamp - msgTimestamp < 0) {
                                            // Node update
                                            //System.out.println(String.format("[INFO]: Updated timestamp for nodeIdx: %d", nodeIdx));
                                            mSysImages[nodeIdx].lastMsgTimestamp = msgTimestamp;
                                            mSysImages[nodeIdx].timestamp++;
                                        }
                                        /*
                                        else {
                                            System.out.println("[INFO]: No update");
                                        }*/
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
                } catch (SocketTimeoutException e) {
                    // Ignored
                }
                
                // Check if nodes should be marked dead.
                for (int i = 0; i < mSysImages.length; i++) {
                    if (mNodeIdx != i && mSysImages[i] != null) {
                        //System.out.println(String.format("dif: %d", currentTimestamp - mSysImages[i].timestamp));
                        if (mTimestampCounter - mSysImages[i].timestamp > NODE_HAS_FAILED_MARK) {
                            // Node deemed to have failed.
                            AddressHolder failedNode = NodeTable.getInstance().getIPaddrs()[i];
                            NodeTable.getInstance().removeAliveNode(i);
                            mSysImages[i] = null;
                            System.out.println(String.format("[INFO]: nodeIdx %d leaving", i));
                            mSysImageSize--;
                            MembershipService.OnNodeLeft(failedNode);
                        }
                    }
                }
                
                elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - elapsedTime);
                if (MIN_PUSH_INTERVAL - elapsedTime > TOLERANCE) {
                    Thread.sleep(MIN_PUSH_INTERVAL - elapsedTime);
                }
                mTimestampCounter++;
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

}
