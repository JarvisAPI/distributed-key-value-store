package com.s44801165.CPEN431.A3.protocol;

import java.io.IOException;
import java.net.InetAddress;
import java.util.zip.CRC32;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.Message;

public class NetworkMessage {
    public static final int ID_SIZE = 16; // in bytes
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024; // in bytes
    private static CRC32 mCrc = new CRC32();
    private byte[] mUniqueId;
    private byte[] mPayload;
    private InetAddress mAddress;
    private int mPort;

    public NetworkMessage() {

    }
    
    public NetworkMessage(byte[] uniqueId) {
        mUniqueId = uniqueId;
    }

    public void setPayload(byte[] payload) {
        int payloadSize = payload.length;
        if (payloadSize > MAX_PAYLOAD_SIZE) {
            System.out.println("Payload in request message is too long, truncating...");
            payloadSize = MAX_PAYLOAD_SIZE;
        }
        mPayload = new byte[payloadSize];
        System.arraycopy(payload, 0, mPayload, 0, payloadSize);
    }

    /**
     * Get bytes to send.
     * 
     * @return all bytes that describes the message.
     */
    public byte[] getDataBytes() {
        CRC32 checksum = new CRC32();
        checksum.update(mUniqueId);
        checksum.update(mPayload);
        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(mUniqueId))
                .setPayload(ByteString.copyFrom(mPayload))
                .setCheckSum(checksum.getValue())
                .build();
        return msg.toByteArray();
    }

    /**
     * 
     * @return the maximum data byte buffer that network messages need.
     */
    public static byte[] getMaxDataBuffer() {
        return new byte[ID_SIZE + MAX_PAYLOAD_SIZE];
    }
    
    public static NetworkMessage contructMessage(byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.newBuilder()
                .mergeFrom(data)
                .build();
        byte[] id = transportMsg.getMessageID().toByteArray();
        byte[] payload = transportMsg.getPayload().toByteArray();
        long checksum = transportMsg.getCheckSum();
        if (!validateChecksum(id, payload, checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        NetworkMessage msg = new NetworkMessage(id);
        msg.mPayload = payload;
        
        return msg;
    }
    
    public static void setMessage(NetworkMessage msg, byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.newBuilder()
                .mergeFrom(data)
                .build();
        byte[] id = transportMsg.getMessageID().toByteArray();
        byte[] payload = transportMsg.getPayload().toByteArray();
        long checksum = transportMsg.getCheckSum();
        if (!validateChecksum(id, payload, checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        msg.mUniqueId = id;
        msg.mPayload = payload;
    }
    
    private static boolean validateChecksum(byte[] id,
            byte[] payload, long checksum) {
        mCrc.reset();
        mCrc.update(id);
        mCrc.update(payload);

        return mCrc.getValue() == checksum;
    }

    public byte[] getId() {
        return mUniqueId;
    }

    public byte[] getPayload() {
        return mPayload;
    }
    
    public void setAddressAndPort(InetAddress addr, int port) {
        mAddress = addr;
        mPort = port;
    }
    
    public InetAddress getAddress() {
        return mAddress;
    }
    
    public int getPort() {
        return mPort;
    }
}
